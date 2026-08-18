#!/usr/bin/env python3
"""Statusengine WebSocket test client.

Connects to the worker's /ws endpoint, lets you subscribe to and
unsubscribe from single topics at runtime and prints every received
event. The Python counterpart of web/ws-test-client.html.

Requires: pip install websockets

Examples:
    python3 web/ws_client.py --api-key <key>
    python3 web/ws_client.py --api-key <key> --topics statusngin_hoststatus
    python3 web/ws_client.py --api-key <key> --topics all --no-interactive
    python3 web/ws_client.py --api-key <key> --quiet   # measure, don't print

Interactive commands (type into stdin while connected):
    sub <topic> [...]     subscribe to one or more topics
    unsub <topic> [...]   unsubscribe from one or more topics
    all                   subscribe to every known topic
    none                  unsubscribe from every known topic
    subs                  show current subscriptions
    topics                list all known topics
    quit                  close the connection and exit
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime
from urllib.parse import urlencode, urlsplit, urlunsplit

try:
    import websockets
except ImportError:
    sys.exit("missing dependency: pip install websockets")

# Kept in sync with the topic table in docs/openapi.yaml.
TOPICS = [
    "statusngin_hoststatus",
    "statusngin_servicestatus",
    "statusngin_hostchecks",
    "statusngin_servicechecks",
    "statusngin_service_perfdata",
    "statusngin_statechanges",
    "statusngin_logentries",
    "statusngin_notifications",
    "statusngin_contactnotificationmethod",
    "statusngin_acknowledgements",
    "statusngin_downtimes",
    "statusngin_core_restart",
]


def build_url(url: str, topics: list[str], api_key: str | None, key_in_query: bool) -> str:
    parts = urlsplit(url)
    query = dict(p.split("=", 1) for p in parts.query.split("&") if "=" in p)
    if topics:
        query["topics"] = ",".join(topics)
    if key_in_query and api_key:
        query["api_key"] = api_key
    return urlunsplit(parts._replace(query=urlencode(query)))


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def frame_events(raw: str) -> tuple[str, list]:
    """Split one received frame into its topic and its list of events.

    A frame's payload is normally a list - the worker sends one frame per
    queue job, and a job carries a bulk array of events. A bare object is
    accepted too, so this client keeps working against a worker that sends
    one frame per event.

    Decoding is deliberately separated from printing: --quiet must be able
    to count events without paying for a json.dumps per event, or the
    measurement would be measuring this client.
    """
    msg = json.loads(raw)
    topic, payload = msg.get("topic", "?"), msg.get("payload")
    return topic, payload if isinstance(payload, list) else [payload]


def print_events(topic: str, events: list, pretty: bool) -> None:
    for event in events:
        body = json.dumps(event, indent=2, ensure_ascii=False) if pretty else json.dumps(event, ensure_ascii=False)
        log(f"{topic}\n{body}" if pretty else f"{topic} {body}")


async def read_commands(ws, subs: set[str]) -> None:
    """Read control commands from stdin and send subscription frames."""
    lines: asyncio.Queue[str] = asyncio.Queue()
    loop = asyncio.get_running_loop()

    # A daemon thread, not run_in_executor: a readline still parked on the
    # terminal would otherwise keep asyncio.run() from ever returning.
    def pump() -> None:
        for line in sys.stdin:
            loop.call_soon_threadsafe(lines.put_nowait, line)
        loop.call_soon_threadsafe(lines.put_nowait, "")

    threading.Thread(target=pump, daemon=True).start()

    while True:
        line = await lines.get()
        if not line:  # stdin closed
            return
        cmd, *args = line.split() or [""]
        if cmd in ("quit", "exit", "q"):
            return
        if cmd == "topics":
            log("known topics:\n  " + "\n  ".join(TOPICS))
        elif cmd == "subs":
            log("current subscriptions: " + (", ".join(sorted(subs)) if subs else "<all topics>"))
        elif cmd in ("sub", "unsub", "all", "none"):
            if cmd == "all":
                cmd, args = "sub", list(TOPICS)
            elif cmd == "none":
                cmd, args = "unsub", list(TOPICS)
            if not args:
                log(f"usage: {cmd} <topic> [...]")
                continue
            unknown = [t for t in args if t not in TOPICS]
            if unknown:
                log(f"warning: unknown topic(s): {', '.join(unknown)}")
            key = "subscribe" if cmd == "sub" else "unsubscribe"
            await ws.send(json.dumps({key: args}))
            subs.update(args) if cmd == "sub" else subs.difference_update(args)
            log(f"{key}d: {', '.join(args)}")
        elif cmd:
            log(f"unknown command: {cmd} (try: sub, unsub, all, none, subs, topics, quit)")


async def receive(ws, pretty: bool, limit: int, quiet: bool, interval: float) -> None:
    """Consume frames, printing every event unless quiet is set.

    With quiet, this loop is the measurement: the only output is one line
    per interval, so nothing the client prints can be what limits the rate
    it reports. That distinction matters - printing every event puts a
    terminal write on the path for each one, and a terminal that stalls
    for longer than the server's per-client send buffer holds is enough to
    make the worker drop messages that the client could easily have kept
    up with.
    """
    events = frames = 0
    start = last = time.perf_counter()
    last_events = last_frames = 0

    try:
        async for raw in ws:
            try:
                topic, batch = frame_events(raw)
            except (json.JSONDecodeError, AttributeError):
                log(f"non-JSON frame: {raw}")
                continue

            frames += 1
            events += len(batch)
            if not quiet:
                print_events(topic, batch, pretty)
            elif (now := time.perf_counter()) - last >= interval:
                window = now - last
                log(f"{(events - last_events) / window:9.0f} events/s "
                    f"{(frames - last_frames) / window:8.0f} frames/s "
                    f"(total {events} events in {frames} frames)")
                last, last_events, last_frames = now, events, frames

            if limit and events >= limit:
                log(f"reached --max {limit}, closing")
                return
    finally:
        # In a finally block so it also runs when the task is cancelled by
        # Ctrl-C or by the peer closing - that is the normal way a
        # measuring run ends.
        elapsed = time.perf_counter() - start
        if quiet and elapsed > 0:
            log(f"received {events} events in {frames} frames over {elapsed:.1f}s "
                f"({events / elapsed:.0f} events/s, {events / max(frames, 1):.1f} events/frame)")


async def run(args: argparse.Namespace) -> int:
    topics = [] if args.topics in (None, "all") else [t.strip() for t in args.topics.split(",") if t.strip()]
    url = build_url(args.url, topics, args.api_key, args.key_in_query)

    headers = {}
    if args.api_key and not args.key_in_query:
        headers["Authorization"] = f"Bearer {args.api_key}"

    log(f"connecting to {url}")
    try:
        # websockets renamed this kwarg in v14; keep working on both.
        try:
            conn = websockets.connect(url, additional_headers=headers)
        except TypeError:
            conn = websockets.connect(url, extra_headers=headers)
        async with conn as ws:
            log("connected. " + ("subscribed: " + ", ".join(topics) if topics else "receiving all topics"))
            if not args.no_interactive:
                log("commands: sub/unsub <topic>, all, none, subs, topics, quit")

            subs = set(topics)
            tasks = [asyncio.create_task(receive(ws, args.pretty, args.max, args.quiet, args.stats_interval))]
            if not args.no_interactive:
                tasks.append(asyncio.create_task(read_commands(ws, subs)))

            stop = asyncio.get_running_loop().create_future()
            with contextlib.suppress(NotImplementedError):
                for sig in (signal.SIGINT, signal.SIGTERM):
                    asyncio.get_running_loop().add_signal_handler(
                        sig, lambda: stop.done() or stop.set_result(None)
                    )

            done, pending = await asyncio.wait([*tasks, stop], return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in done:
                if task is not stop and task.exception():
                    raise task.exception()
    except (OSError, websockets.WebSocketException) as err:
        code = getattr(getattr(err, "response", None), "status_code", None) or getattr(err, "status_code", None)
        if code:
            log(f"handshake rejected: HTTP {code}" + (" - wrong or missing API key?" if code == 401 else ""))
        else:
            log(f"connection failed: {err}")
        return 1

    log("disconnected")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="ws://127.0.0.1:8080/ws", help="WebSocket URL (default: %(default)s)")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("STATUSENGINE_API_KEY"),
        help="API key; defaults to $STATUSENGINE_API_KEY. Sent as 'Authorization: Bearer'.",
    )
    parser.add_argument(
        "--key-in-query",
        action="store_true",
        help="send the key as ?api_key= instead of a header (browser fallback, leaks into logs)",
    )
    parser.add_argument("--topics", help="comma-separated topics to subscribe to on connect ('all' = every topic)")
    parser.add_argument("--pretty", action="store_true", help="pretty-print each payload over multiple lines")
    parser.add_argument("--max", type=int, default=0, help="exit after N events (0 = unlimited)")
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="don't print events; report events/s and frames/s once per --stats-interval. "
        "Use this to measure throughput - printing every event makes the terminal, not the "
        "client or the worker, the bottleneck.",
    )
    parser.add_argument(
        "--stats-interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="how often --quiet reports (default: %(default)s)",
    )
    parser.add_argument("--no-interactive", action="store_true", help="don't read commands from stdin")
    parser.add_argument("--list-topics", action="store_true", help="print the known topics and exit")
    args = parser.parse_args()

    if args.list_topics:
        print("\n".join(TOPICS))
        return 0
    if not args.api_key:
        log("warning: no API key given (--api-key or $STATUSENGINE_API_KEY); the worker logs a generated one at startup")

    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())

# Statusengine Worker (Go)

This project is the next-generation successor to the original PHP Statusengine Worker:
https://github.com/statusengine/worker

It is implemented in Go for maximum concurrency and performance, with a pipeline designed for high-throughput event ingestion, efficient bulk persistence, and low-latency fan-out.

## Built with Vibe Coding

This repository was created using Claude Code and the principles of Vibe Coding. Instead of writing every line of code by hand, I acted as the conductor—guiding the architecture, reviewing the logic, and keeping the "vibe" aligned while Claude handled the heavy lifting of generation and implementation.

## What It Does

- Consumes monitoring events from either Gearman or RabbitMQ.
- Decodes and routes queue payloads by event type.
- Persists events to MySQL using throttled bulk inserts.
- Extracts and forwards perfdata metrics to Graphite (configurable routing).
- Broadcasts events to WebSocket clients with topic-based subscriptions.
- Exposes real-time Prometheus metrics for observability.

## Glossary

A few words in this document mean something narrower than they might elsewhere — especially around Naemon, where several of them are already taken.

| Term | Here it means | Not to be confused with |
|---|---|---|
| **Broker** | The message broker the worker consumes from: **gearmand** (default) or RabbitMQ. When this document says "the backlog waits at the broker", it means jobs sitting in gearmand. | Naemon's **Event Broker Module** (NEB), the shared library loaded *into* the monitoring core. In this stack the NEB module is what *publishes* to the broker — [statusengine/broker](https://github.com/statusengine/broker) — it is not part of this worker and never talks to it directly. |
| **Queue** | One named channel at the broker, e.g. `statusngin_hoststatus`. In Gearman terms this is a *function name*, in RabbitMQ terms a *queue*. Queue names double as WebSocket topics. | The in-process Go channels between the pipeline stages, which this document calls *buffers*. |
| **Job** | One unit of work handed to the worker by the broker. A job carries one payload, which for most queues is a **bulk** array of many events. | A single event. One job typically contains 100. |
| **Event** | One decoded item out of a job's payload — one host check, one status snapshot, one notification. This is what becomes a row and what a WebSocket client receives. | A Naemon "event" in the NEB callback sense. |
| **Handler** | The worker's function for one queue: decode the payload, publish each event to the hub, enqueue each event for insertion. Runs on its own goroutine, one per job. | Naemon's *event handler*, the command run on a state change. That one arrives here as ordinary event data (`event_handler` column). |
| **Worker** | This process. | A Gearman "worker" in the protocol sense — though this process is one of those too, which is why `gearadmin --status` counts it in its last column. |
| **Runner** | Anything with a `Run`/`Flush` pair the pipeline starts and drains: every `BulkInserter` plus the Graphite client. There are 15. | The queue consumer, which has its own `Start`/`Stop` lifecycle. |
| **Batch** | The rows one `INSERT` statement carries — at most 100. Cut purely by size and time, **never** along job boundaries. | A bulk payload. One batch can hold events from several jobs, and one job's events can span several batches. |
| **Flush** | Executing the buffered rows as one bulk `INSERT` and clearing the buffer. Triggered by 100 rows, by the 250ms ticker, or by shutdown. | A Graphite flush, which is the same idea one stage further along. |
| **Topic** | What a WebSocket client subscribes to. Always equal to a queue name. | — |
| **Hub** | The WebSocket pub/sub broadcaster. Has an inbound buffer of its own, hence two distinct drop metrics. | The broker. Nothing is persisted in the hub; a client that is not connected misses the event permanently, by design. |
| **Stale** | For the two status queues only: an event whose envelope timestamp is older than `status_max_age`. Discarded before it reaches MySQL or the hub. | The `status_update_time`-based `DELETE` on core restart, which removes stale *rows* from a previous run. Related idea, different mechanism. |

## Why This Version

Compared to the legacy PHP worker, this Go implementation focuses on:

- Strong concurrency boundaries between ingestion, persistence, and broadcasting.
- Bounded buffering and non-blocking behavior to prevent slow consumers from stalling the pipeline.
- Predictable flush behavior with explicit graceful shutdown semantics.

## Core Components

1. Queue Consumers
- Pluggable backend: Gearman or RabbitMQ.
- Both backends implement a common consumer abstraction.

2. MySQL Bulk Insert Pipeline
- Events are buffered and written in bulk.
- Flush conditions:
	- Batch size reached: 100 rows.
	- Time reached: 250ms since last flush tick.

3. Graphite Routing (Perfdata)
- `statusngin_service_perfdata` can be routed to:
	- MySQL only
	- Graphite only
	- Both

4. WebSocket Hub
- Endpoint: `127.0.0.1:8080/ws` (loopback by default — see `-listen-addr`)
- Clients can subscribe to specific event topics (queue names).
- Subscription can be set:
	- During connect via `?topics=topic1,topic2`
	- At runtime via JSON control frames:
		- `{"subscribe":["statusngin_hoststatus"]}`
		- `{"unsubscribe":["statusngin_hoststatus"]}`
- If no topics are set, the client receives all topics.
- Authentication is **always on** — configuring no key does not disable it, the worker generates a random one per run and logs it as a warning (see `-api-keys` below):
	- Recommended for real clients: `Authorization: Bearer <key>` or `X-Api-Key: <key>` header.
	- `?api_key=<key>` query parameter also accepted, for browser clients that can't set custom headers on a WebSocket handshake (e.g. `web/ws-test-client.html`).
	- An unauthorized request is rejected with HTTP 401 before the handshake upgrades.

5. Prometheus Exporter
- Endpoint: `:9105/metrics`
- Exposes real-time pipeline metrics (queue, DB, websocket, error counters, histograms).

## Graceful Shutdown Behavior

On SIGINT/SIGTERM, the worker performs an ordered shutdown:

1. Stop queue consumption.
2. Flush all pending bulk buffers immediately.
3. Close pipeline goroutines and active WebSocket connections.
4. Shutdown HTTP servers.

This guarantees buffered MySQL rows are written before process exit.

Flushing the buffers is only half of it, though. Queue delivery is at-least-once: a worker that is killed between finishing a job and its acknowledgement reaching the broker gets that job again on restart, with its rows already in MySQL. Every table that can collide on its PRIMARY KEY is therefore written as an upsert, so a redelivery is skipped instead of aborting the whole multi-row `INSERT` and taking the rest of the batch with it. See [MySQL Write Behavior](#mysql-write-behavior) for the full picture and [Verify No Events Are Lost](#verify-no-events-are-lost) for the tool that measures it end to end.

## MySQL Write Behavior

Every rule below exists because dropping a batch was measured to cost real events, twice. This section describes what happens when a write fails, when it is retried, and where data can still be lost.

### The normal path

A queue handler decodes a job and calls `Enqueue` for each event, which hands it to that table's `BulkInserter` over a 100-deep channel. A separate goroutine per table owns the buffer and flushes it as one multi-row `INSERT` when **either** 100 rows have accumulated **or** 250ms have passed since the last flush.

Two consequences are worth internalising, because most of what follows depends on them:

- **A job is acknowledged to the broker as soon as `Enqueue` returns**, not when the row reaches MySQL. The insert happens later, on a different goroutine. A MySQL error therefore never travels back to Gearman, and a failed write is never redelivered.
- **Batches are cut at 100 rows regardless of job boundaries.** One batch routinely holds events from several jobs, so one bad row can take unrelated events down with it. That is exactly how the first data-loss bug did its damage.

### When a failed write is retried

`execWithRetry` in `internal/db/db.go` classifies the error. Only two classes are retried, and the same statement is re-executed unchanged:

| Class | MySQL codes / errors | Attempts | Backoff |
|---|---|---|---|
| **Lock contention** | `1213` deadlock, `1205` lock wait timeout | 3 total | 50ms, then 200ms |
| **Server unreachable** | `driver.ErrBadConn`, `mysql.ErrInvalidConn`, any `net.Error`, `1053` server shutdown, `1040` too many connections, `1927` connection killed | until it succeeds or the context ends | 100ms doubling to a 5s cap |
| **Everything else** | truncation, `NOT NULL`, unknown column, `1062` on a table without an upsert clause | 1 — the batch is dropped | — |

The reasoning behind the split: the first two classes are about *timing*, so the identical statement usually succeeds moments later. Everything else is deterministic and would fail identically three times over, buying nothing but a slower shutdown and a triplicated log line.

Note that MySQL error **2006 ("MySQL server has gone away") never appears in Go** — it is a client-side number from libmysqlclient. `go-sql-driver` reports a lost connection as `ErrInvalidConn`, or as a plain dial error while the server is down. And `database/sql`'s own built-in retry only covers `driver.ErrBadConn`, only three times, and the driver only returns that error when it can prove nothing was written — which is why a server restart needs handling here at all.

### Waiting on an unreachable server is deliberate

While MySQL is unreachable, the flush blocks — and so, in order, do the buffer's goroutine, `Enqueue`, and the queue handler. The consumer's concurrency cap (`-gearman-max-concurrent-jobs`, default 64) then fills, the worker stops taking jobs, and the surplus stays at the broker, where it survives a worker restart and is visible in `gearadmin --status`.

That is the point. A worker that kept accepting jobs and dropping batches would drain the queue into nowhere; measured against a five-second outage, that cost **29,400 of 150,000 events**. Holding instead turns the outage into catch-up time: the same test over a 16-second outage lost nothing.

A permanently broken MySQL therefore stalls the pipeline rather than draining it. The backlog grows, but it is visible and recoverable — the strictly better failure. Watch `statusengine_db_available`; it is `0` for exactly as long as the pipeline is stalled.

### Idempotency: why a retry or a redelivery is harmless

Ten tables with a natural PRIMARY KEY are written as `INSERT ... ON DUPLICATE KEY UPDATE <first PK column> = VALUES(<same column>)`. That update is a genuine no-op — the row only collided because that column already matches — so re-running a statement, or replaying a whole job, changes nothing.

`INSERT IGNORE` would be shorter and is deliberately **not** used: it downgrades *every* error to a warning, including truncation and `NOT NULL` violations, which would make real data problems invisible.

Two tables are knowingly not covered, because neither can collide: `statusengine_logentries` (AUTO_INCREMENT key) and `statusengine_perfdata` (no PRIMARY KEY at all). A redelivery or a mid-statement retry inserts their rows a **second time, silently**. Accepted rather than fixed — both are retention-managed history, and closing it would need a UNIQUE index, i.e. a schema change to a schema owned by openITCOCKPIT. A duplicate row in a history table is the lesser evil against a missing event.

### Where data can still be lost

Four places, in rough order of likelihood:

1. **A permanent SQL error drops its batch** — up to 100 rows, including unrelated events that shared it. Logged as `bulk insert failed, rows dropped` and counted in `statusengine_pipeline_errors_total{component="mysql"}`. In practice this means a schema mismatch, and it should be treated as an incident rather than as noise.
2. **A hard kill loses what is buffered but not yet written** — `SIGKILL`, an OOM kill, a power cut. Up to 200 rows per table (100 in the channel, 100 in the buffer) whose jobs the broker already considers done. This is the at-least-once boundary and cannot be closed without acknowledging per row, which would cost roughly an order of magnitude in throughput. A normal `SIGTERM` is unaffected.
3. **Shutting down while MySQL is unreachable** — the final flush gets a 10-second budget for all 15 runners; whatever cannot be written in that window is dropped. Restarting a worker during a database outage therefore costs its buffers.
4. **Discarded stale status events** — by design, not by accident. `statusngin_hoststatus` and `statusngin_servicestatus` events older than `-status-max-age` (default `5m`) are dropped before MySQL and the WebSocket hub, because they are superseded snapshots. See [Discarding Superseded Status Events](#discarding-superseded-status-events).

### What to watch

| Metric | Healthy | What it means otherwise |
|---|---|---|
| `statusengine_db_available` | `1` | `0` = pipeline stalled on MySQL. Alert on this. |
| `statusengine_pipeline_errors_total{component="mysql"}` | flat | A batch was dropped. Every increment is up to 100 lost rows. |
| `statusengine_db_connection_retries_total` | flat | Climbs for the duration of an outage; measures its length, not a count of incidents. |
| `statusengine_db_batch_retries_total` | ~0 | Lock contention, in practice `db_cleanup` running against a busy table. |
| `statusengine_db_batch_size_at_flush` | mixed | Constant 100 = flushes are batch- rather than ticker-triggered, i.e. saturated. |

`statusengine_db_events_written_total` counts every buffered row as written, including duplicates an upsert skipped, so it briefly overstates after a restart under load. It is a throughput signal, not an audit.

## Discarding Superseded Status Events

`statusngin_hoststatus` and `statusngin_servicestatus` carry a full snapshot of an object's current state, re-sent on every check and upserted into a table that holds exactly one row per object. A snapshot from ten minutes ago has no reader left: MySQL holds a newer one already, and a dashboard showing it would be showing something untrue.

That matters after downtime. These two queues accumulate one job per check interval per object while the worker is not running, and draining that backlog costs exactly as much MySQL time as live traffic would — time the live traffic then spends waiting behind it — to produce rows that are overwritten moments later. Events older than `-status-max-age` (default `5m`) are therefore discarded before they reach either MySQL or the WebSocket hub, letting the worker catch up to the present in seconds.

**This applies to those two queues only.** Every other queue carries history — a check result, a state change, a notification — where each event is a distinct row that nothing will ever supply again, and dropping one on age would be plain data loss.

One caveat deserves attention, because it is silent by nature: the comparison is between the **monitoring core's** clock and **this worker's**. If the two hosts disagree by more than `status_max_age`, every event looks stale and both tables stop being written, with nothing in the log to say so. `statusengine_queue_events_discarded_stale_total` is the only signal, which is why it is worth a dashboard panel even when it is expected to read zero — and why `-status-max-age 0`, which processes everything regardless of age, exists as an escape hatch. The effective setting is logged once at startup.

## Supported Event Topics

The following queue names are also the WebSocket subscription topics:

- `statusngin_hoststatus`
- `statusngin_servicestatus`
- `statusngin_hostchecks`
- `statusngin_servicechecks`
- `statusngin_service_perfdata`
- `statusngin_statechanges`
- `statusngin_logentries`
- `statusngin_notifications`
- `statusngin_contactnotificationmethod`
- `statusngin_acknowledgements`
- `statusngin_downtimes`
- `statusngin_core_restart`

## Build

```bash
go build -o simulator ./cmd/simulator
go build -o gearman_publisher ./cmd/gearman_publisher
go build -o rabbitmq_publisher ./cmd/rabbitmq_publisher
go build -o db_verifier ./cmd/db_verifier
go build -o db_cleanup ./cmd/db_cleanup
go build -o losstest ./cmd/losstest
go build -o worker ./cmd/app
```

## Run Worker

```bash
go run ./cmd/app/main.go
```

Useful flags:

- `-consumer`: `gearman` or `rabbitmq`
- `-gearman-addr`: Gearman server address
- `-rabbitmq-url`: AMQP URL
- `-mysql-dsn`: MySQL DSN
- `-listen-addr`: WebSocket server listen address (default `127.0.0.1:8080` — **loopback only**; exposing it on the network is an explicit opt-in, e.g. `-listen-addr :8080`)
- `-api-keys`: comma-separated API keys accepted by `/ws`. Leaving this empty does **not** disable authentication — the worker generates a random key at startup and logs it as a warning instead, so an unconfigured worker is never an open event stream
- `-metrics-listen-addr`: Prometheus server listen address (default `:9105`)
- `-graphite-addr`: Graphite Carbon address
- `-perfdata-route`: `mysql`, `graphite`, or `both`
- `-status-max-age`: discard `statusngin_hoststatus`/`statusngin_servicestatus` events older than this Go duration (default `5m`); `0` processes every event regardless of age. See [Discarding Superseded Status Events](#discarding-superseded-status-events)

Environment variables with matching names are also supported (for example `STATUSENGINE_CONSUMER`, `STATUSENGINE_MYSQL_DSN`).

### Config file

Settings can also be read from a YAML config file via `-config path/to/config.yaml` (or `STATUSENGINE_CONFIG`). See [`config.example.yaml`](config.example.yaml) for every available key, its default and a description.

Precedence for every setting is: explicit CLI flag > environment variable > config file > built-in default. This lets the config file hold your normal settings while flags/environment variables (handy in Docker/CI) can still override anything for a one-off run.

## Run Database Cleanup

The worker only ever appends to the history tables. `cmd/db_cleanup` is the counterpart that enforces retention: it deletes rows older than a configured number of days and exits, so it belongs in cron or a systemd timer rather than next to the worker.

```bash
go run ./cmd/db_cleanup -config /etc/statusengine/config.yaml
```

It reads **the same config file as the worker** — both binaries ignore each other's keys — and shares `mysql_dsn`, `log_level` and `log_format` with it. Retention is configured per table, in days, separately for hosts and services, using the legacy PHP worker's key names so an existing `config.yml` can be carried over value for value:

| Key | Table | Default |
|---|---|---|
| `age_hostchecks` / `age_servicechecks` | `statusengine_hostchecks` / `statusengine_servicechecks` | 5 |
| `age_host_acknowledgements` / `age_service_acknowledgements` | `statusengine_*_acknowledgements` | 60 |
| `age_host_notifications` / `age_service_notifications` | `statusengine_*_notifications` | 60 |
| `age_host_notifications_log` / `age_service_notifications_log` | `statusengine_*_notifications_log` | 60 |
| `age_host_statehistory` / `age_service_statehistory` | `statusengine_*_statehistory` | 365 |
| `age_host_downtimes` / `age_service_downtimes` | `statusengine_*_downtimehistory` | 60 |
| `age_logentries` | `statusengine_logentries` | 5 |
| `age_perfdata` | `statusengine_perfdata` | 90 |

**`0` disables cleanup of that table entirely** (also the legacy convention). Currently scheduled downtimes and the `hoststatus`/`servicestatus` tables are never touched.

Two more knobs: `cleanup_batch_size` (default 5000) is how many rows each `DELETE` removes — every batch is its own transaction, so smaller values hold locks for shorter and keep replication lag down — and `cleanup_batch_pause` (default `0s`) inserts a pause between batches if the cleanup has to share the database with live check results.

`SIGTERM` and Ctrl-C stop the run cleanly between two batches; whatever was deleted stays deleted and the next run continues from there. The exit code is non-zero only if a table actually failed, so a timer reports real problems and stays quiet otherwise.

Example crontab, nightly at 03:20:

```cron
20 3 * * * /usr/bin/db_cleanup -config /etc/statusengine/config.yaml
```

In a cluster, run this on **exactly one node** — or on several at clearly different times. Simultaneous runs are not dangerous, but they compete for the same locks and finish no sooner.

## Verify No Events Are Lost

`cmd/losstest` answers the one question about the graceful shutdown that reading the code cannot: does a restart under load lose data? Run it before a release, and after any change to the consumer, the shutdown sequence or the bulk-insert path.

It publishes hostcheck events whose hostname is a unique marker, `lt-<run-id>-<seq>`. That column is the first of `statusengine_hostchecks`' PRIMARY KEY, so nothing in the pipeline can merge two of them — a missing sequence number is proof of a lost event, not an artifact of deduplication.

### Before you start

- **Point it at a dev or staging database.** It writes real rows into `statusengine_hostchecks`.
- **Make sure no other worker is connected to the same Gearman server.** It would consume the test events into its own database, and the run would report them as lost. Check with `gearadmin --status`: the last column is the number of connected workers.
- **Build the worker as a binary.** With `go run`, the worker is a *child* process, so your `SIGTERM` hits the parent and the worker keeps running.

```bash
go build -o bin/app ./cmd/app
go build -o bin/losstest ./cmd/losstest
```

### The run

Run from the repo root — the payload fixture is read from `.claude/specs/`.

**1. Build a backlog.** 300,000 events become 3,000 jobs. Publishing up front rather than trickling events in is deliberate: it leaves jobs waiting at the broker, which is the realistic restart scenario and the one that exercises the window where a job is handed over while the consumer is already shutting down.

```bash
./bin/losstest -mode publish -run-id r1 -count 300000
```

**2. Start the worker, interrupt it mid-drain.** Throughput on a developer machine measured between 6,000 and 8,500 events per second, so a 300k backlog leaves well under a minute to react — scale `-count` up if that is too tight on your hardware. Watch the queue drain and send `SIGTERM` somewhere in the middle — not at the very start, and not once it is already empty:

```bash
./bin/app -config /etc/statusengine/config.yaml &
WORKER=$!

gearadmin --status | grep statusngin_hostchecks    # second column = jobs waiting

kill -TERM $WORKER
```

Remember the PID when you start it rather than looking it up later: `pgrep -f bin/app` also matches the shell you type it in, whose command line now contains that string too, and the resulting `kill` takes down your own session along with the worker.

**3. Start the worker again** and let it drain the rest completely, then stop it.

**4. Check what arrived.**

```bash
./bin/losstest -mode verify -run-id r1 -count 300000
```

```
Run "r1", expected 300000 events
  rows in statusengine_hostchecks : 300000
  distinct events  : 300000
  missing          : 0
  still queued     : 0 jobs (~0 events) at localhost:4730

No events lost.
```

`missing: 0` and exit code 0 is the pass condition. **Jobs still queued at the broker are not loss** — they survive a restart by design and are reported separately so the accounting adds up; let the worker finish draining before you judge the result.

If events are missing, the tool prints the gaps as ranges. A contiguous gap that is an exact multiple of 100 is one or more whole jobs that never reached MySQL; check the worker log for `bulk insert failed`.

**5. Remove the test rows.**

```bash
./bin/losstest -mode cleanup -run-id r1
```

Use a fresh `-run-id` per run, or clean up in between — `verify` counts every row belonging to that id, including ones an earlier run left behind. Non-default servers are set with `-server` (Gearman) and `-mysql-dsn`.

## Run Simulator

The simulator replays fixture payloads through the same decode/route/persist pipeline (without requiring a live queue backend):

```bash
go run ./cmd/simulator/main.go
```

## WebSocket Test Client

An interactive test client is included in:

- `web/ws-test-client.html`

It can connect to `ws://localhost:8080/ws`, select topics, subscribe/unsubscribe dynamically, and display incoming event payloads live.

## API Documentation

An OpenAPI 3.1 description of both HTTP-level endpoints (`/ws`'s handshake, authentication and message protocol, plus `/metrics`) lives in [`docs/openapi.yaml`](docs/openapi.yaml), with a real captured example for every event topic.

To browse it as an interactive reference (rendered with [Scalar](https://github.com/scalar/scalar)), open [`docs/index.html`](docs/index.html) directly in a browser, or serve the `docs/` directory with any static file server, e.g.:

```bash
python3 -m http.server 8000 --directory docs
```

then visit `http://localhost:8000`.

## Testing

```bash
go test ./... -v -race
```

## Notes on Queue Payload Shape

Most queues deliver JSON bulk arrays.

Known non-bulk exceptions:

- `statusngin_acknowledgements`
- `statusngin_contactnotificationmethod`
- `statusngin_core_restart`
- `statusngin_downtimes`

## Reference

Legacy PHP implementation:
https://github.com/statusengine/worker

## Truncate all tables (development only)

```SQL
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE statusengine_dbversion;
TRUNCATE TABLE statusengine_host_acknowledgements;
TRUNCATE TABLE statusengine_host_downtimehistory;
TRUNCATE TABLE statusengine_host_notifications;
TRUNCATE TABLE statusengine_host_notifications_log;
TRUNCATE TABLE statusengine_host_scheduleddowntimes;
TRUNCATE TABLE statusengine_host_statehistory;
TRUNCATE TABLE statusengine_hostchecks;
TRUNCATE TABLE statusengine_hoststatus;
TRUNCATE TABLE statusengine_logentries;
TRUNCATE TABLE statusengine_nodes;
TRUNCATE TABLE statusengine_perfdata;
TRUNCATE TABLE statusengine_service_acknowledgements;
TRUNCATE TABLE statusengine_service_downtimehistory;
TRUNCATE TABLE statusengine_service_notifications;
TRUNCATE TABLE statusengine_service_notifications_log;
TRUNCATE TABLE statusengine_service_scheduleddowntimes;
TRUNCATE TABLE statusengine_service_statehistory;
TRUNCATE TABLE statusengine_servicechecks;
TRUNCATE TABLE statusengine_servicestatus;
TRUNCATE TABLE statusengine_tasks;
TRUNCATE TABLE statusengine_users;

SET FOREIGN_KEY_CHECKS = 1;
```
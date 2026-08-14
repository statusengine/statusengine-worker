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
- Endpoint: `:8080/ws`
- Clients can subscribe to specific event topics (queue names).
- Subscription can be set:
	- During connect via `?topics=topic1,topic2`
	- At runtime via JSON control frames:
		- `{"subscribe":["statusngin_hoststatus"]}`
		- `{"unsubscribe":["statusngin_hoststatus"]}`
- If no topics are set, the client receives all topics.
- Authentication (optional, off by default - see `-api-keys` below):
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

Flushing the buffers is only half of it, though. Queue delivery is at-least-once: a worker that is killed between finishing a job and its acknowledgement reaching the broker gets that job again on restart, with its rows already in MySQL. Every table that can collide on its PRIMARY KEY is therefore written as an upsert, so a redelivery is skipped instead of aborting the whole multi-row `INSERT` and taking the rest of the batch with it. See [Verify No Events Are Lost](#verify-no-events-are-lost) for the tool that measures this end to end.

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
- `-listen-addr`: WebSocket server listen address (default `:8080`)
- `-api-keys`: comma-separated API keys accepted by `/ws`. Leaving this empty does **not** disable authentication — the worker generates a random key at startup and logs it as a warning instead, so an unconfigured worker is never an open event stream
- `-metrics-listen-addr`: Prometheus server listen address (default `:9105`)
- `-graphite-addr`: Graphite Carbon address
- `-perfdata-route`: `mysql`, `graphite`, or `both`

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
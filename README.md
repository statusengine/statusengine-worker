# Statusengine Worker (Go)

[![CI](https://github.com/statusengine/statusengine-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/statusengine/statusengine-worker/actions/workflows/ci.yml)

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
| **Job** | One unit of work handed to the worker by the broker. A job carries one payload, which for most queues is a **bulk** array of many events. | A single event. One job typically contains 100, which is unrelated to the batch size below. |
| **Event** | One decoded item out of a job's payload — one host check, one status snapshot, one notification. This is what becomes a row, and what a WebSocket client ultimately reads out of a frame. | A Naemon "event" in the NEB callback sense. |
| **Frame** | One WebSocket message: `{"topic": …, "payload": [ … ]}`, carrying **one job's** events as an array. The unit a client parses, and the unit the Hub buffers and drops. | An event. `payload` is always an array — a single-event job sends an array of one — so a frame usually carries many. |
| **Handler** | The worker's function for one queue: decode the payload, publish the whole batch to the hub as one frame, then enqueue each event for insertion. Runs on its own goroutine, one per job. | Naemon's *event handler*, the command run on a state change. That one arrives here as ordinary event data (`event_handler` column). |
| **Worker** | This process. | A Gearman "worker" in the protocol sense — though this process is one of those too, which is why `gearadmin --status` counts it in its last column. |
| **Runner** | Anything with a `Run`/`Flush` pair the pipeline starts and drains: every `BulkInserter` plus the Graphite client. There are 15. | The queue consumer, which has its own `Start`/`Stop` lifecycle. |
| **Batch** | The rows one `INSERT` statement carries — at most `-mysql-batch-size`, 100 by default. Cut purely by size and time, **never** along job boundaries. | A bulk payload. One batch can hold events from several jobs, and one job's events can span several batches. |
| **Flush** | Executing the buffered rows as one bulk `INSERT` and clearing the buffer. Triggered by the batch size, by the 250ms ticker, or by shutdown. A shutdown flush is the one that can exceed the batch size — see below. | A Graphite flush, which is the same idea one stage further along. |
| **Topic** | What a WebSocket client subscribes to. Always equal to a queue name. | — |
| **Hub** | The WebSocket pub/sub broadcaster. Has an inbound buffer of its own, hence two distinct drop metrics. Both its buffers count frames, so their depth in events scales with how bulky the traffic is. | The broker. Nothing is persisted in the hub; a client that is not connected misses the event permanently, by design. |
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
	- Batch size reached: `-mysql-batch-size` rows (default 100, maximum 700).
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
- **One frame per job, not per event.** A server → client message is `{"topic": "<queue>", "payload": [ … ]}` and `payload` is *always* an array — see [WebSocket Frames](#websocket-frames).
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

## Logging

At the default `-log-level info` the worker logs **lifecycle events and periodic summaries only** — nothing per job, per batch or per row. That is a deliberate constraint rather than an accident of what happened to be written: under systemd the log goes to the journal, and one line per bulk-insert flush is enough to fill a disk.

Measured over a 76-second sustained run writing ~1.96 million rows across two tables:

| | Lines | Log volume |
|---|---|---|
| one line per flush (the old behaviour) | 19,616 | ~145 MB/hour |
| periodic summaries (now) | 20 | ~0.13 MB/hour |

The information is not lost, only aggregated. Every running `BulkInserter` and the Graphite client emit one summary per 30 seconds, matching the interval the consumer and the WebSocket hub already used, so a log read at Info has one rhythm rather than four:

```
level=INFO msg="db: write stats" table=statusengine_hostchecks flushes=4155 rows=415500 rows_per_flush=100 total_processed=741300
level=INFO msg="graphite: write stats" addr=127.0.0.1:2003 flushes=112 metrics=11200 metrics_per_flush=100 total_processed=44800
```

A summary is **skipped entirely when nothing was written**, which is what keeps an idle worker's journal idle: there are fourteen inserters and most tables see no traffic on most installations, and perfdata routes to MySQL only by default, so the Graphite client is usually constructed and never used. A table that stops appearing has stopped writing — that absence is the signal, and `statusengine_db_events_written_total` is the metric that makes it precise. Run's shutdown path emits one final summary, so rows written since the last tick are still reported when a shutdown lands mid-interval.

The per-flush detail still exists and is one flag away:

```bash
./worker -log-level debug   # adds "bulk insert flushed", "metrics flushed", per-row downtime writes
```

Use it to explain a specific flush, not to watch a healthy worker. Errors are unaffected by any of this — a dropped batch, an unreachable MySQL and a failed Graphite write are logged at Warn or Error every single time they happen, never aggregated.

## MySQL Write Behavior

Every rule below exists because dropping a batch was measured to cost real events, twice. This section describes what happens when a write fails, when it is retried, and where data can still be lost.

### The normal path

A queue handler decodes a job and calls `Enqueue` for each event, which hands it to that table's `BulkInserter` over a channel as deep as the batch size. A separate goroutine per table owns the buffer and flushes it as one multi-row `INSERT` when **either** `-mysql-batch-size` rows have accumulated **or** 250ms have passed since the last flush. The batch size defaults to 100 and may be raised to 700; see [Choosing a batch size](#choosing-a-batch-size).

Two consequences are worth internalising, because most of what follows depends on them:

- **A job is acknowledged to the broker as soon as `Enqueue` returns**, not when the row reaches MySQL. The insert happens later, on a different goroutine. A MySQL error therefore never travels back to Gearman, and a failed write is never redelivered.
- **Batches are cut at the batch size regardless of job boundaries.** One batch routinely holds events from several jobs, so one bad row can take unrelated events down with it. That is exactly how the first data-loss bug did its damage — and it is why raising the batch size raises the blast radius with it.

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

While MySQL is unreachable, the flush blocks — and so, in order, do the buffer's goroutine, `Enqueue`, and the queue handler. That queue's concurrency cap (`-gearman-max-concurrent-jobs-per-queue`, default 8) then fills, the worker stops taking jobs **for that queue**, and the surplus stays at the broker, where it survives a worker restart and is visible in `gearadmin --status`. The cap is per queue rather than shared for exactly this reason — see [One connection per queue](#one-connection-per-queue).

That is the point. A worker that kept accepting jobs and dropping batches would drain the queue into nowhere; measured against a five-second outage, that cost **29,400 of 150,000 events**. Holding instead turns the outage into catch-up time: the same test over a 16-second outage lost nothing.

A permanently broken MySQL therefore stalls the pipeline rather than draining it. The backlog grows, but it is visible and recoverable — the strictly better failure. Watch `statusengine_db_available`; it is `0` for exactly as long as the pipeline is stalled.

### Idempotency: why a retry or a redelivery is harmless

Ten tables with a natural PRIMARY KEY are written as `INSERT ... ON DUPLICATE KEY UPDATE <first PK column> = VALUES(<same column>)`. That update is a genuine no-op — the row only collided because that column already matches — so re-running a statement, or replaying a whole job, changes nothing.

`INSERT IGNORE` would be shorter and is deliberately **not** used: it downgrades *every* error to a warning, including truncation and `NOT NULL` violations, which would make real data problems invisible.

Two tables are knowingly not covered, because neither can collide: `statusengine_logentries` (AUTO_INCREMENT key) and `statusengine_perfdata` (no PRIMARY KEY at all). A redelivery or a mid-statement retry inserts their rows a **second time, silently**. Accepted rather than fixed — both are retention-managed history, and closing it would need a UNIQUE index, i.e. a schema migration plus an index on the two highest-volume tables in the database. A duplicate row in a history table is the lesser evil against a missing event.

### Where data can still be lost

Four places, in rough order of likelihood:

1. **A permanent SQL error drops its batch** — up to `-mysql-batch-size` rows, including unrelated events that shared it. Logged as `bulk insert failed, rows dropped` and counted in `statusengine_pipeline_errors_total{component="mysql"}`. In practice this means a schema mismatch, and it should be treated as an incident rather than as noise.
2. **A hard kill loses what is buffered but not yet written** — `SIGKILL`, an OOM kill, a power cut. Up to twice the batch size per table (one batch in the channel, one in the buffer) whose jobs the broker already considers done. This is the at-least-once boundary and cannot be closed without acknowledging per row, which would cost roughly an order of magnitude in throughput. A normal `SIGTERM` is unaffected.
3. **Shutting down while MySQL is unreachable** — the final flush gets a 10-second budget for all 15 runners; whatever cannot be written in that window is dropped. Restarting a worker during a database outage therefore costs its buffers.
4. **Discarded stale status events** — by design, not by accident. `statusngin_hoststatus` and `statusngin_servicestatus` events older than `-status-max-age` (default `5m`) are dropped before MySQL and the WebSocket hub, because they are superseded snapshots. See [Discarding Superseded Status Events](#discarding-superseded-status-events).

WebSocket delivery is deliberately not on that list: the hub drops rather than backpressures, by design, and a client that misses events has lost a view, not data. See [WebSocket Frames](#websocket-frames) for what a drop costs now that a frame is a whole job.

### What to watch

| Metric | Healthy | What it means otherwise |
|---|---|---|
| `statusengine_db_available` | `1` | `0` = pipeline stalled on MySQL. Alert on this. |
| `statusengine_pipeline_errors_total{component="mysql"}` | flat | A batch was dropped. Every increment is up to `-mysql-batch-size` lost rows. |
| `statusengine_db_connection_retries_total` | flat | Climbs for the duration of an outage; measures its length, not a count of incidents. |
| `statusengine_db_batch_retries_total` | ~0 | Lock contention, in practice `db_cleanup` running against a busy table. |
| `statusengine_db_batch_size_at_flush` | mixed | Constant at the batch size = flushes are batch- rather than ticker-triggered, i.e. saturated. Histogram; see below for the counter-only equivalent. |
| `statusengine_db_flushes_total{table}` | rises with load | Successful bulk-insert statements per table. Only useful as the denominator of the row count — see below. |
| `statusengine_graphite_metrics_dropped_total` | `0` | Metrics lost because Carbon was unreachable. Alert on this — unlike the MySQL path there is no backlog to recover them from. |
| `statusengine_graphite_available` | `1` | `0` = metrics are being dropped right now, not merely delayed. |
| `statusengine_websocket_publish_dropped_total` | `0` | The hub's own inbound buffer overflowed — every connected client went blind at once. Alert on this. |
| `statusengine_websocket_messages_dropped_total` | non-zero is normal | One client could not keep up; everyone else got the frame. Counted in events, but lost a frame at a time. |

`statusengine_db_events_written_total` counts every buffered row as written, including duplicates an upsert skipped, so it briefly overstates after a restart under load. It is a throughput signal, not an audit.

### Average batch size without histograms

`statusengine_db_batch_size_at_flush` is a histogram, and it carries no `table` label. Two plain counters give the same answer per table, which is what a monitoring system that ingests only counters needs:

```promql
# Average rows per bulk INSERT, per table
rate(statusengine_db_events_written_total[1m])
  /
rate(statusengine_db_flushes_total[1m])
```

Both counters are incremented in the same branch — successful flushes only — so numerator and denominator always describe the same set of statements. A failed flush appears in `statusengine_pipeline_errors_total{component="mysql"}` instead, rather than dragging the average down for a reason unrelated to batching.

An average approaching `-mysql-batch-size` means flushes are triggered by the batch size rather than the 250ms ticker: that table is saturated. Well below it means the ticker fires first and raising the batch size would change nothing.

The four downtime tables have **no** `db_flushes_total` series. They bypass the buffer and write one row per statement, so the ratio does not apply there — the series is absent rather than zero, which keeps the expression from returning `+Inf`.

### Choosing a batch size

`-mysql-batch-size` defaults to **100** and accepts up to **700**. `-graphite-batch-size` defaults to 100 and accepts up to **1000**. The worker refuses to start on a value outside those ranges rather than quietly clamping it.

Raising it only helps under sustained load. With a 250ms ticker, a batch size of N only binds above roughly 4·N events per second **on that one table** — 100 caps at about 400 rows/s per table, 700 at about 2800. Below that the ticker fires first and the batch size is irrelevant. Against it: a dropped batch costs N events instead of 100, and a longer statement holds locks longer against `db_cleanup`.

The 700 ceiling is arithmetic, not taste. Every flush is sent as a server-side prepared statement (`interpolateParams` is off by default, so `database/sql` falls back to Prepare/Exec/Close), and a prepared statement is limited to **65535 placeholders**. What has to fit is not N rows but `2N-1`: `drainPending` deliberately tops the buffer up from the input channel before a shutdown or core-restart flush, and all of it becomes one statement.

| Batch size | Worst-case rows | × 43 columns | |
|---|---|---|---|
| 700 | 1399 | 60,157 | fits, room for 3 more columns |
| 750 | 1499 | 64,457 | fits, but a 44th column breaks it |
| 1000 | 1999 | 85,957 | **`Error 1390`** |

`Error 1390` ("Prepared statement contains too many placeholders") is deterministic, so it is never retried — every batch would be dropped and `statusengine_hoststatus`/`statusengine_servicestatus` would silently stop being written. `db.NewBulkInserter` panics at construction rather than allow it, and `TestBatchSizeStaysUnderPlaceholderLimit` checks all 14 tables at the ceiling.

Graphite gets a higher ceiling because Carbon's plaintext protocol has no equivalent limit; there the cap only bounds how many metrics a single failed write drops.

## One connection per queue

The Gearman consumer opens **one connection per queue**, each registering only that queue's function, rather than one connection carrying all twelve. That is not tidiness — a single connection lets one busy queue stop every other one.

The library's `Work()` is a single goroutine ranging over a single channel fed by every connection, and it takes a slot from the concurrency budget from inside that loop. Once the budget is gone the send blocks, and while it blocks the loop reads nothing further — for any queue. Since a handler blocks in `Enqueue` for as long as MySQL needs, a `statusngin_servicestatus` backlog parks every slot on MySQL's write rate, and notifications, downtimes and core restarts are then not slow, they are never dispatched at all. It is a liveness problem, not a throughput one.

Measured against a 2,000-job `servicestatus` backlog with 200 `hostchecks` jobs behind it:

| | `hostchecks` after 5s | when it finished |
|---|---|---|
| one shared connection | **0 of 200** | only after the backlog cleared, ~6s |
| one connection per queue | 200 of 200 | within 2s, backlog ~30% done |

This is what the legacy PHP worker gets from forking one client per queue, and what the RabbitMQ consumer here already did with one channel and one consume loop per queue — the two backends were behaving differently, which was never intentional.

### Does RabbitMQ have the same problem?

No, and this is measured rather than inferred from the shape of the code (`TestRabbitMQOneQueueCannotStarveAnother`). The shape alone does not settle it: one channel per queue still means every channel is multiplexed over a **single TCP connection**, and amqp091-go reads that connection in one goroutine which dispatches frames to channels synchronously — structurally the same hazard as the Gearman `Work()` loop above.

What breaks the coupling is inside the library: every consumer gets its own goroutine holding an unbounded buffer between the connection reader and the delivery channel the application ranges over. A handler stuck behind MySQL backs up into that buffer, not into the connection, and `Qos(prefetch)` bounds how far it can back up per channel. The counter-proof — one shared dispatch goroutine across all queues — fails the test deterministically.

One difference between the backends *is* real and worth knowing, because it is not what the flag name suggests:

| | Concurrent handlers per queue | What the cap bounds |
|---|---|---|
| Gearman | `-gearman-max-concurrent-jobs-per-queue` (default 8) | concurrency |
| RabbitMQ | **always 1** | `-rabbitmq-prefetch` bounds *buffered unacked messages*, not concurrency |

`consumeLoop` calls its handler synchronously from a plain `for range` over the delivery channel, so a RabbitMQ queue processes one message at a time no matter how high the prefetch is (measured at prefetch 20: max 1 concurrent handler, 40 × 20 ms of work in 819 ms). Per-queue throughput on the RabbitMQ backend is therefore bounded by one handler's latency. Gearman is the production backend, so this is recorded rather than changed — adding concurrency there would give up in-order processing per queue, which is a design decision rather than a tuning knob.

Two consequences worth knowing:

- **The concurrency cap is per queue** (`-gearman-max-concurrent-jobs-per-queue`, default `8`), so the process-wide worst case is that times the number of queues. It replaces `-gearman-max-concurrent-jobs`, and the old config key or environment variable makes the worker refuse to start — carrying a `64` over would mean 768 concurrent handlers, which is the unbounded-memory situation the cap exists to prevent.
- **`statusengine_queue_jobs_in_flight` is labeled by `queue_name`.** A queue pinned at its cap is that queue falling behind; an unlabeled total cannot tell that apart from every queue being moderately busy. `sum()` without the label gives the old number.

gearmand's `--round-robin` is a related but separate thing: it changes which queue the *server* offers next, and would have spread the shared budget around without removing the coupling, which lived in this process. It is also off by default on gearmand 1.x. Correctness here no longer depends on it.

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
- `-mysql-batch-size`: rows buffered per table before a bulk `INSERT` is flushed ahead of the 250ms ticker (default `100`, maximum `700`). See [Choosing a batch size](#choosing-a-batch-size)
- `-gearman-max-concurrent-jobs-per-queue`: job handlers running at once **per queue** (default `8`). Per queue, not shared — see [One connection per queue](#one-connection-per-queue). Replaces `-gearman-max-concurrent-jobs`, whose value must not be carried over: the worker refuses to start if the old config key or environment variable is still set
- `-rabbitmq-prefetch`: unacknowledged deliveries the broker may push, also per queue (default `100`). This bounds buffered messages, not concurrency — a RabbitMQ queue always processes one message at a time, see [One connection per queue](#one-connection-per-queue)
- `-listen-addr`: WebSocket server listen address (default `127.0.0.1:8080` — **loopback only**; exposing it on the network is an explicit opt-in, e.g. `-listen-addr :8080`)
- `-api-keys`: comma-separated API keys accepted by `/ws`. Leaving this empty does **not** disable authentication — the worker generates a random key at startup and logs it as a warning instead, so an unconfigured worker is never an open event stream
- `-metrics-listen-addr`: Prometheus server listen address (default `:9105`)
- `-graphite-addr`: Graphite Carbon address
- `-graphite-batch-size`: metrics buffered before a Carbon write is flushed ahead of the 250ms ticker (default `100`, maximum `1000`)
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

If events are missing, the tool prints the gaps as ranges. A contiguous gap that is an exact multiple of the publisher's 100 events per job is one or more whole jobs that never reached MySQL (that 100 is the job size, unrelated to `-mysql-batch-size`); check the worker log for `bulk insert failed`.

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

## WebSocket Frames

A server → client message is one **frame** per **job**:

```json
{"topic": "statusngin_hoststatus", "payload": [ {"name": "localhost", ...}, {"name": "db01", ...} ]}
```

`payload` is **always an array**. The four queues that deliver one event per job (`statusngin_acknowledgements`, `statusngin_contactnotificationmethod`, `statusngin_core_restart`, `statusngin_downtimes`) send an array of one, so a client never branches on the payload's shape.

The batching boundary is not invented for the wire — it is the one the data already has. A job arrives from the core as a bulk array, and the worker forwards it whole instead of splitting it into one frame per event. Nothing is delayed to fill a frame, so this costs no latency: a batch is simply however many events the core sent in that job.

What it buys, per job rather than per event: one `json.Marshal`, one hub dispatch, one slot in each client's send buffer, one `write` syscall. And a client's 256-frame buffer now holds 256 *jobs*, which is the difference between roughly a tenth of a second of slack on a busy feed and several seconds of it — enough to absorb a stalling terminal or a garbage-collecting browser tab, which is what a slow client's drops usually turn out to be.

The trade is that a drop is coarser: a full send buffer costs that client the frame's whole batch, not one event. `statusengine_websocket_messages_dropped_total` counts events, so it reports the real loss; it just moves in steps. Frames are counted separately, which makes the average batch size readable without histogram support:

```promql
rate(statusengine_websocket_messages_broadcasted_total[1m])
  /
rate(statusengine_websocket_frames_sent_total[1m])
```

## WebSocket Test Clients

Two interactive test clients are included:

- `web/ws-test-client.html` — browser client: connect, select topics, subscribe/unsubscribe dynamically, watch payloads live.
- `web/ws_client.py` — terminal client, same feature set (`pip install websockets`), plus a measuring mode.

```bash
python3 web/ws_client.py --api-key <key> --topics statusngin_hoststatus
python3 web/ws_client.py --api-key <key> --quiet          # measure, don't print
```

`--quiet` is the mode to use when investigating dropped messages. Printing every event puts a terminal write on the path for each one, and a terminal that stalls for longer than the server's send buffer holds is enough to make the worker drop messages a client could easily have kept up with — so a run that prints measures the terminal, not the pipeline. `--quiet` reports events/s and frames/s once a second and prints nothing else.

## API Documentation

An OpenAPI 3.1 description of both HTTP-level endpoints (`/ws`'s handshake, authentication and message protocol, plus `/metrics`) lives in [`docs/openapi.yaml`](docs/openapi.yaml), with a real captured example for every event topic.

To browse it as an interactive reference (rendered with [Scalar](https://github.com/scalar/scalar)), open [`docs/index.html`](docs/index.html) directly in a browser, or serve the `docs/` directory with any static file server, e.g.:

```bash
python3 -m http.server 8000 --directory docs
```

then visit `http://localhost:8000`.

## Testing

```bash
go test ./... -race -count=1
```

Tests that need a real MySQL, gearmand or RabbitMQ **skip** when it is not reachable, so the suite is usable without setting up all three. That is a convenience locally and a trap in a pipeline: fourteen call sites across eight files skip that way, and they cover the properties that were hardest to get right — that one busy queue cannot starve another, that a redelivered job does not duplicate rows, that `Stop` drains rather than drops. A CI job without those services prints `ok` having verified none of it.

So CI sets one environment variable, and every one of those skips becomes a failure instead:

```bash
STATUSENGINE_TEST_REQUIRE_SERVICES=1 go test ./... -race -count=1
```

Use it locally too when you want to be sure you ran everything. The services it expects are the dev ones documented in `.claude/specs/ressources.txt`: MySQL on `127.0.0.1:3306` (`statusengine-dev`/`statusengine-dev`), gearmand on `127.0.0.1:4730`, RabbitMQ on `127.0.0.1:5672` (`statusengine`/`statusengine`).

### What CI runs

`.github/workflows/ci.yml`, on every push to `main` and every pull request: `gofmt -l`, `go vet`, `go build ./...` (every binary, not just the tested packages) and the suite above with `-race`. MySQL and RabbitMQ come from service containers, gearmand is installed on the runner because it has no official image, and `.claude/specs/mysql_schema.sql` is loaded into the throwaway database — never into a real one, it starts with 22 `DROP TABLE` statements.

`cmd/losstest` is deliberately not part of CI. It needs a sustained multi-second load and a `SIGTERM` timed into the middle of it; see [Verify No Events Are Lost](#verify-no-events-are-lost) and run it before a release.

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
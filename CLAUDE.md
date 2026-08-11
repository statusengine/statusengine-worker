# Project: Go Statusengine Worker

A highly performant, concurrent Go-based event pipeline. It consumes bulk JSON messages from queues (Gearman/RabbitMQ), flushes them via throttled bulk-inserts into MySQL, extracts metrics for Graphite, and broadcasts events selectively via non-blocking WebSockets.

## Tech Stack & Core Packages
- **Language:** Go (Latest stable, strict concurrency compliance)
- **Database:** MySQL 8.0+ (Driver: `go-sql-driver/mysql`)
- **WebSockets:** `gorilla/websocket` or `melody`
- **Legacy Reference:** https://github.com/statusengine/worker (Reference for domain logic, event types, and processing rules)

## System Context & Specs
- **Database Schema:** Read `/.claude/specs/mysql_schema.sql`
- **Queue Payload Examples:** Read JSON dumps in `/.claude/specs/` (Note: Each queue delivers a specific type, but payloads arrive as a JSON bulk array).
- **Queue Payload Bulk Exceptions:** The Queues `statusngin_acknowledgements`, `statusngin_contactnotificationmethod.json`, `statusngin_core_restart.json` and `statusngin_downtimes` do not use bulk payloads.
- **WebSocket/Metrics API Reference:** `docs/openapi.yaml` (OpenAPI 3.1, viewable via `docs/index.html` with Scalar) - the `/ws` message envelope and a real captured example for every event topic, including the `statusngin_downtimes` `type`/`attr` lifecycle table.

## Implementation Status
Every queue is wired end-to-end in `internal/queue/registry.go`'s `NewRouter` - decoded, persisted to MySQL where applicable (rule 3), and broadcast (rule 4):

- **Done** — Host/Service Status, Host/Service Checks, Perfdata (rule 5 MySQL/Graphite routing)
- **Done** — Notifications (`statusngin_notifications`, `statusngin_contactnotificationmethod`)
- **Done** — Acknowledgements (`statusngin_acknowledgements`)
- **Done** — State Changes (`statusngin_statechanges`)
- **Done** — Core Restarts (`statusngin_core_restart`)
- **Done** — Downtimes (`statusngin_downtimes`) - full ADD/LOAD/START/STOP/DELETE lifecycle across the `scheduleddowntimes`/`downtimehistory` table pairs; doesn't use the BulkInserter abstraction (see `.claude/specs/downtime_ablauf.txt` for the processing matrix and why)
- **Done** — Prometheus metrics exporter (`internal/metrics`, served on its own port, default `:9105/metrics`)

## Core Architecture Rules

### 1. Queue Abstraction (Pluggable Ingestion)
- Decouple queue backends using a Go `Consumer` interface (e.g., `Start()`, `Stop()`).
- Support both **Gearman** and **RabbitMQ** as swappable drivers. Gearman is the production backend.
- **gearmand 2.0.0** (released 2026-07-22, previous release 1.1.22) changes **no wire protocol or packet format**, so `mikespook/gearman-go` stays compatible and needs no adaptation. Its three breaking changes are scheduling behaviour: round-robin job assignment is now the default and can no longer be disabled, job priority is now global rather than per function, and the client-side task list is FIFO instead of LIFO (that one affects publishers, not this worker). Round-robin is neutral here: it only changes *which* pending job the server hands out, and this worker cannot be starved by a high-volume queue anyway - the library dispatches every job on its own goroutine and grabs the next immediately, so one queue's handlers never block another's.

### 2. High-Throughput Processing & Concurrency
- **Non-Blocking Architecture:** Ingestion, Database persistence, and WebSocket broadcasting **must** run on separate Goroutines decoupled by channels. WebSockets must never slow down DB ingestion.
- **Worker Pool:** Use buffered channels and bounded worker pools to process decoded queue data concurrently.
- **Backpressure belongs at the broker, not in the process.** A Handler is not a cheap channel write: it decodes the payload and calls `Enqueue` per item, which blocks once that BulkInserter's 100-deep channel is full, so its duration is governed by MySQL. Both consumers are therefore capped - `-gearman-max-concurrent-jobs` (the library otherwise dispatches every job on an unbounded goroutine and never waits) and `-rabbitmq-prefetch` (AMQP otherwise delivers as fast as it can). Surplus then waits at the broker, where it survives a worker restart and is visible via `gearadmin --status` or the RabbitMQ console. Watch `statusengine_queue_jobs_in_flight` (pinned at the cap = falling behind), `statusengine_queue_handler_duration_seconds` and `statusengine_db_batch_size_at_flush` (constant 100 = batch- rather than ticker-triggered, i.e. saturated). Adding a second worker process would make a backlog worse, not better: the bottleneck is behind the intake.

### 3. Throttled Bulk-Inserts (MySQL)
- Implement a **Ticker- & Batch-driven Buffer** for MySQL inserts.
- Messages must be collected and executed as a single Bulk-Insert query as soon as **EITHER** of these conditions is met:
  - The buffer reaches **100 entries**.
  - The oldest item in the buffer reaches **250ms** (`time.Ticker`).

### 4. WebSocket Pub/Sub Broadcaster
- Implement a central WebSocket `Hub` using a publish/subscribe pattern.
- Connected clients can subscribe to specific event types (e.g., only `statusngin_hoststatus`).
- Use non-blocking channel writes (`select { case hub.broadcast <- msg: default: }`) or a dropped-message strategy to prevent slow network clients from backpressuring the pipeline.
- API key authentication on `/ws` is always on (`-api-keys`/`STATUSENGINE_API_KEYS`): real clients send `Authorization: Bearer <key>` or `X-Api-Key`; the `?api_key=` query parameter is accepted too, only for browser clients that can't set headers on a WebSocket handshake (e.g. `web/ws-test-client.html`). Configuring no key does not disable auth - `resolveAPIKeys` in `cmd/app/main.go` generates a random one per run and logs it as a warning, so an unconfigured worker is never an open event stream. The listener binds to `127.0.0.1:8080` by default for the same reason; exposing it on the network is an explicit `-listen-addr` opt-in. See `internal/websocket/auth.go` and the `CheckOrigin` comment in `internal/websocket/client.go` for why origins are deliberately not checked.

### 5. Conditional Perfdata Routing
- Data coming from the `statusngin_service_perfdata` queue contains time-series metrics.
- Implement conditional routing via configuration toggles: write to **MySQL only**, **Graphite only**, or **both**.

### 6. Stability & Lifecycle
- Explicitly handle all errors, reconnect automatically to MySQL/Queues on connection drops.
- Implement full **Graceful Shutdown**: Catch OS signals (SIGINT/SIGTERM), stop the consumer, flush all remaining items from the 250ms DB buffers, cleanly close active WebSocket connections, and then exit.

## Development Commands
- **Initialize & Clean:** `go mod tidy`
- **Run Application:** `go run cmd/app/main.go` - every setting can also come from a YAML config file (`-config path/to/config.yaml` or `STATUSENGINE_CONFIG`, see `config.example.yaml`); precedence is CLI flag > env var > config file > built-in default
- **Test Pipeline:** `go test ./... -race -count=1 -skip TestGearmanConsumerEndToEnd` - the skip is not about a broken test, it is the known upstream race in `gearman-go`'s `Worker.Close()` (see the KNOWN ISSUE comment on `GearmanConsumer.Stop`): it trips the race detector in about half of all runs, and the report attaches to whichever test happens to be running, so without the skip unrelated tests go red. On a machine with no Gearman job server the test skips itself, so a CI without `gearmand` can run the plain `go test ./... -race` and stay green. Run `go test ./internal/queue -race -run TestGearmanConsumerEndToEnd` deliberately now and then to check whether upstream has fixed it.
- **Build Gearman Test Publisher:** `go build -o bin/gearman_publisher cmd/gearman_publisher/main.go` - standalone CLI that publishes synthetic test events for a single queue to a real Gearman job server (`-queue`, `-count`, `-server`), exercising the full Gearman → Router → BulkInserter path from outside the process; run with `go run cmd/gearman_publisher/main.go -queue statusngin_hoststatus -count 1000 -server localhost:4730`
- **Build RabbitMQ Test Publisher:** `go build -o bin/rabbitmq_publisher cmd/rabbitmq_publisher/main.go` - the RabbitMQ counterpart of the above (`-queue`, `-count`, `-server`, an amqp:// URL); run with `go run cmd/rabbitmq_publisher/main.go -queue statusngin_hoststatus -count 1000 -server amqp://statusengine:statusengine@127.0.0.1:5672/`
- **Build DB Shadow Verifier:** `go build -o bin/db_verifier cmd/db_verifier/main.go` - read-only CLI that diffs the most recent rows of the legacy PHP worker's MySQL database against this Go worker's, column by column, to prove shadow-testing data parity (`-dsn-php`, `-dsn-go`, `-tables`, `-limit`, default `-tables` covers every Status/Check/History/Notification/Acknowledgement/Downtime table (excludes `statusengine_dbversion`/`statusengine_nodes`/`statusengine_perfdata`/`statusengine_tasks`/`statusengine_users`/`statusengine_logentries`, still selectable explicitly), default `-limit` 5000); run with `go run cmd/db_verifier/main.go -dsn-php "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine_php" -dsn-go "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine_go"`

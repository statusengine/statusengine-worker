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

## Core Architecture Rules

### 1. Queue Abstraction (Pluggable Ingestion)
- Decouple queue backends using a Go `Consumer` interface (e.g., `Start()`, `Stop()`).
- Support both **Gearman** and **RabbitMQ** as swappable drivers.

### 2. High-Throughput Processing & Concurrency
- **Non-Blocking Architecture:** Ingestion, Database persistence, and WebSocket broadcasting **must** run on separate Goroutines decoupled by channels. WebSockets must never slow down DB ingestion.
- **Worker Pool:** Use buffered channels and bounded worker pools to process decoded queue data concurrently.

### 3. Throttled Bulk-Inserts (MySQL)
- Implement a **Ticker- & Batch-driven Buffer** for MySQL inserts.
- Messages must be collected and executed as a single Bulk-Insert query as soon as **EITHER** of these conditions is met:
  - The buffer reaches **100 entries**.
  - The oldest item in the buffer reaches **250ms** (`time.Ticker`).

### 4. WebSocket Pub/Sub Broadcaster
- Implement a central WebSocket `Hub` using a publish/subscribe pattern.
- Connected clients can subscribe to specific event types (e.g., only `statusengine_hoststatus`).
- Use non-blocking channel writes (`select { case hub.broadcast <- msg: default: }`) or a dropped-message strategy to prevent slow network clients from backpressuring the pipeline.

### 5. Conditional Perfdata Routing
- Data coming from the `statusengine_service_perfdata` queue contains time-series metrics.
- Implement conditional routing via configuration toggles: write to **MySQL only**, **Graphite only**, or **both**.

### 6. Stability & Lifecycle
- Explicitly handle all errors, reconnect automatically to MySQL/Queues on connection drops.
- Implement full **Graceful Shutdown**: Catch OS signals (SIGINT/SIGTERM), stop the consumer, flush all remaining items from the 250ms DB buffers, cleanly close active WebSocket connections, and then exit.

## Development Commands
- **Initialize & Clean:** `go mod tidy`
- **Run Application:** `go run cmd/app/main.go`
- **Test Pipeline:** `go test ./... -v -race`

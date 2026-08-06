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
- `-metrics-listen-addr`: Prometheus server listen address (default `:9105`)
- `-graphite-addr`: Graphite Carbon address
- `-perfdata-route`: `mysql`, `graphite`, or `both`

Environment variables with matching names are also supported (for example `STATUSENGINE_CONSUMER`, `STATUSENGINE_MYSQL_DSN`).

## Run Simulator

The simulator replays fixture payloads through the same decode/route/persist pipeline (without requiring a live queue backend):

```bash
go run ./cmd/simulator/main.go
```

## WebSocket Test Client

An interactive test client is included in:

- `web/ws-test-client.html`

It can connect to `ws://localhost:8080/ws`, select topics, subscribe/unsubscribe dynamically, and display incoming event payloads live.

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
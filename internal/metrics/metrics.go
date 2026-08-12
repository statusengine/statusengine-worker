// Package metrics defines the Prometheus metrics exported by every stage of
// the pipeline (queue ingestion, MySQL bulk-inserts, WebSocket broadcasting)
// and the components they're written from. Every metric is a package-level
// var registered on the default registry via promauto, so any package can
// import statusengine-worker/internal/metrics and call e.g.
// metrics.DBEventsWrittenTotal.WithLabelValues(table).Add(...) directly,
// without needing a registry handed to it. cmd/app/main.go exposes them all
// on a dedicated HTTP server via promhttp.Handler().
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Pipeline component labels for PipelineErrorsTotal.
const (
	ComponentMySQL     = "mysql"
	ComponentWebSocket = "websocket"
	ComponentGraphite  = "graphite"
	ComponentQueue     = "queue"
)

var (
	// QueueMessagesReceivedTotal counts every raw message received from a
	// queue, labeled by queue name (e.g. "statusngin_hoststatus"), regardless
	// of the backend (Gearman or RabbitMQ) that delivered it.
	QueueMessagesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "queue",
		Name:      "messages_received_total",
		Help:      "Total number of messages received per queue.",
	}, []string{"queue_name"})

	// QueueJobsInFlight tracks how many queue messages are being handled
	// right now, across every queue. It is the metric that shows whether
	// the consumer's concurrency cap (-gearman-max-concurrent-jobs) is
	// actually being hit: sitting at the cap means the broker is feeding
	// faster than the pipeline drains, and the backlog is being held at
	// the broker rather than piling up in this process.
	QueueJobsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "statusengine",
		Subsystem: "queue",
		Name:      "jobs_in_flight",
		Help:      "Number of queue messages currently being handled.",
	})

	// QueuePayloadsRepairedTotal counts payloads that were not valid
	// UTF-8 and had their invalid bytes reinterpreted as Windows-1252
	// before decoding (see repairUTF8 in internal/queue). A non-zero
	// value points at a monitoring host emitting non-UTF-8 plugin output
	// - worth fixing at the source, since the repair is a best guess.
	QueuePayloadsRepairedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "queue",
		Name:      "payloads_repaired_total",
		Help:      "Total number of payloads whose invalid UTF-8 was repaired, per queue.",
	}, []string{"queue_name"})

	// QueueHandlerDurationSeconds observes how long handling one message
	// takes, end to end: decode, WebSocket publish and enqueueing every
	// decoded item for insertion. Since Enqueue blocks once a
	// BulkInserter's channel is full, this is where MySQL backpressure
	// becomes visible from the ingestion side.
	//
	// Labeled by queue name, whose cardinality is fixed by the queue list
	// in registry.go - unlike the per-connection client_id label this
	// package used to carry on the WebSocket drop counter.
	QueueHandlerDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "statusengine",
		Subsystem: "queue",
		Name:      "handler_duration_seconds",
		Help:      "Duration of handling one message, per queue.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"queue_name"})

	// DBEventsWrittenTotal counts rows successfully persisted by a bulk
	// insert, labeled by destination table.
	DBEventsWrittenTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "events_written_total",
		Help:      "Total number of events successfully written per table.",
	}, []string{"table"})

	// DBBatchFlushDurationSeconds observes how long each bulk-insert
	// ExecContext call takes, to spot MySQL-side bottlenecks.
	DBBatchFlushDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "batch_flush_duration_seconds",
		Help:      "Duration of bulk-insert flushes to MySQL.",
		Buckets:   prometheus.DefBuckets,
	})

	// DBBatchSizeAtFlush observes how many rows each flush actually
	// contained, to see whether flushes are mostly triggered by the
	// 100-item batch size or the 250ms ticker (CLAUDE.md rule 3).
	DBBatchSizeAtFlush = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "batch_size_at_flush",
		Help:      "Number of rows contained in each bulk-insert flush.",
		Buckets:   []float64{1, 5, 10, 25, 50, 75, 100},
	})

	// WebsocketClientsActive tracks currently connected WebSocket clients.
	WebsocketClientsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "clients_active",
		Help:      "Number of currently connected WebSocket clients.",
	})

	// WebsocketMessagesBroadcastedTotal counts messages successfully handed
	// to a client's send buffer.
	WebsocketMessagesBroadcastedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "messages_broadcasted_total",
		Help:      "Total number of messages broadcasted to WebSocket clients.",
	})

	// WebsocketMessagesDroppedTotal counts messages dropped because a
	// client's send buffer was full (the Hub's non-blocking-select
	// strategy, CLAUDE.md rule 4).
	//
	// Deliberately unlabeled: labeling by client id looks tempting, but a
	// client id is unique per connection, so every reconnect mints a
	// permanent new time series - in this process's registry and, worse,
	// in the scraping Prometheus's TSDB, where deleting the label here
	// wouldn't help. A dashboard that reconnects every few minutes is
	// enough to turn one metric into six figures' worth of series. Which
	// client was too slow is answered instead by the per-client
	// "dropped" count the Hub logs when that client disconnects.
	WebsocketMessagesDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "messages_dropped_total",
		Help:      "Total number of messages dropped for slow WebSocket clients.",
	})

	// PipelineErrorsTotal counts errors encountered anywhere in the
	// pipeline, labeled by the component that hit them (ComponentMySQL,
	// ComponentWebSocket, ComponentGraphite, ComponentQueue).
	PipelineErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "pipeline",
		Name:      "errors_total",
		Help:      "Total number of errors encountered per pipeline component.",
	}, []string{"component"})
)

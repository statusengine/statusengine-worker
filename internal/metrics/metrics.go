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
	// strategy, CLAUDE.md rule 4), labeled by the client that was too slow.
	WebsocketMessagesDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "messages_dropped_total",
		Help:      "Total number of messages dropped for a slow WebSocket client.",
	}, []string{"client_id"})

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

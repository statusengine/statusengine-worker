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

// Components lists every value that appears in PipelineErrorsTotal's
// "component" label.
var Components = []string{ComponentMySQL, ComponentWebSocket, ComponentGraphite, ComponentQueue}

// Why the Init* functions below exist:
//
// A labeled metric has no time series until some label combination is
// used for the first time. An unlabeled Counter shows up in /metrics as 0
// immediately, but statusengine_pipeline_errors_total{component="mysql"}
// simply does not exist until MySQL has actually failed once. For a
// dashboard that is the difference between a panel reading "0" and one
// reading "No data", and for an alert it is the difference between
// rate(...) == 0 and an expression that never evaluates. The healthy
// state is exactly the state that produces no data, which is precisely
// when someone is looking at the dashboard to confirm things are fine.
//
// Pre-creating the child with WithLabelValues (and discarding it) is the
// documented way to fix that: the series is registered at 0 and starts
// counting from there.
//
// This is only safe because every label here is drawn from a small, fixed
// set - queue names, destination tables, the four components above. It
// must not be extended to a label whose values are open-ended (a client
// id, a hostname), where pre-creating would be indistinguishable from a
// cardinality leak.

// InitQueue pre-creates the three per-queue series for queueName, so a
// worker that has not yet received a message on that queue still reports
// zeros for it rather than nothing at all. Called from queue.NewRouter
// for every queue it wires up.
func InitQueue(queueName string) {
	QueueMessagesReceivedTotal.WithLabelValues(queueName)
	QueuePayloadsRepairedTotal.WithLabelValues(queueName)
	QueueHandlerDurationSeconds.WithLabelValues(queueName)
}

// InitStaleDiscards pre-creates the per-queue series on
// QueueEventsDiscardedStaleTotal. Separate from InitQueue because only the
// two status queues can ever discard on age - pre-creating it for all 12
// would advertise a behaviour the other ten do not have.
func InitStaleDiscards(queueName string) {
	QueueEventsDiscardedStaleTotal.WithLabelValues(queueName)
}

// InitTable pre-creates the per-table series on DBEventsWrittenTotal.
// Called from db.NewBulkInserter, so every table this worker can write to
// is covered automatically - including one added later, which is the
// point of putting the call in the constructor rather than in a list that
// would have to be kept in sync by hand.
func InitTable(table string) {
	DBEventsWrittenTotal.WithLabelValues(table)
}

// init pre-creates the per-component series on PipelineErrorsTotal. Unlike
// queue names and table names, this package owns the full set of values
// itself, so there is nothing for a caller to pass in and no way for a
// caller to get it right that this package could not.
func init() {
	for _, component := range Components {
		PipelineErrorsTotal.WithLabelValues(component)
	}
	// A worker that has not written anything yet is not a worker whose
	// database is down - see DBAvailable.
	DBAvailable.Set(1)
}

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

	// QueueEventsDiscardedStaleTotal counts status events dropped for
	// being older than the configured maximum age, before they reached
	// either MySQL or a WebSocket client (see NewStaleDroppingHandler in
	// internal/queue). Only the two status queues can ever appear here.
	//
	// A burst after a restart is the feature working: that is the backlog
	// of superseded snapshots being skipped. A value that keeps climbing
	// while the worker is up is not - it means the monitoring core's clock
	// and this worker's disagree by more than the configured age, in which
	// case both queues are being discarded wholesale and silently.
	QueueEventsDiscardedStaleTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "queue",
		Name:      "events_discarded_stale_total",
		Help:      "Total number of status events discarded for being older than the configured maximum age, per queue.",
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

	// DBBatchRetriesTotal counts how often a bulk insert was re-executed
	// unchanged because MySQL reported a deadlock (1213) or a lock wait
	// timeout (1205). Both are transient and cost nothing but a few
	// hundred milliseconds; what they indicate is contention on a table
	// this worker writes to, in practice another writer such as
	// cmd/db_cleanup. Occasional increments are harmless, a steadily
	// rising value means the two are fighting over the same rows and the
	// cleanup should run at a quieter time or in smaller batches.
	DBBatchRetriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "batch_retries_total",
		Help:      "Total number of bulk-insert retries after a transient MySQL locking failure.",
	})

	// DBConnectionRetriesTotal counts flush attempts that failed because
	// MySQL was unreachable and were therefore repeated. Unlike a lock
	// retry this is not bounded, so the counter climbs for as long as the
	// outage lasts - what it measures is the length of the outage, not a
	// number of incidents. Use DBAvailable to alert, this one to see how
	// often it happens at all.
	DBConnectionRetriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "connection_retries_total",
		Help:      "Total number of bulk-insert attempts repeated because MySQL was unreachable.",
	})

	// DBAvailable is 1 while bulk inserts are succeeding and 0 from the
	// moment one fails with a connection error until one succeeds again.
	// This is the metric to alert on: while it is 0 the pipeline is not
	// losing data (it holds the batch and backpressures to the broker),
	// but it is also not draining, so the backlog at Gearman/RabbitMQ is
	// growing and will need time to catch up afterwards.
	//
	// Pre-set to 1 in init below rather than left at Prometheus' default
	// of 0, which would otherwise read as "MySQL is down" for every
	// worker that has not flushed its first batch yet.
	DBAvailable = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "available",
		Help:      "1 if bulk inserts are reaching MySQL, 0 while the pipeline is stalled on an unreachable server.",
	})

	// DBBatchSizeAtFlush observes how many rows each flush actually
	// contained, to see whether flushes are mostly triggered by the
	// configured batch size or the 250ms ticker (CLAUDE.md rule 3).
	//
	// The buckets have to span the whole configurable range, not just the
	// default of 100: once -mysql-batch-size is raised, buckets that stop
	// at 100 put every flush in +Inf and the saturation signal is gone
	// exactly when it starts to matter. 700 is db.MaxConfigurableBatchSize,
	// the largest a normal flush can be; 1400 covers the drain flush on
	// shutdown, which deliberately overshoots to just under twice that.
	DBBatchSizeAtFlush = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "batch_size_at_flush",
		Help:      "Number of rows contained in each bulk-insert flush.",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 700, 1400},
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

	// WebsocketPublishDroppedTotal counts events dropped by Hub.Publish
	// because the Hub's own inbound buffer was full - the ingestion side
	// giving up rather than backpressuring the pipeline (CLAUDE.md rule
	// 4).
	//
	// Deliberately separate from WebsocketMessagesDroppedTotal above,
	// because the two mean very different things. That one fires when a
	// single client cannot keep up: annoying for that client, invisible
	// to everyone else. This one fires when the Hub's Run goroutine
	// cannot keep up at all, so the event never reaches *any* client -
	// every connected dashboard goes blind at once. Folding them into one
	// counter would hide a total outage inside a number that is routinely
	// non-zero for a single slow browser tab.
	//
	// A rate above zero here means Run is the bottleneck, not the
	// network: it is the metric to alert on.
	WebsocketPublishDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "publish_dropped_total",
		Help:      "Total number of events dropped because the Hub's inbound broadcast buffer was full.",
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

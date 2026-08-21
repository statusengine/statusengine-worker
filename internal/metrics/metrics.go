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
	ComponentCommand   = "command"
)

// Components lists every value that appears in PipelineErrorsTotal's
// "component" label.
var Components = []string{ComponentMySQL, ComponentWebSocket, ComponentGraphite, ComponentQueue, ComponentCommand}

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
	QueueJobsInFlight.WithLabelValues(queueName)
}

// InitStaleDiscards pre-creates the per-queue series on
// QueueEventsDiscardedStaleTotal. Separate from InitQueue because only the
// two status queues can ever discard on age - pre-creating it for all 12
// would advertise a behaviour the other ten do not have.
func InitStaleDiscards(queueName string) {
	QueueEventsDiscardedStaleTotal.WithLabelValues(queueName)
}

// InitFlushes pre-creates the per-table series on DBFlushesTotal. Separate
// from InitTable because only tables written through a BulkInserter batch at
// all: the four downtime tables bypass it and write one row per statement
// (see execDowntimeAction in internal/queue/registry.go). Pre-creating this
// for them would advertise batching they do not do, and - worse - would leave
// a permanent 0 denominator under a climbing events_written_total, so the
// rows-per-flush ratio would read +Inf rather than "not applicable".
func InitFlushes(table string) {
	DBFlushesTotal.WithLabelValues(table)
}

// InitCommands pre-creates the labeled series on the command API's two
// counters: one per accepted command name, one per rejection reason. Called
// from cmd/app when the endpoint is registered.
//
// It matters more here than elsewhere. commands_rejected_total{reason="denied"}
// is the series an operator alerts on - somebody with a valid key tried to
// shut the monitoring core down - and an alert on a series that does not
// exist until the first such attempt is an alert that cannot fire the first
// time it should.
func InitCommands() {
	for _, name := range CommandNames {
		CommandsReceivedTotal.WithLabelValues(name)
	}
	for _, reason := range CommandRejectReasons {
		CommandsRejectedTotal.WithLabelValues(reason)
	}
}

// CommandNames lists every command name the API accepts, and
// CommandRejectReasons every value of the "reason" label. Both are
// duplicated from internal/command rather than imported, because that
// package imports this one - TestCommandMetricLabelsMatchTheCommandPackage
// fails if the copies drift apart.
var (
	CommandNames         = []string{"check_result", "schedule_check", "delete_downtime", "raw"}
	CommandRejectReasons = []string{"auth", "malformed", "unknown_command", "denied", "too_large"}
)

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
// SetBuildInfo publishes the running binary's identity as the single
// statusengine_build_info series. Called once at startup; calling it twice
// with different values would leave both series exported, so it is not.
func SetBuildInfo(version, revision, goVersion string) {
	BuildInfo.WithLabelValues(version, revision, goVersion).Set(1)
}

func init() {
	for _, component := range Components {
		PipelineErrorsTotal.WithLabelValues(component)
	}
	// A worker that has not written anything yet is not a worker whose
	// database is down - see DBAvailable. Same for Carbon, where it also
	// covers the default -perfdata-route mysql, in which the Graphite
	// client is built and never asked to write anything.
	DBAvailable.Set(1)
	GraphiteAvailable.Set(1)
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
	// right now, per queue. It is the metric that shows whether the
	// consumer's concurrency cap (-gearman-max-concurrent-jobs-per-queue)
	// is actually being hit: a queue sitting at the cap means the broker
	// is feeding it faster than the pipeline drains, and its backlog is
	// being held at the broker rather than piling up in this process.
	//
	// Labeled by queue because the Gearman consumer opens one connection
	// per queue with a budget of its own (see queue.GearmanConsumer): the
	// useful question is no longer "is the worker saturated" but "which
	// queue is", and an unlabeled total cannot answer it - a sum of 64
	// looks identical whether it is one queue starving the rest or twelve
	// queues sharing the load evenly. sum(queue_jobs_in_flight) still
	// gives the old process-wide number.
	//
	// Bounded by the number of queue names the Router registers, a
	// compile-time constant set, so this label can never grow without
	// limit. Pre-created at zero for all of them by InitQueue.
	QueueJobsInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "statusengine",
		Subsystem: "queue",
		Name:      "jobs_in_flight",
		Help:      "Number of queue messages currently being handled, per queue.",
	}, []string{"queue_name"})

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

	// DBFlushesTotal counts successful bulk-insert statements per table -
	// the denominator DBEventsWrittenTotal needs to become an average batch
	// size:
	//
	//	rate(db_events_written_total[1m]) / rate(db_flushes_total[1m])
	//
	// DBBatchSizeAtFlush already observes that distribution, but it is a
	// histogram and carries no table label, so it can neither be broken down
	// per table nor read at all by a monitoring system that only ingests
	// counters. Two counters can do both.
	//
	// Counted in the same branch as DBEventsWrittenTotal - successful
	// flushes only - so numerator and denominator always describe the same
	// set of statements. Failed flushes are counted by
	// PipelineErrorsTotal{component="mysql"} instead; mixing them in here
	// would drag the average down for a reason that has nothing to do with
	// batching.
	DBFlushesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "db",
		Name:      "flushes_total",
		Help:      "Total number of successful bulk-insert statements per table.",
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

	// GraphiteMetricsWrittenTotal counts metrics successfully written to
	// Carbon, and GraphiteFlushesTotal the write calls that carried them.
	// The pair is the same counter-ratio idiom as
	// db_events_written_total / db_flushes_total, for the same reason:
	// average metrics per flush, per rate(), without needing a histogram
	// (openITCOCKPIT ingests counters only).
	//
	// "Written" here means the TCP write returned without error. Carbon's
	// plaintext protocol has no acknowledgement of any kind, so this
	// cannot mean "stored" - a Carbon that accepts the bytes and discards
	// them looks identical from here. That is a property of the protocol,
	// not a gap in the instrumentation, and it is why the drop counter
	// below is the more important of the two.
	GraphiteMetricsWrittenTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "graphite",
		Name:      "metrics_written_total",
		Help:      "Total number of metrics written to Carbon.",
	})

	GraphiteFlushesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "graphite",
		Name:      "flushes_total",
		Help:      "Total number of successful writes to Carbon. Denominator for metrics_written_total.",
	})

	// GraphiteMetricsDroppedTotal counts metrics thrown away because a
	// dial or a write to Carbon failed. This is the one Graphite metric
	// worth alerting on, and it exists because the Graphite path fails
	// differently from the MySQL one *by design*: an unreachable MySQL
	// blocks the pipeline and holds its batch (CLAUDE.md rule 3), an
	// unreachable Carbon drops it. Retrying here would either stall the
	// database path behind Graphite or grow the buffer without bound, so
	// MySQL wins and the metrics are lost.
	//
	// That is a deliberate trade, but until now it was an invisible one:
	// pipeline_errors_total{component="graphite"} counts failed *flushes*,
	// so one increment could be one metric or a thousand. This counts the
	// metrics themselves, which is what "how much did we lose" means.
	// Every increment is data that no longer exists anywhere - unlike the
	// MySQL case, there is no backlog at the broker to recover it from.
	GraphiteMetricsDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "graphite",
		Name:      "metrics_dropped_total",
		Help:      "Total number of metrics dropped because Carbon was unreachable or the write failed.",
	})

	// GraphiteAvailable is 1 while writes to Carbon are succeeding and 0
	// from the moment a dial or write fails until one succeeds again -
	// the counterpart to DBAvailable, with one important difference in
	// what it implies. DBAvailable at 0 means the pipeline is stalled but
	// intact; GraphiteAvailable at 0 means metrics are being dropped for
	// as long as it stays there.
	//
	// Pre-set to 1 in init below, like DBAvailable: Prometheus' default of
	// 0 would read as "Carbon is down" on every worker that has not
	// flushed yet - which includes every worker running the default
	// -perfdata-route mysql, where this client is constructed and never
	// used. It only ever drops to 0 after a write was actually attempted
	// and failed.
	GraphiteAvailable = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "statusengine",
		Subsystem: "graphite",
		Name:      "available",
		Help:      "1 if writes are reaching Carbon, 0 while metrics are being dropped.",
	})

	// CommandsReceivedTotal counts external commands accepted by the
	// command API and published onto statusngin_cmd, labeled by command
	// name. It counts *commands*, not requests: one bulk request carrying
	// 50 check results adds 50 here and 1 to CommandsPublishedTotal, the
	// same events-vs-frames split the WebSocket counters use (rule 4).
	CommandsReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "commands",
		Name:      "received_total",
		Help:      "External commands accepted and published, by command name.",
	}, []string{"command"})

	// CommandsRejectedTotal counts requests refused before publishing,
	// labeled by reason: auth, malformed, unknown_command, denied,
	// too_large.
	//
	// reason="denied" is the one to alert on. It means a caller who
	// authenticated successfully asked to shut down or restart the
	// monitoring core, or to have Naemon read commands out of a file -
	// either a badly broken client or someone probing what the key can do.
	// reason="auth" is noisier and less conclusive on an exposed port.
	CommandsRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "commands",
		Name:      "rejected_total",
		Help:      "Command requests refused before publishing, by reason.",
	}, []string{"reason"})

	// CommandsPublishedTotal counts messages handed to the broker - one per
	// request, whatever its bulk size. Together with CommandsReceivedTotal
	// it gives commands per request from two plain counters:
	// rate(commands_received_total[1m]) / rate(commands_published_total[1m]).
	CommandsPublishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "commands",
		Name:      "published_total",
		Help:      "Messages published onto the command queue (one per accepted request).",
	})

	// CommandPublishErrorsTotal counts requests that were valid but could
	// not be handed to the broker, and were answered with a 503. Unlike a
	// dropped Graphite batch this loses nothing: the caller was told, and
	// can send it again.
	CommandPublishErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "commands",
		Name:      "publish_errors_total",
		Help:      "Valid command requests that could not be published (answered with 503).",
	})

	// WebsocketClientsActive tracks currently connected WebSocket clients.
	WebsocketClientsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "clients_active",
		Help:      "Number of currently connected WebSocket clients.",
	})

	// WebsocketMessagesBroadcastedTotal counts events successfully handed to
	// a client's send buffer.
	//
	// Events, not frames: one frame carries a whole queue job's worth of
	// events (see websocket.Hub's wire format), so counting frames here
	// would make this number silently incomparable with
	// queue_events_discarded_stale_total and db_events_written_total, which
	// are the two things anyone reading it wants to compare it against.
	// WebsocketFramesSentTotal below is the frame count, and the ratio of
	// the two is the average batch size per frame.
	WebsocketMessagesBroadcastedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "messages_broadcasted_total",
		Help:      "Total number of events broadcasted to WebSocket clients (events, not frames).",
	})

	// WebsocketFramesSentTotal counts frames handed to a client's send
	// buffer - the denominator of WebsocketMessagesBroadcastedTotal.
	//
	// It exists so the average number of events per frame is readable from
	// two plain counters, without histogram support:
	//
	//	rate(websocket_messages_broadcasted_total[1m])
	//	  / rate(websocket_frames_sent_total[1m])
	//
	// which is the same idiom as db_events_written_total /
	// db_flushes_total. That ratio is what says whether batching is doing
	// anything: at 1.0 every frame carries a single event and the pipeline
	// is idle enough that jobs arrive one event at a time; a rising value
	// means jobs are arriving in bulk, which is also when a client's send
	// buffer is under pressure.
	WebsocketFramesSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "frames_sent_total",
		Help:      "Total number of WebSocket frames handed to clients. Denominator of messages_broadcasted_total.",
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
	//
	// Counted in events, like the broadcast counter above, so the two stay
	// comparable. Note what batching changed about the shape of this
	// number rather than its meaning: a single full send buffer now costs
	// a whole job's worth of events at once instead of one, so this
	// counter moves in coarser steps - while moving far less often, since
	// the same 256-frame buffer now holds hundreds of times more events.
	WebsocketMessagesDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "statusengine",
		Subsystem: "websocket",
		Name:      "messages_dropped_total",
		Help:      "Total number of events dropped for slow WebSocket clients (events, not frames).",
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
		Help:      "Total number of events dropped because the Hub's inbound broadcast buffer was full (events, not frames).",
	})

	// BuildInfo is the standard Prometheus build-info idiom: a gauge that
	// is always 1, carrying the interesting values as labels. It answers
	// "which build is running?" from the same scrape that shows the
	// behaviour, which is when the question actually gets asked - a stale
	// binary and a real regression look identical in a log.
	//
	// Joining on it is what makes it useful:
	//
	//	statusengine_db_events_written_total * on() group_left(version)
	//	  statusengine_build_info
	//
	// The labels are bounded by definition (one build per process), so
	// this is the one case where labeling with a free-form string does not
	// risk unbounded cardinality - unlike a client id or a hostname.
	// Populated by SetBuildInfo below rather than at declaration, so this
	// package does not depend on internal/version.
	BuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "statusengine",
		Subsystem: "",
		Name:      "build_info",
		Help:      "Always 1. Carries the build identity of the running worker in its labels.",
	}, []string{"version", "revision", "go_version"})

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

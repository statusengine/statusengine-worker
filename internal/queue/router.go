package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"statusengine-worker/internal/metrics"
	"statusengine-worker/internal/websocket"
)

// ErrPermanent marks a Handler failure that will fail again identically no
// matter how often the same payload is redelivered - a malformed or
// structurally unexpected message, as opposed to a transient failure like
// MySQL being briefly unreachable. Consumers use errors.Is(err,
// ErrPermanent) to decide whether redelivering is worth anything: without
// that distinction a single undecodable message is nacked-and-requeued
// forever, spinning a CPU core, flooding the log and starving every healthy
// message behind it in the same queue.
var ErrPermanent = errors.New("permanent handler failure")

// decodeError wraps a decode failure so it satisfies errors.Is(err,
// ErrPermanent) while still carrying the topic and the original error for
// the log line. Every Handler that decodes a payload reports its decode
// failures through this one helper.
func decodeError(topic string, err error) error {
	return fmt.Errorf("queue: decode %s: %w: %w", topic, ErrPermanent, err)
}

// Handler decodes one raw queue payload and dispatches every item it
// contains to persistence and/or WebSocket subscribers. A Consumer calls
// the Handler registered for a queue name for every message it receives on
// that queue.
type Handler func(ctx context.Context, payload []byte) error

// observeHandler runs handle and records how long it took plus how many
// handlers are in flight while it does. Both Consumers call it rather than
// invoking a Handler directly, so the two backends cannot drift into
// reporting different things.
//
// These two numbers are what answer "is the worker keeping up?". A
// Handler is not a cheap channel write: it decodes the payload and calls
// Enqueue per item, which blocks once that BulkInserter's channel is full,
// so its duration is ultimately governed by MySQL. Rising duration
// together with an in-flight count pinned at the consumer's concurrency
// cap is the signature of the pipeline falling behind - with the backlog
// waiting at the broker, which is where it belongs.
//
// Two existing signals complement these: DBBatchSizeAtFlush sitting at the
// configured batch size means flushes are batch- rather than ticker-triggered
// (i.e. saturated), and go_goroutines, which promhttp's default registry
// exports on its own, shows whether work is accumulating in-process.
func observeHandler(ctx context.Context, queueName string, handle Handler, payload []byte) error {
	metrics.QueueJobsInFlight.Inc()
	defer metrics.QueueJobsInFlight.Dec()

	// Normalize before anything parses this. Done here rather than in
	// each of the twelve decode functions because both Consumers funnel
	// every payload through this one call - including the downtime and
	// core-restart handlers, which don't go through NewHandler.
	if repaired, changed := repairUTF8(payload); changed {
		payload = repaired
		metrics.QueuePayloadsRepairedTotal.WithLabelValues(queueName).Inc()
		logRepair(queueName)
	}

	start := time.Now()
	err := handle(ctx, payload)
	metrics.QueueHandlerDurationSeconds.WithLabelValues(queueName).Observe(time.Since(start).Seconds())

	return err
}

// Router maps a queue name (e.g. "statusngin_hoststatus") to the Handler
// responsible for decoding and dispatching its messages. Both the Gearman
// and RabbitMQ consumers are driven by the same Router, so the decode and
// dispatch logic for a given queue is written exactly once regardless of
// which broker delivers it.
type Router map[string]Handler

// Runner is the subset of *db.BulkInserter[T] the queue package needs to
// manage its lifecycle without depending on T: starting its Run loop and
// flushing it on graceful shutdown (CLAUDE.md rule 6). Every
// *db.BulkInserter[T], for any T, satisfies this interface.
type Runner interface {
	Run(ctx context.Context)
	Flush(ctx context.Context) error
}

// enqueuer is the subset of *db.BulkInserter[P] a Handler needs to persist
// a decoded item. Declared locally (rather than referencing *db.BulkInserter
// directly) so Handlers stay testable without a real database.
type enqueuer[P any] interface {
	Enqueue(ctx context.Context, item P) error
}

// NewHandler builds a Handler for a queue whose decoded items must reach
// both MySQL and WebSocket subscribers: decode turns the raw payload into
// its individual items; the whole batch is published on hub under topic as
// one frame, then each item is enqueued on ins for bulk-insertion
// (CLAUDE.md rule 2 - the actual DB write and WebSocket dispatch happen
// asynchronously in ins's and hub's own goroutines, decoupled from this
// call by their respective channels). Broadcasting first is deliberate;
// see publishBatch.
func NewHandler[P any](hub *websocket.Hub, topic string, ins enqueuer[P], decode func([]byte) ([]P, error)) Handler {
	return func(ctx context.Context, payload []byte) error {
		items, err := decode(payload)
		if err != nil {
			return decodeError(topic, err)
		}

		publishBatch(hub, topic, items)

		for _, item := range items {
			if err := ins.Enqueue(ctx, item); err != nil {
				return fmt.Errorf("queue: enqueue %s event: %w", topic, err)
			}
		}
		return nil
	}
}

// timestamped is implemented by the event types whose freshness decides
// whether they are worth processing at all - see NewStaleDroppingHandler.
// The value is the unix second the monitoring core produced the event,
// taken from the message Envelope, and is also what lands in the row's
// status_update_time column.
type timestamped interface {
	eventTimestamp() int64
}

// NewStaleDroppingHandler builds a Handler like NewHandler, except items
// older than maxAge are discarded before they reach either MySQL or the
// WebSocket hub. A maxAge of zero or less disables the filter entirely,
// making this behave exactly like NewHandler.
//
// This is only correct for the two status queues, and using it anywhere
// else would silently lose data. statusngin_hoststatus and
// statusngin_servicestatus carry a full *snapshot* of an object's current
// state, re-sent on every check, and each one overwrites its predecessor
// in an upserted single-row-per-object table. A snapshot from ten minutes
// ago therefore has no reader: MySQL holds a newer one already or is about
// to, and a dashboard showing it would be showing something untrue. Every
// other queue carries history - a check result, a state change, a
// notification - where each event is a distinct row that nothing else will
// ever supply again, so age is no reason to drop it.
//
// What this is for: while the worker is down, these two queues accumulate
// one job per check interval per object, and none of that backlog is worth
// writing on restart. Draining it costs exactly as much MySQL time as
// live traffic would, which is time the live traffic then waits for.
// Dropping it lets the worker catch up to the present in seconds rather
// than minutes.
//
// Note the failure mode this creates, because it is silent by nature: the
// comparison is between the *core's* clock and this worker's. If the two
// hosts disagree by more than maxAge, every event looks stale and the two
// queues stop being written at all, with nothing in the log to say so.
// statusengine_queue_events_discarded_stale_total is the only signal, so
// it is worth a dashboard panel even when it is expected to be zero.
func NewStaleDroppingHandler[P timestamped](hub *websocket.Hub, topic string, ins enqueuer[P], decode func([]byte) ([]P, error), maxAge time.Duration) Handler {
	if maxAge <= 0 {
		return NewHandler(hub, topic, ins, decode)
	}

	return func(ctx context.Context, payload []byte) error {
		items, err := decode(payload)
		if err != nil {
			return decodeError(topic, err)
		}

		// One cutoff for the whole payload rather than one time.Now() per
		// item: a bulk message is decoded in microseconds, so the extra
		// precision would be noise, and this runs on the hottest path in
		// the worker.
		cutoff := time.Now().Add(-maxAge).Unix()

		// The same cutoff the loop below applies, so subscribers see
		// exactly what MySQL gets - a stale snapshot is no more use to a
		// dashboard than it is to the database.
		publishFiltered(hub, topic, items, func(item P) bool {
			return item.eventTimestamp() >= cutoff
		})

		var dropped int
		for _, item := range items {
			if item.eventTimestamp() < cutoff {
				dropped++
				metrics.QueueEventsDiscardedStaleTotal.WithLabelValues(topic).Inc()
				continue
			}

			if err := ins.Enqueue(ctx, item); err != nil {
				return fmt.Errorf("queue: enqueue %s event: %w", topic, err)
			}
		}

		if dropped > 0 {
			// Debug, not Info: draining a backlog produces one of these per
			// job, and the counter above is the metric that actually
			// answers "how much was dropped".
			slog.Debug("queue: discarded stale status events",
				"topic", topic, "dropped", dropped, "of", len(items), "max_age", maxAge)
		}
		return nil
	}
}

// NewBroadcastHandler builds a Handler for a queue with no MySQL
// destination (e.g. no matching table, or a not-yet-implemented routing
// target such as Graphite): every decoded item is only published to hub
// under topic.
func NewBroadcastHandler[P any](hub *websocket.Hub, topic string, decode func([]byte) ([]P, error)) Handler {
	return func(_ context.Context, payload []byte) error {
		items, err := decode(payload)
		if err != nil {
			return decodeError(topic, err)
		}

		publishBatch(hub, topic, items)
		return nil
	}
}

// publishBatch JSON-encodes items as one array and publishes it to hub
// under topic as a single frame, logging (rather than failing the whole
// dispatch) on encode errors - one unencodable batch must never take down
// the job around it.
//
// One frame per job, not per event, and the batching boundary is the one
// the data already has: a job arrives from the broker as a bulk array, and
// exploding it into N frames only to have every stage downstream pay N
// times is work this pipeline was creating for itself. Per job instead of
// per event, that is one json.Marshal, one Hub channel send, one dispatch
// map walk, one entry in each client's send buffer and one write syscall
// in writePump.
//
// It also fixes an ordering problem that batching makes unavoidable to
// notice: every caller used to alternate publish and Enqueue per event, so
// an event's broadcast waited on the previous event's database write - and
// Enqueue blocks whenever MySQL is the bottleneck (CLAUDE.md rule 3).
// Publishing the whole batch before the enqueue loop is what rule 2 asks
// for and what the old shape quietly wasn't.
//
// Encoding is skipped entirely when no client is connected. This sits on
// the hottest path in the worker - it runs once per job, for every queue -
// and a production worker normally has nobody attached to /ws at all, so
// without this check every ingested job pays for a full reflection-driven
// marshal (and the garbage that comes with it) to produce bytes that are
// then immediately discarded by Publish.
func publishBatch[P any](hub *websocket.Hub, topic string, items []P) {
	if len(items) == 0 || !hub.HasClients() {
		return
	}

	raw, err := json.Marshal(items)
	if err != nil {
		slog.Error("queue: failed to encode events for websocket",
			"topic", topic, "events", len(items), "error", err)
		return
	}
	hub.Publish(topic, raw, len(items))
}

// publish is publishBatch for the queues that deliver one event per job
// (CLAUDE.md's bulk exceptions). It still sends an array, of one element,
// so clients never have to branch on the payload's shape.
func publish[P any](hub *websocket.Hub, topic string, item P) {
	if !hub.HasClients() {
		return
	}
	publishBatch(hub, topic, []P{item})
}

// publishFiltered publishes only the items keep returns true for, as one
// frame. For the handlers that persist a subset of a job's events and
// broadcast exactly that same subset.
//
// The filtered slice is built inside the HasClients guard rather than
// reusing the one the caller's enqueue loop walks: those loops skip with
// continue and allocate nothing, and a worker with no /ws client attached -
// the normal case - should not start allocating a slice per job just
// because someone might connect later. Evaluating keep a second time is
// cheaper than that, and only happens while a client is actually watching.
func publishFiltered[P any](hub *websocket.Hub, topic string, items []P, keep func(P) bool) {
	if !hub.HasClients() {
		return
	}

	kept := make([]P, 0, len(items))
	for _, item := range items {
		if keep(item) {
			kept = append(kept, item)
		}
	}
	publishBatch(hub, topic, kept)
}

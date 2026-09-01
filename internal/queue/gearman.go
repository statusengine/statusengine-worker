package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	gearman "github.com/mikespook/gearman-go/worker"

	"statusengine-worker/internal/metrics"
)

// outboundBufferSize is the capacity of the raw-Message channel Start
// returns. Sends onto it are best-effort (see Start): a full buffer never
// blocks job processing.
const outboundBufferSize = 256

// statsLogInterval is how often a running consumer logs a summary of
// messages processed. Per-message logging would put a log call on the hot
// job-handling path; aggregating into one line per interval keeps
// structured logging visible without slowing down ingestion (CLAUDE.md
// rule 2).
const statsLogInterval = 30 * time.Second

// GearmanConsumer implements queue.Consumer against a Gearman job server.
// It opens one connection per queue name present in its Router, each
// registering exactly that one function; each job's payload is decoded and
// dispatched by the matching Handler.
//
// One connection per queue rather than one carrying all twelve functions,
// because the concurrency cap is otherwise a single budget that whichever
// queue has a backlog holds entirely. The library's Work loop is one
// goroutine ranging over one channel fed by every agent, and handleInPack
// sends to worker.limit from inside it (worker.go:198); that send blocks
// once the cap is reached, and while it blocks the loop reads nothing
// further from any agent, for any function. Since a Handler blocks in
// Enqueue for as long as MySQL needs (CLAUDE.md rule 3), a status backlog
// parks every slot on MySQL's write rate - and notifications, downtimes
// and core restarts are then not merely slow, they are not looked at at
// all. That is a liveness problem, not a throughput one.
//
// Splitting the connections gives each queue its own Work loop and its own
// budget, which is what makes the fix independent of the server: the Go
// client only ever sends GRAB_JOB_UNIQ, so *which* queue it is offered is
// gearmand's decision, and gearmand's --round-robin (off by default) would
// only spread the shared budget around rather than remove the coupling.
// This is also what the legacy PHP worker achieves by forking one client
// per queue, and what the RabbitMQ consumer here already does with one
// channel and one consumeLoop per queue.
type GearmanConsumer struct {
	addr   string
	router Router

	// maxConcurrentJobsPerQueue caps how many job handlers may run at once
	// *for one queue*. See NewGearmanConsumer for why this must never be
	// gearman.Unlimited.
	maxConcurrentJobsPerQueue int

	// mu guards queues (the *gearman.Worker inside each entry is swapped
	// by superviseReconnects) and out. Closing stopping happens under it
	// too - see Stop for why that is the whole synchronisation between a
	// reconnect landing and a shutdown starting.
	mu     sync.Mutex
	queues []*queueWorker
	out    chan Message

	stopOnce  sync.Once
	stopping  chan struct{}
	statsDone chan struct{}

	// handlerWG counts job handlers currently in flight, so Stop can wait
	// for them before closing out (which they send on). stoppedMu/stopped
	// guard the WaitGroup itself: sync.WaitGroup forbids an Add that runs
	// concurrently with a Wait once the counter has reached zero, and job
	// handlers - which run on the library's own agent goroutines - would
	// otherwise Add at exactly the moment Stop Waits. Handlers take the
	// read lock to Add; Stop takes the write lock to publish stopped=true,
	// which both establishes the happens-before Wait needs and guarantees
	// no further Add can start.
	stoppedMu sync.RWMutex
	stopped   bool
	handlerWG sync.WaitGroup

	// processed/errors/reconnects count activity since Start, for the
	// periodic stats log. Incremented from per-connection job-handler
	// goroutines and the per-queue reconnect supervisors, hence atomic.
	processed  atomic.Uint64
	errors     atomic.Uint64
	reconnects atomic.Uint64
}

// queueWorker is one queue's connection to the job server, together with
// what it takes to rebuild it. The *gearman.Worker is a generation rather
// than an identity: superviseReconnects replaces it wholesale after a
// disconnect, which is why every reader takes GearmanConsumer.mu.
type queueWorker struct {
	queue  string
	handle Handler

	// w is the current generation. Read and written only under
	// GearmanConsumer.mu.
	w *gearman.Worker

	// lost carries one reconnect request from the worker's ErrorHandler to
	// this queue's supervisor. Capacity 1 and only ever written to with a
	// non-blocking send: the ErrorHandler runs on the agent goroutine with
	// the agent's own mutex held (agent.go's disconnect_error), so blocking
	// there would deadlock anything that needs that mutex - including the
	// reconnect itself. One pending request is all the supervisor needs,
	// since it rebuilds the whole connection regardless of how many times
	// it was told.
	lost chan struct{}
}

// NewGearmanConsumer creates a consumer that will connect to the Gearman
// job server at addr (host:port) and handle every queue name in router,
// running at most maxConcurrentJobsPerQueue handlers at a time *per
// queue* - so the worst case across the whole process is that number
// times len(router).
//
// That cap is not a throughput knob, it is what keeps a backlog at the
// job server instead of inside this process. The library dispatches every
// assigned job on its own goroutine and immediately grabs the next
// without waiting (worker.go's handleInPack), so with gearman.Unlimited
// there is no upper bound on either. A Handler is not cheap - it decodes
// the payload and then blocks in Enqueue whenever a BulkInserter's
// channel is full - so if the core produces faster than MySQL absorbs,
// goroutines accumulate, each holding a decoded payload, until the
// process runs out of memory. The worst moment for that is exactly the
// one that produces the largest backlog: restarting after an outage.
//
// With a cap, that queue's Work loop blocks handing out the next job once
// the cap is reached, stops reading its socket, and the backpressure
// reaches gearmand over TCP. The backlog then waits where it survives a
// worker restart and is visible via `gearadmin --status`.
//
// Per queue rather than shared: a shared budget is a budget one backlogged
// queue takes all of, which starves every other queue outright - see the
// type comment and TestOneQueueCannotConsumeTheWholeJobBudget. A per-queue
// cap costs nothing for an idle queue, since a cap only binds when there
// is traffic to bind.
//
// maxConcurrentJobsPerQueue must be >= 1: gearman.Unlimited is 0, so
// passing zero would silently restore the unbounded behaviour this exists
// to prevent.
func NewGearmanConsumer(addr string, router Router, maxConcurrentJobsPerQueue int) *GearmanConsumer {
	return &GearmanConsumer{
		addr:                      addr,
		router:                    router,
		maxConcurrentJobsPerQueue: maxConcurrentJobsPerQueue,
		statsDone:                 make(chan struct{}),
		stopping:                  make(chan struct{}),
	}
}

// Start connects to the Gearman job server - once per queue in the Router,
// each connection registering only that queue's function - and begins
// processing jobs. It returns a channel carrying a copy of every raw
// message received, for observability; the actual decode/persist/broadcast
// work happens inside the Router's Handlers, invoked synchronously as each
// job arrives.
func (c *GearmanConsumer) Start(ctx context.Context) (<-chan Message, error) {
	// Rejected rather than tolerated: the connections are opened inside
	// the per-queue loop below, so an empty Router would mean Start
	// returning success having connected to nothing and consuming nothing
	// forever. Back when a single connection was opened before the loop,
	// that case at least failed on an unreachable server; now it would
	// not, and a misconfiguration that leaves the Router empty is exactly
	// the kind that must not look healthy.
	if len(c.router) == 0 {
		return nil, errors.New("gearman: no queues to consume, the Router is empty")
	}

	out := make(chan Message, outboundBufferSize)
	queues := make([]*queueWorker, 0, len(c.router))

	// Anything that fails partway through leaves the connections opened so
	// far with nobody to close them, so unwind before returning. Note that
	// Close cannot actually drop them: the library returns from it early
	// while running is false, and running is only set by Work, which has
	// not been started yet. Those sockets go when the process does - which
	// it always does, since a failed Start is fatal in cmd/app.
	fail := func(err error) (<-chan Message, error) {
		for _, qw := range queues {
			qw.w.Close()
		}
		return nil, err
	}

	for queueName, handle := range c.router {
		qw := &queueWorker{
			queue:  queueName,
			handle: handle,
			lost:   make(chan struct{}, 1),
		}

		w, err := c.newWorker(ctx, qw, out)
		if err != nil {
			return fail(err)
		}
		qw.w = w

		queues = append(queues, qw)
	}

	c.mu.Lock()
	c.queues = queues
	c.out = out
	c.mu.Unlock()

	for _, qw := range queues {
		go qw.w.Work()
		go c.superviseReconnects(ctx, qw, out)
	}
	go c.logStatsPeriodically(ctx)

	go func() {
		<-ctx.Done()
		c.Stop()
	}()

	inOrder := 0
	for queueName := range c.router {
		if RequiresInOrderProcessing(queueName) {
			inOrder++
		}
	}
	slog.Info("gearman: consumer started",
		"addr", c.addr, "queues", len(c.router),
		"max_concurrent_jobs_per_queue", c.maxConcurrentJobsPerQueue,
		"serialized_queues", inOrder)

	return out, nil
}

// newWorker opens one connection to the job server for qw's queue,
// registers that queue's function on it and reports it ready to work. It
// does not start the Work loop - the caller does, because Start and
// superviseReconnects hang the worker into different places first.
//
// Shared by both of those on purpose: a connection rebuilt after a drop
// has to be configured identically to the one opened at startup, and the
// only way to guarantee that is for there to be one piece of code that
// configures it.
func (c *GearmanConsumer) newWorker(ctx context.Context, qw *queueWorker, out chan Message) (*gearman.Worker, error) {
	// Not gearman.Unlimited - see NewGearmanConsumer. Note the library
	// buffers limit-1 tokens (worker.go's New) and sends one only after
	// spawning the job's goroutine, so New(n) permits exactly n concurrent
	// handlers and New(1) serializes them. That is correct as written; it
	// only looks like an off-by-one.
	//
	// A queue whose messages build on one another gets 1 regardless of the
	// configured cap, which is what makes New(1)'s serialization
	// load-bearing rather than a curiosity. See RequiresInOrderProcessing:
	// statusngin_downtimes delivers a downtime's ADD/START/STOP/DELETE as
	// separate jobs, and handling them concurrently silently corrupts the
	// two downtime table pairs whenever a backlog makes them overlap.
	// Costs nothing: that queue sees a handful of messages an hour, and
	// per-queue concurrency buys no throughput anyway (CLAUDE.md rule 2 -
	// the bottleneck is the single Run goroutine per table).
	w := gearman.New(c.jobLimitFor(qw.queue))
	if err := w.AddServer(gearman.Network, c.addr); err != nil {
		return nil, fmt.Errorf("gearman: connect to %s for %q: %w", c.addr, qw.queue, err)
	}
	w.ErrorHandler = c.errorHandler(qw)

	// Exactly one function per worker: that is the whole point of the
	// split (see the type comment). Registering a second here would
	// silently restore the shared budget for those two queues.
	if err := w.AddFunc(qw.queue, c.jobHandler(ctx, qw.queue, qw.handle, out), 0); err != nil {
		return nil, fmt.Errorf("gearman: register function %q: %w", qw.queue, err)
	}
	if err := w.Ready(); err != nil {
		return nil, fmt.Errorf("gearman: ready for %q: %w", qw.queue, err)
	}

	// Set here rather than pre-created at 1 in metrics.InitQueue, so the
	// series means "this queue has a connection" and not "the Router was
	// wired up". Ready has dialled and sent CAN_DO by this point.
	metrics.QueueConnected.WithLabelValues(qw.queue).Set(1)

	return w, nil
}

// jobLimitFor is the concurrency cap to open one queue's connection with:
// the configured per-queue cap, or 1 for a queue that must be processed in
// order. Split out so Start can report how many queues are serialized
// without repeating the rule.
func (c *GearmanConsumer) jobLimitFor(queueName string) int {
	if RequiresInOrderProcessing(queueName) {
		return 1
	}
	return c.maxConcurrentJobsPerQueue
}

// errorHandler builds the gearman.ErrorHandler for one queue. Its real job
// is to notice that this queue's only connection has died, because nothing
// else in the library will: agent.work reports the disconnect here and then
// returns, leaving Work() ranging over a channel no agent will ever send on
// again (worker.go's `for inpack = range worker.in`, which only Close
// terminates). The worker then sits there looking healthy and consumes
// nothing, for good - which is exactly what a lost job server used to cost
// this process.
//
// Only a *gearman.WorkerDisconnectError means that. Handler failures reach
// the same ErrorHandler (handleInPack routes exec's error through
// worker.err), and treating those as a disconnect would tear down a working
// connection every time MySQL rejected a batch.
func (c *GearmanConsumer) errorHandler(qw *queueWorker) func(error) {
	return func(err error) {
		var disconnected *gearman.WorkerDisconnectError
		if !errors.As(err, &disconnected) {
			slog.Warn("gearman: worker error", "queue", qw.queue, "error", err)
			return
		}

		metrics.QueueConnected.WithLabelValues(qw.queue).Set(0)
		slog.Warn("gearman: connection lost, reconnecting",
			"queue", qw.queue, "error", err, "retry_interval", reconnectDelay)

		// Non-blocking, and it has to be: this runs on the agent goroutine
		// while agent.disconnect_error holds the agent's mutex, so blocking
		// here would hold that mutex for as long as the supervisor took to
		// answer. One queued request is enough - see queueWorker.lost.
		select {
		case qw.lost <- struct{}{}:
		default:
		}
	}
}

// superviseReconnects rebuilds one queue's connection after it is lost,
// retrying every reconnectDelay until it succeeds, ctx is cancelled or Stop
// is called. One goroutine per queue, so twelve queues dropped by a single
// job server restart come back in parallel rather than one after another -
// the same reason closeWorkers closes them in parallel.
//
// It builds a fresh gearman.Worker rather than calling the library's
// WorkerDisconnectError.Reconnect(), for two independent reasons. The first
// is decisive: agent.disconnect_error calls the ErrorHandler while holding
// the agent's mutex, and reconnect() takes that same mutex, so reconnecting
// from where the disconnect is reported is a self-deadlock - it has to
// happen on another goroutine anyway. The second is that reconnect() reuses
// the dead agent in place: it overwrites a.conn without closing the old
// socket (leaving the fd to the GC finalizer), it takes the agent mutex and
// then the worker mutex, which is the reverse of AddFunc -> broadcast ->
// agent.Write, and it calls agentWG.Add on a WaitGroup that Close may be
// waiting on. Rebuilding uses only New/AddServer/AddFunc/Ready/Work/Close,
// which run on every startup and in every test in this package, and closing
// the dead generation disposes of its socket properly.
//
// Jobs that were in flight when the connection dropped lose their
// acknowledgement, so the job server hands them out again after the
// reconnect. That is harmless here and it is not an accident: it is exactly
// the redelivery CLAUDE.md rule 6's upserts exist to absorb.
func (c *GearmanConsumer) superviseReconnects(ctx context.Context, qw *queueWorker, out chan Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopping:
			return
		case <-qw.lost:
		}

		c.reconnects.Add(1)
		metrics.QueueReconnectsTotal.WithLabelValues(qw.queue).Inc()

		// End the dead generation before building the next one. Its Work
		// loop is parked forever on a channel nothing will send to again,
		// and its agent still holds the dead socket; Close is what releases
		// both, after letting the handlers still running report their
		// result (bounded by the fork's DrainTimeout, 30s).
		//
		// Before the retry loop rather than after, so that worst case falls
		// inside an outage the worker is waiting out anyway instead of
		// being added on top of a reconnect that would otherwise succeed.
		c.mu.Lock()
		dead := qw.w
		c.mu.Unlock()
		dead.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopping:
				return
			case <-time.After(reconnectDelay):
			}

			fresh, err := c.newWorker(ctx, qw, out)
			if err != nil {
				slog.Warn("gearman: reconnect attempt failed", "queue", qw.queue, "error", err)
				continue
			}

			// Hanging the new generation in and observing that a shutdown
			// has begun must not be able to interleave, which is why Stop
			// closes c.stopping under this same mutex. Either this lands
			// first and Stop's snapshot includes it, or Stop got there
			// first and this generation is not in that snapshot - in which
			// case it must not be allowed to consume, because Stop has
			// already returned and the BulkInserters behind it are being
			// flushed.
			//
			// What makes that safe is the order below, not the Close: a
			// worker only asks for jobs once Work sends its first
			// GRAB_JOB_UNIQ, and Work is started after this check. The
			// Close is best-effort cleanup and known to be a no-op here,
			// since the library returns early from it while running is
			// false - the same reason Start's fail unwind above cannot
			// close what it opened either. What is left behind is an idle
			// socket that has sent CAN_DO and will never grab, released
			// when the process exits.
			c.mu.Lock()
			select {
			case <-c.stopping:
				c.mu.Unlock()
				fresh.Close()
				return
			default:
			}
			qw.w = fresh
			c.mu.Unlock()

			go fresh.Work()
			slog.Info("gearman: reconnected", "queue", qw.queue, "addr", c.addr)
			break
		}
	}
}

// jobHandler builds the gearman.JobHandler for one queue. Extracted from
// Start only so the per-queue setup loop above stays readable; it closes
// over nothing that differs between workers except queueName and handle.
func (c *GearmanConsumer) jobHandler(ctx context.Context, queueName string, handle Handler, out chan Message) gearman.JobFunc {
	return func(job gearman.Job) ([]byte, error) {
		if !c.beginHandler() {
			// Stop already ran: out may be closed, so this handler must
			// not touch it. Report the job as failed rather than silently
			// succeeding - it was genuinely not processed.
			return nil, errConsumerStopped
		}
		defer c.handlerWG.Done()

		payload := job.Data()
		metrics.QueueMessagesReceivedTotal.WithLabelValues(queueName).Inc()

		select {
		case out <- Message{Queue: queueName, Payload: payload}:
		default:
			// Raw-message observation is best-effort; never block job
			// processing on a slow/absent reader.
		}

		if err := observeHandler(ctx, queueName, handle, payload); err != nil {
			c.errors.Add(1)
			metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentQueue).Inc()
			slog.Warn("gearman: handler failed", "queue", queueName, "error", err)
			return nil, err
		}
		c.processed.Add(1)
		return nil, nil
	}
}

// closeWorkers shuts every per-queue connection down at the same time,
// which is the whole reason it is a function rather than a loop body.
//
// Close drains the jobs already running on that worker before dropping
// its connections, bounded by the fork's DrainTimeout (30s) - see Stop for
// why that draining exists. One queue at a time would therefore make the
// worst case len(workers) * DrainTimeout, turning a bounded shutdown into
// a six-minute hang the moment more than one queue has a stuck handler.
// Each Close is independent and touches only its own worker, so there is
// nothing to serialize. TestCloseWorkersDrainsInParallel measures it.
func closeWorkers(workers []*gearman.Worker) {
	var closing sync.WaitGroup
	for _, w := range workers {
		closing.Add(1)
		go func(w *gearman.Worker) {
			defer closing.Done()
			w.Close() // stops this queue's Work() and further job dispatch
		}(w)
	}
	closing.Wait()
}

// errConsumerStopped is returned to the Gearman job server for a job that
// arrives after Stop has begun tearing the consumer down.
var errConsumerStopped = errors.New("gearman: consumer is shutting down")

// beginHandler registers one in-flight job handler, reporting false if the
// consumer has already been stopped and the handler must not run. See the
// stoppedMu field comment for why the Add is lock-guarded.
func (c *GearmanConsumer) beginHandler() bool {
	c.stoppedMu.RLock()
	defer c.stoppedMu.RUnlock()

	if c.stopped {
		return false
	}
	c.handlerWG.Add(1)
	return true
}

// logStatsPeriodically emits one structured summary line of messages
// processed/errored every statsLogInterval, until ctx is cancelled or Stop
// closes statsDone - message counts rather than per-message logging, so
// observability never adds overhead to the hot job-handling path
// (CLAUDE.md rule 2).
func (c *GearmanConsumer) logStatsPeriodically(ctx context.Context) {
	ticker := time.NewTicker(statsLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slog.Info("gearman: consumer stats",
				"addr", c.addr, "processed", c.processed.Load(), "errors", c.errors.Load(),
				"reconnects", c.reconnects.Load())
		case <-c.statsDone:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop disconnects every per-queue connection and closes the channel
// returned by Start. It waits for any job handler already in flight to
// finish first, so the output channel is never closed while a send to it
// might still be in progress. Safe to call multiple times and safe to call
// without a prior Start.
//
// It also tells the per-queue reconnect supervisors to stop retrying, so a
// shutdown that lands during a job-server outage is not held up by one -
// closing c.stopping is what ends both their backoff wait and their loop.
// See superviseReconnects for why that close happens under c.mu.
//
// Three upstream defects made this unsafe; all are fixed in the patched
// fork this module points at (see go.mod's replace directive).
//
// Two were data races: Worker.Close closed worker.in while the
// per-connection agent goroutines were still sending on it, and
// worker.running was written under the worker mutex but read from exec
// without it. The fork waits on a WaitGroup covering those goroutines
// before closing the channel, and makes running an atomic.Bool.
// Shutdown is no longer best-effort, and the full suite runs under -race
// with nothing skipped.
//
// The third cost data rather than stability, and is why w.Close() below
// is safe to call before handlerWG.Wait(). Close used to clear running
// and drop the connections before waiting, while exec writes a job's
// WORK_COMPLETE only while running is true - so every handler still
// running at that moment finished its work, wrote its rows, and then
// silently skipped its acknowledgement. The job server re-queued exactly
// those jobs (bounded by -gearman-max-concurrent-jobs) and redelivered
// them after the restart. Measured here before the fix: a SIGTERM under
// load re-queued 64 jobs and lost 1.1% of all events, because the
// redelivery collided on a PRIMARY KEY and took the rest of its INSERT
// batch down with it. The fork now drains in-flight jobs before
// disconnecting, so nothing is handed out twice; cmd/losstest measures
// it (300,000 of 300,000, and processed + still-queued adds up to
// exactly what was published).
//
// Both halves are needed and neither replaces the other: this one stops
// redelivery from happening on an orderly shutdown, and the upserts in
// registry.go keep it harmless when it happens anyway - a crash, an
// OOM-kill or a lost acknowledgement on the network.
//
// If the replace directive is ever dropped, expect
// TestGearmanConsumerEndToEnd to fail under -race in about half of all
// runs, with the report attaching to whichever test happens to be
// running when it fires, and expect the redelivery to come back.
func (c *GearmanConsumer) Stop() error {
	c.stopOnce.Do(func() {
		close(c.statsDone)

		// Both under the mutex, and that is the entire synchronisation
		// against a reconnect landing at the same moment: a supervisor
		// hangs its new worker in under this same lock and checks
		// c.stopping while holding it, so it either gets into the snapshot
		// below or sees the shutdown and closes its own worker. Splitting
		// these two lines would leave a window in which a fresh connection
		// is created that nothing ever closes.
		c.mu.Lock()
		close(c.stopping)
		queues, out := c.queues, c.out
		workers := make([]*gearman.Worker, 0, len(queues))
		for _, qw := range queues {
			workers = append(workers, qw.w)
		}
		c.mu.Unlock()

		if len(workers) == 0 {
			return
		}

		closeWorkers(workers)

		for _, qw := range queues {
			metrics.QueueConnected.WithLabelValues(qw.queue).Set(0)
		}

		// Close the gate before waiting: after this write lock is
		// released, beginHandler can only ever return false, so the
		// WaitGroup counter can no longer rise and Wait is safe. Doing it
		// after Close (rather than before) keeps the window in which a
		// legitimately dispatched job gets rejected as small as possible.
		c.stoppedMu.Lock()
		c.stopped = true
		c.stoppedMu.Unlock()

		c.handlerWG.Wait()

		if out != nil {
			close(out)
		}

		slog.Info("gearman: consumer stopped",
			"addr", c.addr, "processed", c.processed.Load(), "errors", c.errors.Load(),
			"reconnects", c.reconnects.Load())
	})
	return nil
}

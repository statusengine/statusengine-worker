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
// It registers one worker function per queue name present in its Router;
// each job's payload is decoded and dispatched by the matching Handler.
type GearmanConsumer struct {
	addr   string
	router Router

	// maxConcurrentJobs caps how many job handlers may run at once. See
	// NewGearmanConsumer for why this must never be gearman.Unlimited.
	maxConcurrentJobs int

	mu     sync.Mutex
	worker *gearman.Worker
	out    chan Message

	stopOnce  sync.Once
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

	// processed/errors count jobs handled since Start, for the periodic
	// stats log. Incremented from per-connection job-handler goroutines,
	// hence atomic.
	processed atomic.Uint64
	errors    atomic.Uint64
}

// NewGearmanConsumer creates a consumer that will connect to the Gearman
// job server at addr (host:port) and handle every queue name in router,
// running at most maxConcurrentJobs handlers at a time.
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
// With a cap, the library's Work loop blocks handing out the next job
// once the cap is reached, stops reading its socket, and the backpressure
// reaches gearmand over TCP. The backlog then waits where it survives a
// worker restart and is visible via `gearadmin --status`.
//
// maxConcurrentJobs must be >= 1: gearman.Unlimited is 0, so passing zero
// would silently restore the unbounded behaviour this exists to prevent.
func NewGearmanConsumer(addr string, router Router, maxConcurrentJobs int) *GearmanConsumer {
	return &GearmanConsumer{
		addr:              addr,
		router:            router,
		maxConcurrentJobs: maxConcurrentJobs,
		statsDone:         make(chan struct{}),
	}
}

// Start connects to the Gearman job server, registers a worker function
// for every queue in the Router and begins processing jobs. It returns a
// channel carrying a copy of every raw message received, for observability
// - the actual decode/persist/broadcast work happens inside the Router's
// Handlers, invoked synchronously as each job arrives.
func (c *GearmanConsumer) Start(ctx context.Context) (<-chan Message, error) {
	// Not gearman.Unlimited - see NewGearmanConsumer. Note the library
	// buffers limit-1 tokens (worker.go's New) and sends one only after
	// spawning the job's goroutine, so New(n) permits exactly n
	// concurrent handlers and New(1) serializes them. That is correct as
	// written; it only looks like an off-by-one.
	w := gearman.New(c.maxConcurrentJobs)
	if err := w.AddServer(gearman.Network, c.addr); err != nil {
		return nil, fmt.Errorf("gearman: connect to %s: %w", c.addr, err)
	}
	w.ErrorHandler = func(err error) {
		slog.Warn("gearman: worker error", "error", err)
	}

	out := make(chan Message, outboundBufferSize)

	for queueName, handle := range c.router {
		queueName, handle := queueName, handle // capture per-iteration values for the closure
		err := w.AddFunc(queueName, func(job gearman.Job) ([]byte, error) {
			if !c.beginHandler() {
				// Stop already ran: out may be closed, so this handler
				// must not touch it. Report the job as failed rather than
				// silently succeeding - it was genuinely not processed.
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
		}, 0)
		if err != nil {
			w.Close()
			return nil, fmt.Errorf("gearman: register function %q: %w", queueName, err)
		}
	}

	if err := w.Ready(); err != nil {
		w.Close()
		return nil, fmt.Errorf("gearman: ready: %w", err)
	}

	c.mu.Lock()
	c.worker = w
	c.out = out
	c.mu.Unlock()

	go w.Work()
	go c.logStatsPeriodically(ctx)

	go func() {
		<-ctx.Done()
		c.Stop()
	}()

	slog.Info("gearman: consumer started", "addr", c.addr, "queues", len(c.router))

	return out, nil
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
				"addr", c.addr, "processed", c.processed.Load(), "errors", c.errors.Load())
		case <-c.statsDone:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop disconnects from the job server and closes the channel returned by
// Start. It waits for any job handler already in flight to finish first,
// so the output channel is never closed while a send to it might still be
// in progress. Safe to call multiple times and safe to call without a
// prior Start.
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

		c.mu.Lock()
		w, out := c.worker, c.out
		c.mu.Unlock()

		if w == nil {
			return
		}

		w.Close() // stops Work() and further job dispatch

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
			"addr", c.addr, "processed", c.processed.Load(), "errors", c.errors.Load())
	})
	return nil
}

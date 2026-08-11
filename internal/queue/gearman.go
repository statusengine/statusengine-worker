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
// KNOWN ISSUE, upstream only - this consumer's own handler bookkeeping is
// synchronized, see beginHandler. github.com/mikespook/gearman-go's
// Worker.Close() closes its internal job channel (worker.in, worker.go:231)
// without synchronizing against the per-connection agent goroutines that
// send to it (agent.go:101), so shutting down while a packet is in flight
// is a genuine data race: reproducible with `go test -race` in about half
// of all runs (5 of 10, measured August 2026), independent of anything
// this package does. There is no exported way to wait for those
// goroutines first.
//
// The blast radius is smaller than the race report suggests: agent.work
// recovers its own panics (agent.go:47) and routes them to the worker's
// ErrorHandler, so a losing race shows up as a logged
// "send on closed channel" and one dead agent goroutine during shutdown -
// not a crashed worker. That is the observed outcome, not a guaranteed
// one: a data race is undefined behaviour, and the recovered panic is
// simply how the runtime happens to react to closechan racing chansend.
// What it does cost for certain is the race detector's value as a CI
// gate, since the report lands on whichever test happens to be running
// when it fires - hence the -skip in CLAUDE.md's test command.
//
// DECIDED, August 2026: not forked. Fixing it properly means a patched
// fork behind a go.mod replace (see
// github.com/mikespook/gearman-go/issues/88, which fixed a related but
// distinct race in 2019), and a permanent maintenance obligation for a
// ~10-line patch is out of proportion to a shutdown-only race the
// library already contains. Treat Gearman shutdown as best-effort.
//
// When revisiting, check in this order:
//   - Has mikespook/gearman-go moved at all? It has been untouched since
//     v0.0.0-20220520031403 (May 2022), so any new commit is news.
//   - Is there a maintained fork worth pointing a replace directive at?
//   - Does the -skip above still reproduce? If upstream fixed it, the
//     skip should go.
//
// gearmand 2.0.0 is explicitly NOT a reason to act: it changes no wire
// protocol, so this client stays compatible and a fork would not have to
// grow to cover it (see CLAUDE.md rule 1). No need to research that again.
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

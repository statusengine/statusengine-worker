package queue

import (
	"context"
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

	mu     sync.Mutex
	worker *gearman.Worker
	out    chan Message

	stopOnce  sync.Once
	handlerWG sync.WaitGroup
	statsDone chan struct{}

	// processed/errors count jobs handled since Start, for the periodic
	// stats log. Incremented from per-connection job-handler goroutines,
	// hence atomic.
	processed atomic.Uint64
	errors    atomic.Uint64
}

// NewGearmanConsumer creates a consumer that will connect to the Gearman
// job server at addr (host:port) and handle every queue name in router.
func NewGearmanConsumer(addr string, router Router) *GearmanConsumer {
	return &GearmanConsumer{addr: addr, router: router, statsDone: make(chan struct{})}
}

// Start connects to the Gearman job server, registers a worker function
// for every queue in the Router and begins processing jobs. It returns a
// channel carrying a copy of every raw message received, for observability
// - the actual decode/persist/broadcast work happens inside the Router's
// Handlers, invoked synchronously as each job arrives.
func (c *GearmanConsumer) Start(ctx context.Context) (<-chan Message, error) {
	w := gearman.New(gearman.Unlimited)
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
			c.handlerWG.Add(1)
			defer c.handlerWG.Done()

			payload := job.Data()
			metrics.QueueMessagesReceivedTotal.WithLabelValues(queueName).Inc()

			select {
			case out <- Message{Queue: queueName, Payload: payload}:
			default:
				// Raw-message observation is best-effort; never block job
				// processing on a slow/absent reader.
			}

			if err := handle(ctx, payload); err != nil {
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
// KNOWN ISSUE: github.com/mikespook/gearman-go's Worker.Close() has an
// unsynchronized close of its internal job channel (worker.in) against the
// per-connection agent goroutines that send to it - confirmed with
// `go test -race`, independent of anything this package does; there is no
// public API to wait for those goroutines first, and a panic inside them
// cannot be recovered from here (recover only works within the panicking
// goroutine). In practice the race window is narrow and only reachable
// during shutdown, but a "send on closed channel" panic there is possible.
// Until upstream fixes this (see github.com/mikespook/gearman-go/issues/88,
// which fixed a related but distinct race in 2019) or this consumer is
// pointed at a patched fork, treat Gearman shutdown as best-effort.
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
		c.handlerWG.Wait()

		if out != nil {
			close(out)
		}

		slog.Info("gearman: consumer stopped",
			"addr", c.addr, "processed", c.processed.Load(), "errors", c.errors.Load())
	})
	return nil
}

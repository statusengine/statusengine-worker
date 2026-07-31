package queue

import (
	"context"
	"fmt"
	"log"
	"sync"

	gearman "github.com/mikespook/gearman-go/worker"
)

// outboundBufferSize is the capacity of the raw-Message channel Start
// returns. Sends onto it are best-effort (see Start): a full buffer never
// blocks job processing.
const outboundBufferSize = 256

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
}

// NewGearmanConsumer creates a consumer that will connect to the Gearman
// job server at addr (host:port) and handle every queue name in router.
func NewGearmanConsumer(addr string, router Router) *GearmanConsumer {
	return &GearmanConsumer{addr: addr, router: router}
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
		log.Printf("gearman: worker error: %v", err)
	}

	out := make(chan Message, outboundBufferSize)

	for queueName, handle := range c.router {
		queueName, handle := queueName, handle // capture per-iteration values for the closure
		err := w.AddFunc(queueName, func(job gearman.Job) ([]byte, error) {
			c.handlerWG.Add(1)
			defer c.handlerWG.Done()

			payload := job.Data()

			select {
			case out <- Message{Queue: queueName, Payload: payload}:
			default:
				// Raw-message observation is best-effort; never block job
				// processing on a slow/absent reader.
			}

			if err := handle(ctx, payload); err != nil {
				log.Printf("gearman: handler for %q failed: %v", queueName, err)
				return nil, err
			}
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

	go func() {
		<-ctx.Done()
		c.Stop()
	}()

	return out, nil
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
	})
	return nil
}

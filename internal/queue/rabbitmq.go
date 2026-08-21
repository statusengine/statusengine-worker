package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"statusengine-worker/internal/metrics"
)

// reconnectDelay is how long the consumer waits between reconnect attempts
// after an unexpected disconnect. Unlike the Gearman client library (see
// gearman.go's KNOWN ISSUE comment for the one thing it doesn't handle),
// amqp091-go never reconnects on its own - a dropped TCP connection just
// closes every delivery channel and stops there - so this consumer has to
// redial itself to satisfy CLAUDE.md rule 6 ("reconnect automatically to
// MySQL/Queues on connection drops").
const reconnectDelay = 2 * time.Second

// requeueDelay is how long a delivery loop pauses before requeueing a
// message its Handler failed on for a retryable reason. Without it, a
// failure that persists for any length of time (MySQL restarting, say)
// makes the broker redeliver the same message as fast as the loop can nack
// it, burning a CPU core and filling the log for the entire outage.
const requeueDelay = 250 * time.Millisecond

// stopDrainTimeout bounds how long Stop waits for Handlers still in flight
// before giving up on them. Generous enough that a normal in-flight batch
// always finishes, short enough that a wedged Handler can't turn a
// graceful shutdown into a hang.
const stopDrainTimeout = 5 * time.Second

// RabbitMQConsumer implements queue.Consumer against a RabbitMQ broker. It
// opens one channel and consumer per queue name present in its Router;
// each delivery's body is decoded and dispatched by the matching Handler,
// then acked (or nacked-and-requeued on handler failure).
//
// Every queue in the Router is declared (durable, not auto-deleted, not
// exclusive) before consuming, so this consumer works whether it or a
// publisher connects first - RabbitMQ queue declaration is idempotent as
// long as every declaration agrees on the same arguments.
//
// Durable is load-bearing rather than a preference. A queue that is
// neither durable nor exclusive is RabbitMQ's deprecated
// transient_nonexcl_queues feature: 3.13 logs a warning once per broker
// start, and 4.x refuses the declare outright with a connection
// exception, so the worker declares nothing and consumes nothing. Durable
// has worked since AMQP 0-9-1, which makes it the only setting that works
// on every broker version this worker might meet.
//
// It does not make the events survive a broker restart, and is not meant
// to. Queue durability and message persistence are separate properties:
// durable stores the queue *definition* on disk, while a message is only
// written durably if its publisher marked it persistent. The Statusengine
// NEB broker publishes with no delivery_mode at all
// (amqp_basic_publish(..., properties=nullptr, ...) in
// src/MessageHandler/RabbitmqClient.cpp), i.e. transient - so the queues
// still buffer in RAM while no worker is connected, and a broker restart
// still empties them. That is the intended behaviour and the change to
// durable preserves it exactly.
//
// The NEB broker declares these same queues, with durable taken from its
// DurableQueues setting. Both sides have to agree or whichever declares
// second gets a 406 PRECONDITION_FAILED, which is why
// cmd/rabbitmq_publisher and the tests declare with identical arguments.
//
// If the underlying connection drops unexpectedly, a background supervisor
// goroutine redials, redeclares every queue and resumes consuming - see
// superviseReconnects.
type RabbitMQConsumer struct {
	url    string
	router Router

	// prefetch caps how many unacknowledged deliveries the broker may
	// push per queue. See NewRabbitMQConsumer.
	prefetch int

	mu          sync.Mutex
	conn        *amqp.Connection
	channels    []*amqp.Channel
	closeNotify chan *amqp.Error

	stopOnce   sync.Once
	stopping   chan struct{}
	consumerWG sync.WaitGroup
	statsDone  chan struct{}

	// processed/errors/dropped/reconnects count activity since Start, for
	// the periodic stats log and the final stop-summary line. Incremented
	// from per-queue delivery-loop goroutines and the supervisor, hence
	// atomic. dropped counts messages discarded as permanently
	// unprocessable (see consumeLoop) - a non-zero value means data was
	// thrown away and belongs in an alert.
	processed  atomic.Uint64
	errors     atomic.Uint64
	dropped    atomic.Uint64
	reconnects atomic.Uint64
}

// NewRabbitMQConsumer creates a consumer that will connect to the RabbitMQ
// broker at rawURL (an amqp:// URI, e.g. amqp://user:pass@host:5672/) and
// handle every queue name in router, letting the broker push at most
// prefetch unacknowledged deliveries per queue.
//
// Without that limit AMQP delivers as fast as it can, and since each
// queue's consumeLoop handles deliveries one at a time, everything the
// broker has ends up buffered in this process - the same failure the
// Gearman consumer's concurrency cap prevents, arriving from the other
// direction. With it, the surplus stays unacknowledged at the broker,
// where it survives a worker restart and is visible in the management UI.
//
// The limit applies per queue, so the worst-case in-memory backlog is
// prefetch multiplied by the number of queues in the Router (currently
// 12). prefetch must be >= 1; 0 means "unlimited" in AMQP.
func NewRabbitMQConsumer(rawURL string, router Router, prefetch int) *RabbitMQConsumer {
	return &RabbitMQConsumer{
		url:       rawURL,
		router:    router,
		prefetch:  prefetch,
		statsDone: make(chan struct{}),
		stopping:  make(chan struct{}),
	}
}

// Start dials the broker, declares and starts consuming every queue in the
// Router, and begins processing deliveries. The first connection attempt
// happens synchronously, so a startup problem (bad URL, unreachable broker,
// bad credentials) is reported the same way every other Consumer reports
// one; every attempt after that is handled by the background reconnect
// supervisor. It returns a channel carrying a copy of every raw message
// received, for observability - the actual decode/persist/broadcast work
// happens inside the Router's Handlers.
func (c *RabbitMQConsumer) Start(ctx context.Context) (<-chan Message, error) {
	out := make(chan Message, outboundBufferSize)

	if err := c.connect(ctx, out); err != nil {
		close(out)
		return nil, err
	}

	go c.superviseReconnects(ctx, out)
	go c.logStatsPeriodically(ctx)
	go func() {
		<-ctx.Done()
		c.Stop()
	}()

	slog.Info("rabbitmq: consumer started", "url", redactURL(c.url), "queues", len(c.router))

	return out, nil
}

// connect dials the broker, declares every queue in the Router and starts
// one delivery-handling goroutine per queue feeding into out, then records
// the new connection/channels so Stop and superviseReconnects can find
// them. It is called once synchronously from Start and again, in a loop,
// by superviseReconnects after every unexpected disconnect - on any
// failure it cleans up whatever it opened itself and leaves the previous
// generation's connection (if any) untouched, so a failed reconnect
// attempt never disturbs Stop's bookkeeping.
func (c *RabbitMQConsumer) connect(ctx context.Context, out chan<- Message) error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("rabbitmq: dial %s: %w", redactURL(c.url), err)
	}
	closeNotify := conn.NotifyClose(make(chan *amqp.Error, 1))

	channels := make([]*amqp.Channel, 0, len(c.router))
	for queueName, handle := range c.router {
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return fmt.Errorf("rabbitmq: open channel for %q: %w", queueName, err)
		}

		// Declare rather than assume the queue already exists: whichever
		// side connects first - this consumer, the NEB broker or a
		// publisher - creates it. Durable for the reasons in the type
		// comment above; every other flag stays false.
		if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			conn.Close()
			// A 406 PRECONDITION_FAILED here is by far the likeliest
			// failure and says nothing useful on its own, so name the
			// cause: someone else declared this queue with different
			// arguments, and the argument that realistically differs is
			// durability.
			return fmt.Errorf("rabbitmq: declare queue %q (a 406 PRECONDITION_FAILED means another client "+
				"already declared it with different arguments - check DurableQueues in the NEB broker's "+
				"statusengine.toml, both sides must agree): %w", queueName, err)
		}

		// Bound the broker's push rate before consuming, not after: this
		// channel serves exactly one consumer, so a per-consumer limit
		// (global=false) is the right scope.
		if err := ch.Qos(c.prefetch, 0, false); err != nil {
			conn.Close()
			return fmt.Errorf("rabbitmq: set prefetch for %q: %w", queueName, err)
		}

		deliveries, err := ch.ConsumeWithContext(ctx, queueName, "", false, false, false, false, nil)
		if err != nil {
			conn.Close()
			return fmt.Errorf("rabbitmq: consume %q: %w", queueName, err)
		}

		channels = append(channels, ch)

		queueName, handle := queueName, handle // capture per-iteration values for the goroutine
		c.consumerWG.Add(1)
		go c.consumeLoop(ctx, queueName, handle, deliveries, out)
	}

	c.mu.Lock()
	c.conn = conn
	c.channels = channels
	c.closeNotify = closeNotify
	c.mu.Unlock()

	return nil
}

// consumeLoop drains deliveries for one queue until the underlying channel
// closes (connection lost, or Stop closed it deliberately), dispatching
// each delivery's body to handle and acking/nacking based on the result.
func (c *RabbitMQConsumer) consumeLoop(ctx context.Context, queueName string, handle Handler, deliveries <-chan amqp.Delivery, out chan<- Message) {
	defer c.consumerWG.Done()

	for d := range deliveries {
		metrics.QueueMessagesReceivedTotal.WithLabelValues(queueName).Inc()

		select {
		case out <- Message{Queue: queueName, Payload: d.Body}:
		default:
			// Raw-message observation is best-effort; never block delivery
			// processing on a slow/absent reader.
		}

		if err := observeHandler(ctx, queueName, handle, d.Body); err != nil {
			c.errors.Add(1)
			metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentQueue).Inc()

			// A permanent failure (an undecodable payload) produces the
			// exact same error on every redelivery, so requeueing it just
			// spins this loop at full speed and blocks every healthy
			// message behind it. Drop it instead - to the queue's
			// dead-letter exchange if one is configured, otherwise for
			// good - and log it loudly, since discarding data is not
			// something that should pass unnoticed.
			if errors.Is(err, ErrPermanent) {
				c.dropped.Add(1)
				slog.Error("rabbitmq: dropping unprocessable message",
					"queue", queueName, "bytes", len(d.Body), "error", err)
				if nackErr := d.Nack(false, false); nackErr != nil {
					slog.Warn("rabbitmq: nack failed", "queue", queueName, "error", nackErr)
				}
				continue
			}

			// Anything else (MySQL down, buffer full, context cancelled)
			// is worth retrying, so requeue - but pause first, otherwise a
			// broker-side outage of any length turns this loop into a busy
			// wait against the same message. The pause is interruptible so
			// it never delays a graceful shutdown.
			slog.Warn("rabbitmq: handler failed, requeueing", "queue", queueName, "error", err)
			if !c.pauseBeforeRequeue(ctx) {
				// Shutting down: leave the message unacked so the broker
				// redelivers it to whoever consumes next.
				return
			}
			if nackErr := d.Nack(false, true); nackErr != nil {
				slog.Warn("rabbitmq: nack failed", "queue", queueName, "error", nackErr)
			}
			continue
		}
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Warn("rabbitmq: ack failed", "queue", queueName, "error", ackErr)
		}
		c.processed.Add(1)
	}
}

// pauseBeforeRequeue waits requeueDelay before a retryable message is
// nacked back onto the queue, returning false if the consumer is shutting
// down instead - in which case the caller must stop rather than requeue, so
// shutdown is never held up by a retry backoff.
func (c *RabbitMQConsumer) pauseBeforeRequeue(ctx context.Context) bool {
	timer := time.NewTimer(requeueDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-c.stopping:
		return false
	}
}

// superviseReconnects watches the current generation's connection for an
// unexpected close and redials with reconnectDelay between attempts until
// one succeeds, ctx is cancelled or Stop is called - closing out only once
// none of that will ever happen again. A graceful Stop (the normal
// cmd/app shutdown path, which calls Stop before cancelling its pipeline
// context - see cmd/app/main.go step 6a) also closes the connection and so
// also fires closeNotify; c.stopping distinguishes that from a real drop so
// a clean shutdown never triggers a pointless reconnect attempt.
func (c *RabbitMQConsumer) superviseReconnects(ctx context.Context, out chan<- Message) {
	// out belongs to the consumeLoop goroutines that send on it, so it must
	// not be closed while any of them is still running: a send racing this
	// close is an unrecoverable "send on closed channel" panic (it happens
	// in another goroutine, where a recover here can't reach it). Every
	// return path below reaches this only after ctx was cancelled or Stop
	// was called, both of which close the connection and so close every
	// generation's deliveries channel, which is what makes the loops exit -
	// so this Wait always terminates. This is the sole owner of the close;
	// the WaitGroup is only ever Added to from connect, which runs either
	// synchronously before this goroutine starts or from this goroutine
	// itself, so no Add can ever race this Wait.
	defer func() {
		c.consumerWG.Wait()
		close(out)
	}()

	for {
		c.mu.Lock()
		closeNotify := c.closeNotify
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-c.stopping:
			return
		case amqpErr := <-closeNotify:
			select {
			case <-ctx.Done():
				return
			case <-c.stopping:
				return
			default:
			}
			c.reconnects.Add(1)
			slog.Warn("rabbitmq: connection lost, reconnecting", "error", amqpErr, "retry_interval", reconnectDelay)
		}

		// Let this generation's consumeLoop goroutines finish exiting
		// (their deliveries channels close as a direct consequence of the
		// connection going away) before connect adds new ones to the same
		// WaitGroup.
		c.consumerWG.Wait()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopping:
				return
			case <-time.After(reconnectDelay):
			}

			if err := c.connect(ctx, out); err != nil {
				slog.Warn("rabbitmq: reconnect attempt failed", "error", err)
				continue
			}
			slog.Info("rabbitmq: reconnected", "queues", len(c.router))
			break
		}
	}
}

// logStatsPeriodically emits one structured summary line of messages
// processed/errored every statsLogInterval, until ctx is cancelled or Stop
// closes statsDone - message counts rather than per-message logging, so
// observability never adds overhead to the hot delivery-handling path
// (CLAUDE.md rule 2).
func (c *RabbitMQConsumer) logStatsPeriodically(ctx context.Context) {
	ticker := time.NewTicker(statsLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slog.Info("rabbitmq: consumer stats",
				"processed", c.processed.Load(), "errors", c.errors.Load(),
				"dropped", c.dropped.Load(), "reconnects", c.reconnects.Load())
		case <-c.statsDone:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop closes the current connection (and, before that, every channel
// opened on it), which causes every consumeLoop's deliveries channel to
// close and its goroutine to exit, and signals superviseReconnects to stop
// retrying rather than treat this as a drop to recover from. It then waits
// for any Handler still in flight to return, so that once Stop has
// returned, nothing is being enqueued into the pipeline any more. Safe to
// call multiple times and safe to call without a prior Start.
//
// That wait is what makes cmd/app/main.go's shutdown order sound: step 6a
// stops the consumer "so no new data comes in", then step 6b flushes every
// BulkInserter. Returning while Handlers were still running meant rows
// enqueued after their inserter had already flushed, which are then lost -
// on every shutdown under load. The Gearman consumer has always waited on
// its handlers here (see handlerWG); this is the same guarantee.
//
// The wait is bounded: a shutdown must never hang outright, so after
// stopDrainTimeout it gives up and says so rather than blocking forever.
func (c *RabbitMQConsumer) Stop() error {
	var err error
	c.stopOnce.Do(func() {
		close(c.stopping)
		close(c.statsDone)

		c.mu.Lock()
		conn, channels := c.conn, c.channels
		c.mu.Unlock()

		for _, ch := range channels {
			_ = ch.Close()
		}
		if conn != nil {
			// A mid-flight shutdown routinely races the delivery loops:
			// one of them acks on a channel whose connection is already on
			// its way out, the broker answers with a connection-level
			// error and closes it, and this Close then reports that the
			// connection is not open. The connection being gone is the
			// goal, so that is not a failure - only a Close that leaves it
			// open is. Reporting the former had cmd/app log "error
			// stopping consumer" on every shutdown that happened to catch
			// a backlog.
			if closeErr := conn.Close(); closeErr != nil {
				if conn.IsClosed() {
					slog.Debug("rabbitmq: connection was already closed on shutdown", "error", closeErr)
				} else {
					err = closeErr
				}
			}
		}

		// Closing the connection above closed every consumeLoop's
		// deliveries channel, so the loops are already on their way out.
		// The only things one can still be blocked on are its Handler's
		// Enqueue (which selects on ctx, and whose BulkInserter is still
		// draining - cmd/app/main.go doesn't cancel the pipeline until
		// step 6c) and pauseBeforeRequeue (which selects on c.stopping,
		// closed just above). Sends to out are non-blocking. So this
		// terminates on its own; the timeout is a backstop, not the plan.
		if !waitTimeout(&c.consumerWG, stopDrainTimeout) {
			slog.Warn("rabbitmq: gave up waiting for in-flight handlers",
				"timeout", stopDrainTimeout,
				"note", "buffered rows enqueued after this point may not be flushed")
		}

		slog.Info("rabbitmq: consumer stopped",
			"processed", c.processed.Load(), "errors", c.errors.Load(),
			"dropped", c.dropped.Load(), "reconnects", c.reconnects.Load())
	})
	return err
}

// waitTimeout waits for wg, reporting false if d elapsed first. The
// spawned goroutine outlives a timed-out call until the WaitGroup does
// drain, which is acceptable precisely because the only caller is a
// shutdown path that is about to end the process anyway.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// redactURL masks an amqp:// URL's password before it ever reaches a log
// line or error message - c.url carries real broker credentials (see
// .claude/specs/ressources.txt), and CLAUDE.md's connection-drop handling
// means dial errors are exactly the kind of thing that gets logged
// repeatedly during an outage. Falls back to a fixed placeholder if url
// doesn't even parse, rather than risking a malformed credential leaking
// through unredacted.
func redactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "(invalid rabbitmq url)"
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "****")
	}
	return u.String()
}

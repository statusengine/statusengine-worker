package queue

import (
	"context"
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

// RabbitMQConsumer implements queue.Consumer against a RabbitMQ broker. It
// opens one channel and consumer per queue name present in its Router;
// each delivery's body is decoded and dispatched by the matching Handler,
// then acked (or nacked-and-requeued on handler failure).
//
// Every queue in the Router is declared (not durable, not auto-deleted, not
// exclusive - matching the real broker's own statusngin_* queues, see
// .claude/specs/ressources.txt) before consuming, so this consumer works
// whether it or a publisher (e.g. cmd/rabbitmq_publisher) connects first -
// RabbitMQ queue declaration is idempotent as long as every declaration
// agrees on the same arguments, which is why cmd/rabbitmq_publisher
// declares with the exact same arguments.
//
// If the underlying connection drops unexpectedly, a background supervisor
// goroutine redials, redeclares every queue and resumes consuming - see
// superviseReconnects.
type RabbitMQConsumer struct {
	url    string
	router Router

	mu          sync.Mutex
	conn        *amqp.Connection
	channels    []*amqp.Channel
	closeNotify chan *amqp.Error

	stopOnce   sync.Once
	stopping   chan struct{}
	consumerWG sync.WaitGroup
	statsDone  chan struct{}

	// processed/errors/reconnects count activity since Start, for the
	// periodic stats log and the final stop-summary line. Incremented from
	// per-queue delivery-loop goroutines and the supervisor, hence atomic.
	processed  atomic.Uint64
	errors     atomic.Uint64
	reconnects atomic.Uint64
}

// NewRabbitMQConsumer creates a consumer that will connect to the RabbitMQ
// broker at rawURL (an amqp:// URI, e.g. amqp://user:pass@host:5672/) and
// handle every queue name in router.
func NewRabbitMQConsumer(rawURL string, router Router) *RabbitMQConsumer {
	return &RabbitMQConsumer{
		url:       rawURL,
		router:    router,
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

		// Declare rather than assume the queue already exists (unlike the
		// original implementation): whichever side connects first - this
		// consumer or a publisher - creates it. Not durable, matching how
		// the real broker's statusngin_* queues are already declared (see
		// .claude/specs/ressources.txt); this must stay identical to
		// cmd/rabbitmq_publisher's declaration, or RabbitMQ rejects the
		// mismatched redeclare with a 406 PRECONDITION_FAILED channel error.
		if _, err := ch.QueueDeclare(queueName, false, false, false, false, nil); err != nil {
			conn.Close()
			return fmt.Errorf("rabbitmq: declare queue %q: %w", queueName, err)
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

		if err := handle(ctx, d.Body); err != nil {
			c.errors.Add(1)
			metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentQueue).Inc()
			slog.Warn("rabbitmq: handler failed", "queue", queueName, "error", err)
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

// superviseReconnects watches the current generation's connection for an
// unexpected close and redials with reconnectDelay between attempts until
// one succeeds, ctx is cancelled or Stop is called - closing out only once
// none of that will ever happen again. A graceful Stop (the normal
// cmd/app shutdown path, which calls Stop before cancelling its pipeline
// context - see cmd/app/main.go step 6a) also closes the connection and so
// also fires closeNotify; c.stopping distinguishes that from a real drop so
// a clean shutdown never triggers a pointless reconnect attempt.
func (c *RabbitMQConsumer) superviseReconnects(ctx context.Context, out chan<- Message) {
	defer close(out)

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
				"processed", c.processed.Load(), "errors", c.errors.Load(), "reconnects", c.reconnects.Load())
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
// retrying rather than treat this as a drop to recover from. Safe to call
// multiple times and safe to call without a prior Start.
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
			err = conn.Close()
		}

		slog.Info("rabbitmq: consumer stopped",
			"processed", c.processed.Load(), "errors", c.errors.Load(), "reconnects", c.reconnects.Load())
	})
	return err
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

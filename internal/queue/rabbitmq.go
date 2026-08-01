package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConsumer implements queue.Consumer against a RabbitMQ broker. It
// opens one channel and consumer per queue name present in its Router;
// each delivery's body is decoded and dispatched by the matching Handler,
// then acked (or nacked-and-requeued on handler failure).
//
// Queues are expected to already exist (declared by whatever publishes
// events onto them, e.g. the Icinga/Nagios event broker module) - this
// consumer only consumes, it never declares topology.
type RabbitMQConsumer struct {
	url    string
	router Router

	mu       sync.Mutex
	conn     *amqp.Connection
	channels []*amqp.Channel

	stopOnce   sync.Once
	consumerWG sync.WaitGroup
	statsDone  chan struct{}

	// processed/errors count deliveries handled since Start, for the
	// periodic stats log. Incremented from per-queue delivery-loop
	// goroutines, hence atomic.
	processed atomic.Uint64
	errors    atomic.Uint64
}

// NewRabbitMQConsumer creates a consumer that will connect to the RabbitMQ
// broker at url (an amqp:// URI) and handle every queue name in router.
func NewRabbitMQConsumer(url string, router Router) *RabbitMQConsumer {
	return &RabbitMQConsumer{url: url, router: router, statsDone: make(chan struct{})}
}

// Start dials the broker, opens one Channel and consumer per queue in the
// Router, and begins processing deliveries. It returns a channel carrying
// a copy of every raw message received, for observability - the actual
// decode/persist/broadcast work happens inside the Router's Handlers.
func (c *RabbitMQConsumer) Start(ctx context.Context) (<-chan Message, error) {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial %s: %w", c.url, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	out := make(chan Message, outboundBufferSize)

	for queueName, handle := range c.router {
		ch, err := conn.Channel()
		if err != nil {
			c.Stop()
			return nil, fmt.Errorf("rabbitmq: open channel for %q: %w", queueName, err)
		}

		deliveries, err := ch.ConsumeWithContext(ctx, queueName, "", false, false, false, false, nil)
		if err != nil {
			c.Stop()
			return nil, fmt.Errorf("rabbitmq: consume %q: %w", queueName, err)
		}

		c.mu.Lock()
		c.channels = append(c.channels, ch)
		c.mu.Unlock()

		queueName, handle := queueName, handle // capture per-iteration values for the goroutine
		c.consumerWG.Add(1)
		go func() {
			defer c.consumerWG.Done()

			// ConsumeWithContext closes deliveries once ctx is cancelled or
			// the channel/connection goes away, so this loop's exit is what
			// gates closing out below - never before every sender is done.
			for d := range deliveries {
				select {
				case out <- Message{Queue: queueName, Payload: d.Body}:
				default:
					// Raw-message observation is best-effort; never block
					// delivery processing on a slow/absent reader.
				}

				if err := handle(ctx, d.Body); err != nil {
					c.errors.Add(1)
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
		}()
	}

	go func() {
		c.consumerWG.Wait()
		close(out)
	}()

	go func() {
		<-ctx.Done()
		c.Stop()
	}()

	go c.logStatsPeriodically(ctx)

	slog.Info("rabbitmq: consumer started", "queues", len(c.router))

	return out, nil
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
			slog.Info("rabbitmq: consumer stats", "processed", c.processed.Load(), "errors", c.errors.Load())
		case <-c.statsDone:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop closes every channel and the connection, which in turn causes
// ConsumeWithContext's deliveries channels to close and their processing
// goroutines to exit. Safe to call multiple times and safe to call without
// a prior Start.
func (c *RabbitMQConsumer) Stop() error {
	var err error
	c.stopOnce.Do(func() {
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

		slog.Info("rabbitmq: consumer stopped", "processed", c.processed.Load(), "errors", c.errors.Load())
	})
	return err
}

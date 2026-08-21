package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gearman "github.com/mikespook/gearman-go/client"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher puts a validated command envelope onto the queue the NEB broker
// module consumes. There is one implementation per backend, chosen to match
// whichever backend the worker consumes with: the module reads the command
// queue on every handler it has configured, so publishing to the same
// broker the events come from is what reaches it.
//
// Publish reports only whether the broker accepted the message. Neither
// Gearman's SUBMIT_JOB_BG nor an AMQP basic.publish says anything about
// what Naemon later did with it - see the handler's 202 for why that
// distinction is kept visible all the way out to the caller.
type Publisher interface {
	Publish(ctx context.Context, payload []byte) error
	Close() error
}

// publishTimeout bounds one publish attempt. This sits behind an HTTP
// request, so the useful behaviour on an unreachable broker is to fail
// quickly and let the caller retry - the opposite of the MySQL path, which
// blocks precisely because there is no caller left to tell (CLAUDE.md rule
// 3).
const publishTimeout = 5 * time.Second

// GearmanPublisher submits background jobs to a Gearman job server.
//
// The connection is established lazily and re-established on demand rather
// than held open and supervised. A command API is not a hot path: it sees a
// handful of requests a minute at most, so paying a dial on the first
// request after an idle period costs nothing, while a supervised connection
// would be a goroutine and a reconnect loop to get right for no gain.
type GearmanPublisher struct {
	addr string
	// queue is always the package's Queue constant in the running worker -
	// it has to be, or the NEB module never sees the message. It is a field
	// rather than a use of the constant only so tests can publish into a
	// scratch queue and consume it back, instead of racing a running NEB
	// module for the real one.
	queue string

	mu     sync.Mutex
	client *gearman.Client
}

// NewGearmanPublisher returns a publisher for the job server at addr
// (host:port). It does not connect; the first Publish does.
func NewGearmanPublisher(addr string) *GearmanPublisher {
	return &GearmanPublisher{addr: addr, queue: Queue}
}

func (p *GearmanPublisher) Publish(ctx context.Context, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// One transparent retry, because the common failure here is a
	// connection the job server closed while nothing was being sent -
	// idle-timed-out, or restarted. That is indistinguishable from a real
	// outage until the redial has been tried, and the caller should not
	// have to see a 503 for it. A second failure is reported: retrying past
	// that would just hold the HTTP request open on a broker that is
	// genuinely down.
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		client, err := p.connectLocked()
		if err != nil {
			lastErr = err
			continue
		}
		if _, err := client.DoBg(p.queue, payload, gearman.JobNormal); err != nil {
			lastErr = err
			p.closeLocked()
			continue
		}
		return nil
	}
	return fmt.Errorf("gearman: submit to %s: %w", p.queue, lastErr)
}

// connectLocked returns the live client, dialing if there is not one. The
// caller must hold p.mu.
func (p *GearmanPublisher) connectLocked() (*gearman.Client, error) {
	if p.client != nil {
		return p.client, nil
	}
	client, err := gearman.New(gearman.Network, p.addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.addr, err)
	}
	// The library's read loop reports asynchronous failures here. Without a
	// handler it panics on some of them, which would take the whole worker
	// down over an unreachable job server.
	client.ErrorHandler = func(e error) {
		slog.Debug("command: gearman client error", "error", e)
	}
	client.ResponseTimeout = publishTimeout
	p.client = client
	return client, nil
}

func (p *GearmanPublisher) closeLocked() {
	if p.client != nil {
		_ = p.client.Close()
		p.client = nil
	}
}

func (p *GearmanPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
	return nil
}

// RabbitMQPublisher publishes to the command queue over AMQP.
//
// It publishes to the default exchange with the queue name as the routing
// key, exactly as cmd/rabbitmq_publisher does. The NEB module binds its
// queues to a "statusengine" direct exchange, but going through the default
// exchange reaches the same queue without this side having to agree with
// the module on an exchange name and its durability as well - one fewer
// declaration that both sides must match, and that mismatch is a 406 that
// takes the whole connection down (see internal/queue/rabbitmq.go).
type RabbitMQPublisher struct {
	url string
	// queue is the package's Queue constant in the running worker; see the
	// note on GearmanPublisher.queue for why it is a field.
	queue string

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewRabbitMQPublisher returns a publisher for the broker at rawURL. It
// does not connect; the first Publish does.
func NewRabbitMQPublisher(rawURL string) *RabbitMQPublisher {
	return &RabbitMQPublisher{url: rawURL, queue: Queue}
}

func (p *RabbitMQPublisher) Publish(ctx context.Context, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		ch, err := p.connectLocked()
		if err != nil {
			lastErr = err
			continue
		}
		// Deliberately no DeliveryMode: the message stays transient, like
		// everything else on these queues. Durability here is the queue's
		// property, not the message's - see internal/queue/rabbitmq.go.
		err = ch.PublishWithContext(ctx, "", p.queue, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
		})
		if err != nil {
			lastErr = err
			p.closeLocked()
			continue
		}
		return nil
	}
	return fmt.Errorf("rabbitmq: publish to %s: %w", p.queue, lastErr)
}

// connectLocked returns a usable channel, dialing if there is not one. The
// caller must hold p.mu.
func (p *RabbitMQPublisher) connectLocked() (*amqp.Channel, error) {
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}
	p.closeLocked()

	conn, err := amqp.Dial(p.url)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	// Declared with the same arguments as everywhere else, so this works
	// whether it, the consumer or the NEB module gets here first.
	if _, err := ch.QueueDeclare(p.queue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare queue %q (a 406 PRECONDITION_FAILED means another client declared "+
			"it with different arguments - check DurableQueues in the NEB broker's statusengine.toml): %w", p.queue, err)
	}
	p.conn, p.ch = conn, ch
	return ch, nil
}

func (p *RabbitMQPublisher) closeLocked() {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

func (p *RabbitMQPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
	return nil
}

// NewPublisher returns the publisher matching backend ("gearman" or
// "rabbitmq"), which is the same value cmd/app resolves for -consumer: the
// command queue lives at the same broker the events come from.
func NewPublisher(backend, gearmanAddr, rabbitMQURL string) (Publisher, error) {
	switch backend {
	case "gearman":
		return NewGearmanPublisher(gearmanAddr), nil
	case "rabbitmq":
		return NewRabbitMQPublisher(rabbitMQURL), nil
	default:
		return nil, errors.New(`command: backend must be "gearman" or "rabbitmq"`)
	}
}

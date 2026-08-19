package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRabbitMQOneQueueCannotStarveAnother is the RabbitMQ counterpart of
// TestOneQueueCannotConsumeTheWholeJobBudget, and it exists because the
// claim "the RabbitMQ path already consumes per queue, so it never had the
// Gearman starvation problem" was asserted from the shape of connect()
// rather than measured.
//
// The shape is only half the argument. This consumer does open one channel,
// one Qos and one consumeLoop per queue - but all of those channels are
// multiplexed over a *single* TCP connection, and amqp091-go reads that
// connection in a single goroutine: dispatchN calls channel.recv
// synchronously (connection.go:928). That is structurally the same hazard
// as the library's Work loop in gearman.go, so the question of whether a
// blocked handler on one queue parks the reader for every queue is a real
// one and not answerable by looking at this package alone.
//
// It does not, and the reason is consumers.buffer (consumers.go): every
// consumer gets its own goroutine holding an *unbounded* slice between the
// connection reader and the delivery channel the application ranges over.
// The reader hands a delivery off and moves on; a consumeLoop stuck in a
// Handler backs up into that slice, not into the connection. Qos(prefetch)
// bounds how far it can back up, per channel.
//
// So this test holds every one of the busy queue's deliveries inside its
// Handler - a Handler blocked in Enqueue behind MySQL, which is the real
// case (CLAUDE.md rule 3) - and insists a second queue is still served
// while they are held.
func TestRabbitMQOneQueueCannotStarveAnother(t *testing.T) {
	// Comfortably more than the prefetch below, so the broker has a real
	// backlog left for the busy queue after it has pushed all it may.
	const (
		busyMessages = 50
		prefetch     = 5
	)

	// Unique per run: this is a shared dev broker, and a leftover message
	// from a killed earlier run must not be able to satisfy the assertion.
	run := time.Now().UnixNano()
	busyQueue := fmt.Sprintf("queue_pkg_test_rmq_busy_%d", run)
	quietQueue := fmt.Sprintf("queue_pkg_test_rmq_quiet_%d", run)

	busyStarted := make(chan struct{}, busyMessages)
	release := make(chan struct{})
	quietHandled := make(chan struct{}, 1)

	router := Router{
		busyQueue: func(_ context.Context, _ []byte) error {
			busyStarted <- struct{}{}
			<-release
			return nil
		},
		quietQueue: func(_ context.Context, _ []byte) error {
			select {
			case quietHandled <- struct{}{}:
			default:
			}
			return nil
		},
	}

	consumer := NewRabbitMQConsumer(rabbitmqURL, router, prefetch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := consumer.Start(ctx); err != nil {
		skipOrFailService(t, "no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}
	// close(release) before Stop, or Stop waits out its drain timeout on
	// handlers this test is deliberately holding.
	defer func() {
		close(release)
		consumer.Stop()
	}()

	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()

	// The queues are declared by the consumer with these exact arguments;
	// publishing to them needs no declaration of its own, but cleaning up
	// does need a handle, so delete them at the end either way.
	t.Cleanup(func() {
		cleanCh, err := conn.Channel()
		if err != nil {
			return
		}
		defer cleanCh.Close()
		for _, q := range []string{busyQueue, quietQueue} {
			// Purge before delete: an unacked delivery is redelivered on
			// channel close, and a queue left behind on a shared dev
			// broker is litter.
			cleanCh.QueuePurge(q, false)
			cleanCh.QueueDelete(q, false, false, false)
		}
	})

	publish := func(queue string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if err := ch.PublishWithContext(ctx, "", queue, false, false,
				amqp.Publishing{Body: []byte(`{"n":1}`)}); err != nil {
				t.Fatalf("publish to %s: %v", queue, err)
			}
		}
	}

	// Build the backlog first and wait until it is genuinely stuck: every
	// slot the prefetch allows is inside a blocked Handler, with more
	// messages still waiting at the broker. Without this wait the
	// assertion below could pass simply because the busy queue had not
	// started yet.
	publish(busyQueue, busyMessages)
	for i := 0; i < 1; i++ {
		select {
		case <-busyStarted:
		case <-time.After(10 * time.Second):
			t.Fatal("the busy queue's handler never ran; the test never reached the state it means to test")
		}
	}

	// Only now the second queue gets anything at all.
	publish(quietQueue, 1)

	select {
	case <-quietHandled:
	case <-time.After(5 * time.Second):
		t.Fatalf("a queue with %d held messages starved a second queue: its single message "+
			"was not handled within 5s", busyMessages)
	}
}

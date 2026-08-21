package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRabbitMQConsumerPrefetchKeepsBacklogAtTheBroker proves the point of
// Channel.Qos: with a consumer that cannot keep up, the surplus has to
// stay unacknowledged at the broker instead of being pushed into this
// process's memory.
//
// The observable is the queue's "ready" count. Ready messages are the ones
// the broker still holds and has not handed to a consumer; deliveries sent
// to a consumer but not yet acked count as unacked, not ready. So with a
// prefetch of N and a handler that never returns, at most N messages leave
// the broker and everything beyond that must remain ready. Without Qos the
// broker pushes the lot and ready drops to zero - which is precisely the
// unbounded in-memory backlog the cap exists to prevent.
func TestRabbitMQConsumerPrefetchKeepsBacklogAtTheBroker(t *testing.T) {
	const (
		queueName = "queue_pkg_test_prefetch"
		prefetch  = 5
		published = 200
	)

	// A handler that blocks until the test releases it, so nothing is ever
	// acked and the prefetch window stays full.
	release := make(chan struct{})
	var entered atomic.Int64
	router := Router{
		queueName: func(ctx context.Context, _ []byte) error {
			entered.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		},
	}

	consumer := NewRabbitMQConsumer(rabbitmqURL, router, prefetch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		skipOrFailService(t, "no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()

	t.Cleanup(func() {
		close(release)
		consumer.Stop()

		cleanConn, err := amqp.Dial(rabbitmqURL)
		if err != nil {
			t.Logf("cleanup: dial: %v", err)
			return
		}
		defer cleanConn.Close()
		cleanCh, err := cleanConn.Channel()
		if err != nil {
			t.Logf("cleanup: channel: %v", err)
			return
		}
		defer cleanCh.Close()
		if _, err := cleanCh.QueueDelete(queueName, false, false, false); err != nil {
			t.Logf("cleanup: delete %s: %v", queueName, err)
		}
	})

	declareTestQueue(t, ch, queueName)
	for i := 0; i < published; i++ {
		if err := ch.PublishWithContext(ctx, "", queueName, false, false,
			amqp.Publishing{Body: []byte(`{"seq":1}`)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	if _, err := consumer.Start(ctx); err != nil {
		skipOrFailService(t, "no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}

	// Let the broker push whatever it is willing to push.
	deadline := time.Now().Add(3 * time.Second)
	for entered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if entered.Load() == 0 {
		t.Fatal("no message was ever delivered")
	}
	time.Sleep(500 * time.Millisecond)

	// Count what the broker still holds, on a channel of our own.
	infoCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("open info channel: %v", err)
	}
	defer infoCh.Close()
	info := declareTestQueue(t, infoCh, queueName)

	// Everything beyond the prefetch window must still be at the broker.
	// Compared generously: the exact split between ready and unacked moves
	// around a little, the point is that it is not "everything".
	minReady := published - prefetch - 10
	if info.Messages < minReady {
		t.Fatalf("only %d of %d messages are still queued at the broker (expected at least %d): "+
			"the prefetch limit is not in effect and the backlog is being buffered in-process",
			info.Messages, published, minReady)
	}

	// And the handler must not have been handed more than the window.
	if got := entered.Load(); got > prefetch {
		t.Fatalf("%d handlers were entered with a prefetch of %d", got, prefetch)
	}
}

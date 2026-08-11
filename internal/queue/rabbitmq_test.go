package queue

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// rabbitmqURL points at the local dev RabbitMQ broker documented in
// .claude/specs/ressources.txt.
const rabbitmqURL = "amqp://statusengine:statusengine@127.0.0.1:5672/"

func TestRabbitMQConsumerEndToEnd(t *testing.T) {
	received := make(chan []byte, 1)
	queueName := "queue_pkg_test_queue"
	router := Router{
		queueName: func(_ context.Context, payload []byte) error {
			received <- payload
			return nil
		},
	}

	consumer := NewRabbitMQConsumer(rabbitmqURL, router, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := consumer.Start(ctx)
	if err != nil {
		t.Skipf("no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}
	defer consumer.Stop()

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

	want := []byte(`{"hello":"world"}`)
	if err := ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{Body: want}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-received:
		if string(got) != string(want) {
			t.Fatalf("handler received %s, want %s", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the Router handler to be invoked")
	}

	select {
	case msg, ok := <-out:
		if !ok {
			t.Fatal("output channel closed before delivering the raw message")
		}
		if msg.Queue != queueName {
			t.Fatalf("raw message queue = %q, want %q", msg.Queue, queueName)
		}
		if string(msg.Payload) != string(want) {
			t.Fatalf("raw message payload = %s, want %s", msg.Payload, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the raw message on the output channel")
	}

	if err := consumer.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected output channel to be closed after Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("output channel was not closed after Stop")
	}
}

// TestRabbitMQConsumerReconnectsAfterConnectionDrop proves the reconnect
// supervisor added to satisfy CLAUDE.md rule 6 actually recovers from an
// unexpected disconnect, not just a graceful Stop: it force-closes the
// consumer's live connection out from under it (as a network blip or a
// broker restart would), then publishes a second message and confirms the
// Router handler still gets invoked once superviseReconnects has redialed
// and resumed consuming.
func TestRabbitMQConsumerReconnectsAfterConnectionDrop(t *testing.T) {
	received := make(chan []byte, 2)
	queueName := "queue_pkg_test_reconnect_queue"
	router := Router{
		queueName: func(_ context.Context, payload []byte) error {
			received <- payload
			return nil
		},
	}

	consumer := NewRabbitMQConsumer(rabbitmqURL, router, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := consumer.Start(ctx); err != nil {
		t.Skipf("no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}
	defer consumer.Stop()

	publish := func(body []byte) {
		t.Helper()
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
		if err := ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{Body: body}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	publish([]byte(`{"seq":1}`))
	select {
	case got := <-received:
		if string(got) != `{"seq":1}` {
			t.Fatalf("first message = %s, want {\"seq\":1}", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first message")
	}

	// Force-close the consumer's current connection directly, simulating an
	// unexpected drop (network blip, broker restart) rather than a Stop()
	// call - this is what superviseReconnects exists to recover from.
	consumer.mu.Lock()
	conn := consumer.conn
	consumer.mu.Unlock()
	if err := conn.Close(); err != nil {
		t.Fatalf("force-close consumer connection: %v", err)
	}

	// Give superviseReconnects time to notice the drop and redial
	// (reconnectDelay plus setup) before publishing the next message.
	deadline := time.Now().Add(10 * time.Second)
	for {
		consumer.mu.Lock()
		reconnected := consumer.conn != conn
		consumer.mu.Unlock()
		if reconnected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("consumer did not reconnect within 10s of the connection being dropped")
		}
		time.Sleep(100 * time.Millisecond)
	}

	publish([]byte(`{"seq":2}`))
	select {
	case got := <-received:
		if string(got) != `{"seq":2}` {
			t.Fatalf("second message = %s, want {\"seq\":2}", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the second message after reconnect - the consumer did not resume consuming")
	}
}

func TestRabbitMQConsumerStopWithoutStartIsSafe(t *testing.T) {
	c := NewRabbitMQConsumer(rabbitmqURL, Router{}, 100)
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop without Start should be a no-op, got: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("second Stop should also be a no-op, got: %v", err)
	}
}

func TestRabbitMQConsumerStartFailsFastWhenUnreachable(t *testing.T) {
	c := NewRabbitMQConsumer("amqp://guest:guest@127.0.0.1:1/", Router{}, 100) // port 1: nothing listens
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Start(ctx); err == nil {
		t.Fatal("expected Start to fail against an unreachable broker")
	}
}

func TestRabbitMQConsumerStartFailsFastOnInvalidURL(t *testing.T) {
	c := NewRabbitMQConsumer("not-a-valid-amqp-url", Router{}, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Start(ctx); err == nil {
		t.Fatal("expected Start to fail on a malformed AMQP URL")
	}
}

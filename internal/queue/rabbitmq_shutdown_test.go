package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRabbitMQConsumerStopUnderLoadClosesOutputSafely covers the shutdown
// path that matters in production: Stop arriving while delivery loops are
// still mid-message. The raw-message channel is closed by the reconnect
// supervisor but written to by every consumeLoop, so closing it a moment
// too early is a "send on closed channel" - a panic on a goroutine this
// package doesn't own and therefore cannot recover from.
//
// It has to be run with -race to be worth much: the window is narrow
// enough that an unsynchronized close usually gets away with it, but the
// race detector flags the unsynchronized access every time.
func TestRabbitMQConsumerStopUnderLoadClosesOutputSafely(t *testing.T) {
	queueName := "queue_pkg_test_stop_under_load"

	// A handler slow enough that Stop reliably lands while several delivery
	// loops are still inside one. inFlight tracks how many are mid-handler
	// at any moment, so the test can also assert what Stop promises: that
	// nothing is still enqueueing into the pipeline once it has returned.
	var inFlight atomic.Int64
	router := Router{
		queueName: func(_ context.Context, _ []byte) error {
			inFlight.Add(1)
			defer inFlight.Add(-1)
			// Long enough that Stop reliably lands inside a handler rather
			// than between two. Only one handler per queue is ever in
			// flight (the loop is serial, and closing the connection stops
			// it after the current one), so this doesn't scale with the
			// message count.
			time.Sleep(25 * time.Millisecond)
			return nil
		},
	}

	consumer := NewRabbitMQConsumer(rabbitmqURL, router)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := consumer.Start(ctx)
	if err != nil {
		t.Skipf("no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}
	defer consumer.Stop()

	// Drain out exactly the way cmd/app/main.go does, so the send side is
	// genuinely live when the close happens.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range out {
		}
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
	if _, err := ch.QueueDeclare(queueName, false, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	const messages = 200
	for i := 0; i < messages; i++ {
		if err := ch.PublishWithContext(ctx, "", queueName, false, false,
			amqp.Publishing{Body: []byte(`{"seq":1}`)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Stop deliberately mid-flight, without waiting for the backlog.
	time.Sleep(50 * time.Millisecond)
	if err := consumer.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// cmd/app/main.go flushes every BulkInserter immediately after this
	// returns, on the strength of "the consumer is stopped, so no new data
	// comes in". A Handler still running here would enqueue rows after
	// their inserter had already flushed, and those rows are lost.
	if running := inFlight.Load(); running != 0 {
		t.Fatalf("Stop returned with %d handler(s) still running; rows they enqueue now are lost", running)
	}

	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("raw-message channel was never closed after Stop")
	}
}

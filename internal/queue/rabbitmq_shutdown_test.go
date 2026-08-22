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
	// This test deliberately stops mid-backlog, so whatever the cancel's
	// drain does not reach is requeued when the connection goes away.
	// Without cleaning up, every run leaves messages behind on a shared
	// dev broker, and after a few dozen runs the accumulated backlog
	// changes the timing enough to make the test itself flaky.
	cleanupTestQueue(t, queueName)

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
			// flight, the loop being serial - but Stop's basic.cancel lets
			// it work through everything the broker already pushed, so the
			// wait here does scale with the prefetch window (100 x 25ms),
			// not with the message count.
			time.Sleep(25 * time.Millisecond)
			return nil
		},
	}

	consumer := NewRabbitMQConsumer(rabbitmqURL, router, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := consumer.Start(ctx)
	if err != nil {
		skipOrFailService(t, "no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
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
	declareTestQueue(t, ch, queueName)

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

// TestRabbitMQStopAcksTheMessageItWasHandling pins the one promise
// basic.cancel buys: a message whose Handler was still running when Stop
// arrived is acknowledged, not redelivered.
//
// Before the cancel, Stop closed the channels first, so the Handler
// finished, wrote its rows, and then acked into a channel that no longer
// existed - the broker logged
//
//	operation basic.ack caused a connection exception channel_error
//
// and requeued a message that had in fact been processed. The next start
// did the work again; only rule 6's idempotent writes kept that harmless.
//
// The observable is the queue depth after Stop: 0 if the ack landed, 1 if
// the broker took the message back. One message and one Handler, so
// nothing here depends on timing beyond Stop landing inside the Handler,
// which the entered channel guarantees.
func TestRabbitMQStopAcksTheMessageItWasHandling(t *testing.T) {
	queueName := "queue_pkg_test_stop_ack"
	cleanupTestQueue(t, queueName)

	entered := make(chan struct{}, 1)
	router := Router{
		queueName: func(_ context.Context, _ []byte) error {
			entered <- struct{}{}
			// Long enough that Stop is reliably inside this Handler, short
			// enough to stay well under stopDrainTimeout so the drain
			// really does wait for it.
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	}

	consumer := NewRabbitMQConsumer(rabbitmqURL, router, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := consumer.Start(ctx)
	if err != nil {
		skipOrFailService(t, "no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}
	defer consumer.Stop()
	go func() {
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

	if err := ch.PublishWithContext(ctx, "", queueName, false, false,
		amqp.Publishing{Body: []byte(`{"seq":1}`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was never invoked")
	}

	if err := consumer.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Stop has closed the consumer's connection, so a requeue has already
	// happened at the broker by now if it was going to. Sampling for a
	// while rather than once anyway: a single early read could catch the
	// moment before the broker books the message back.
	inspect, err := conn.Channel()
	if err != nil {
		t.Fatalf("open inspect channel: %v", err)
	}
	defer inspect.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		q := declareTestQueue(t, inspect, queueName)
		if q.Messages != 0 {
			t.Fatalf("queue holds %d message(s) after Stop; the handler finished, so this one "+
				"was processed and then redelivered - its acknowledgement was lost", q.Messages)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

package queue

import (
	"context"
	"testing"
	"time"
)

// No RabbitMQ broker is reachable in this dev environment (unlike Gearman
// and MySQL, see .claude/specs/ressources.txt), so these tests cover error
// handling and lifecycle safety rather than a live end-to-end round trip -
// the decode/dispatch logic itself is already covered by router_test.go
// and shared with GearmanConsumer via the same Router.

func TestRabbitMQConsumerStopWithoutStartIsSafe(t *testing.T) {
	c := NewRabbitMQConsumer("amqp://guest:guest@127.0.0.1:5672/", Router{})
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop without Start should be a no-op, got: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("second Stop should also be a no-op, got: %v", err)
	}
}

func TestRabbitMQConsumerStartFailsFastWhenUnreachable(t *testing.T) {
	c := NewRabbitMQConsumer("amqp://guest:guest@127.0.0.1:1/", Router{}) // port 1: nothing listens
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Start(ctx); err == nil {
		t.Fatal("expected Start to fail against an unreachable broker")
	}
}

func TestRabbitMQConsumerStartFailsFastOnInvalidURL(t *testing.T) {
	c := NewRabbitMQConsumer("not-a-valid-amqp-url", Router{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Start(ctx); err == nil {
		t.Fatal("expected Start to fail on a malformed AMQP URL")
	}
}

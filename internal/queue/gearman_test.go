package queue

import (
	"context"
	"testing"
	"time"

	gearmanClient "github.com/mikespook/gearman-go/client"
)

// gearmanAddr points at the local dev Gearman job server documented in
// .claude/specs/ressources.txt.
const gearmanAddr = "127.0.0.1:4730"

func TestGearmanConsumerEndToEnd(t *testing.T) {
	received := make(chan []byte, 1)
	fnName := "queue_pkg_test_fn"
	router := Router{
		fnName: func(_ context.Context, payload []byte) error {
			received <- payload
			return nil
		},
	}

	consumer := NewGearmanConsumer(gearmanAddr, router)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := consumer.Start(ctx)
	if err != nil {
		t.Skipf("no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}
	defer consumer.Stop()

	cli, err := gearmanClient.New(gearmanClient.Network, gearmanAddr)
	if err != nil {
		t.Fatalf("gearman client: %v", err)
	}
	defer cli.Close()

	want := []byte(`{"hello":"world"}`)
	if _, err := cli.DoBg(fnName, want, gearmanClient.JobNormal); err != nil {
		t.Fatalf("submit background job: %v", err)
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
		if msg.Queue != fnName {
			t.Fatalf("raw message queue = %q, want %q", msg.Queue, fnName)
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

func TestGearmanConsumerStopWithoutStartIsSafe(t *testing.T) {
	c := NewGearmanConsumer(gearmanAddr, Router{})
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop without Start should be a no-op, got: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("second Stop should also be a no-op, got: %v", err)
	}
}

func TestGearmanConsumerStartFailsFastWhenUnreachable(t *testing.T) {
	c := NewGearmanConsumer("127.0.0.1:1", Router{}) // port 1: nothing listens
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Start(ctx); err == nil {
		t.Fatal("expected Start to fail against an unreachable job server")
	}
}

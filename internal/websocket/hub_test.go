package websocket

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// newTestClient creates a Client with the given buffer size, registers it
// with hub and returns it. It does not start real network pumps - tests
// drive c.send directly, exactly as the Hub's dispatch loop would.
func newTestClient(hub *Hub, bufSize int, topics ...string) *Client {
	topicSet := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		topicSet[t] = struct{}{}
	}
	c := &Client{
		hub:    hub,
		send:   make(chan []byte, bufSize),
		topics: topicSet,
	}
	hub.register <- c
	return c
}

func TestHubDispatchRespectsSubscription(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	subscribed := newTestClient(hub, 4, "statusngin_hoststatus")
	everything := newTestClient(hub, 4)

	hub.Publish("statusngin_hoststatus", []byte(`{"name":"localhost"}`))
	hub.Publish("statusngin_servicestatus", []byte(`{"description":"PING"}`))

	// Only the hoststatus event should reach the subscribed client.
	select {
	case msg := <-subscribed.send:
		var out outboundMessage
		if err := json.Unmarshal(msg, &out); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if out.Topic != "statusngin_hoststatus" {
			t.Fatalf("expected statusngin_hoststatus, got %s", out.Topic)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscribed client message")
	}

	select {
	case msg := <-subscribed.send:
		t.Fatalf("subscribed client should not receive servicestatus event, got %s", msg)
	case <-time.After(50 * time.Millisecond):
	}

	// The wildcard client should receive both.
	for i := 0; i < 2; i++ {
		select {
		case <-everything.send:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for wildcard client message #%d", i)
		}
	}
}

func TestHubDispatchNeverBlocksOnFullClientBuffer(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	slow := newTestClient(hub, 1) // tiny buffer, never drained
	fast := newTestClient(hub, 16)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			hub.Publish("statusngin_hoststatus", []byte(`{}`))
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish loop blocked - slow client backpressured the hub")
	}

	// The fast client should still have received messages up to its own
	// buffer capacity, proving the slow client didn't starve dispatch.
	select {
	case <-fast.send:
	case <-time.After(time.Second):
		t.Fatal("fast client received nothing")
	}

	// The slow client's buffer (capacity 1) absorbed exactly one message;
	// every subsequent publish was dropped for it rather than blocking.
	if len(slow.send) != 1 {
		t.Fatalf("expected slow client's buffer to hold exactly 1 dropped-rest message, got %d", len(slow.send))
	}
}

func TestHubUnregisterClosesSendChannel(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	c := newTestClient(hub, 1)
	hub.unregister <- c

	select {
	case _, ok := <-c.send:
		if ok {
			t.Fatal("expected closed channel with no pending value")
		}
	case <-time.After(time.Second):
		t.Fatal("send channel was not closed after unregister")
	}
}

func TestHubShutdownClosesAllClients(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)

	c := newTestClient(hub, 1)
	time.Sleep(10 * time.Millisecond) // let registration land before cancel

	cancel()

	select {
	case _, ok := <-c.send:
		if ok {
			t.Fatal("expected closed channel after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("client was not closed on shutdown")
	}
}

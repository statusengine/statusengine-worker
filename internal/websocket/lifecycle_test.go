package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"
)

// The tests in this file all guard against the same failure mode: a
// goroutine parking forever on one of the Hub's three unbuffered
// lifecycle channels (register, unregister, updateSubscription) after Run
// has returned and nobody is reading them any more.
//
// That failure mode is a *hang*, not a wrong value, so every assertion
// here needs its own deadline - a broken implementation would otherwise
// wedge the test binary until the package timeout rather than reporting
// anything useful.
//
// It only became worth fixing once Run's lifetime stopped being "exactly
// once, until the process exits": with a restart or reload feature, a
// stopped Hub outlives its own goroutine and every park becomes a
// permanent leak of a goroutine and a file descriptor.

const lifecycleTimeout = 3 * time.Second

// stoppedHub returns a Hub whose Run has already returned.
func stoppedHub(t *testing.T) *Hub {
	t.Helper()

	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		hub.Run(ctx)
	}()

	cancel()
	select {
	case <-runDone:
	case <-time.After(lifecycleTimeout):
		t.Fatal("Hub.Run did not return after its context was cancelled")
	}
	return hub
}

// wsURL turns an httptest server's URL into a dialable ws:// one.
func wsURL(srv *httptest.Server, query string) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + query
}

// TestServeWSDoesNotParkAfterHubStopped covers the worst of the three
// sends: ServeWS registers *before* starting either pump, so a park there
// strands a live connection with no reader, no writer and nothing that
// will ever close it.
func TestServeWSDoesNotParkAfterHubStopped(t *testing.T) {
	hub := stoppedHub(t)

	served := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(served)
		ServeWS(hub, w, r, nil)
	}))
	defer srv.Close()

	conn, _, err := gorillaws.DefaultDialer.Dial(wsURL(srv, ""), nil)
	if err != nil {
		// The handshake itself may or may not complete depending on how
		// quickly ServeWS bails; either outcome is fine, what matters is
		// that the handler below returns.
		t.Logf("dial against stopped hub failed (acceptable): %v", err)
	} else {
		defer conn.Close()
	}

	select {
	case <-served:
	case <-time.After(lifecycleTimeout):
		t.Fatal("ServeWS never returned - it is parked on hub.register with no reader")
	}
}

// TestClientPumpsExitAfterHubStopped covers the unregister send: a client
// connected while the Hub was alive must still be able to tear itself
// down once the Hub has stopped underneath it.
func TestClientPumpsExitAfterHubStopped(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		hub.Run(ctx)
	}()

	// Track the read pump specifically: it owns the unregister send.
	readPumpDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		client := &Client{
			hub:    hub,
			conn:   conn,
			id:     "lifecycle-test",
			send:   make(chan []byte, sendBufferSize),
			topics: map[string]struct{}{},
		}
		select {
		case hub.register <- client:
		case <-hub.done:
			conn.Close()
			return
		}
		go client.writePump()
		go func() {
			defer close(readPumpDone)
			client.readPump()
		}()
	}))
	defer srv.Close()

	conn, _, err := gorillaws.DefaultDialer.Dial(wsURL(srv, ""), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Let the registration land before pulling the rug out.
	waitFor(t, hub.HasClients, "the client to register")

	cancel()
	select {
	case <-runDone:
	case <-time.After(lifecycleTimeout):
		t.Fatal("Hub.Run did not return after cancel")
	}

	select {
	case <-readPumpDone:
	case <-time.After(lifecycleTimeout):
		t.Fatal("readPump never returned - it is parked on hub.unregister with no reader")
	}
}

// TestHubRunIsSingleUse documents the contract in the Hub type comment: a
// Hub is not restartable, and calling Run again must fail loudly at worst
// - never with a "close of closed channel" panic out of the shutdown
// path, which is what a bare close(h.done) would produce.
func TestHubRunIsSingleUse(t *testing.T) {
	hub := stoppedHub(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the second Run returns immediately

	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.Run(ctx) // must not panic
	}()

	select {
	case <-done:
	case <-time.After(lifecycleTimeout):
		t.Fatal("a second Run call did not return")
	}

	// And the done channel must still read as closed for everyone else.
	select {
	case <-hub.done:
	default:
		t.Fatal("hub.done is no longer closed after a second Run")
	}
}

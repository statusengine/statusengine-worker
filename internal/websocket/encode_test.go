package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestEncodeMatchesOutboundMessage pins the hand-assembled wire frame to
// the struct that defines the format. encode skips json.Marshal on the hot
// path (see its comment), so nothing but this test stops the two from
// drifting apart - a stray byte here is a protocol break for every client.
func TestEncodeMatchesOutboundMessage(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		payload string
	}{
		{"plain topic", "statusngin_hoststatus", `{"hostname":"localhost"}`},
		{"array payload", "statusngin_hostchecks", `[{"a":1},{"b":2}]`},
		{"scalar payload", "statusngin_core_restart", `null`},
		{"topic needing escaping", `weird"topic\with//escapes`, `{"x":1}`},
		{"non-ascii topic", "täglich_überwachung_日本", `{"x":1}`},
		{"payload with unicode", "statusngin_notifications", `{"msg":"CRITICAL – 日本"}`},
	}

	hub := NewHub()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, err := json.Marshal(outboundMessage{Topic: tc.topic, Payload: json.RawMessage(tc.payload)})
			if err != nil {
				t.Fatalf("reference marshal: %v", err)
			}

			got := hub.encode(Event{Topic: tc.topic, Payload: []byte(tc.payload)})
			if !bytes.Equal(got, want) {
				t.Fatalf("encode mismatch\n got: %s\nwant: %s", got, want)
			}

			// Second call must hit the topic-prefix cache and still agree.
			if got := hub.encode(Event{Topic: tc.topic, Payload: []byte(tc.payload)}); !bytes.Equal(got, want) {
				t.Fatalf("cached encode mismatch\n got: %s\nwant: %s", got, want)
			}
		})
	}
}

// TestEncodeDoesNotAliasTheCachedPrefix guards the one way a []byte cache
// can go wrong: if encode appended into the cached prefix's own backing
// array instead of a fresh buffer, the second event under a topic would
// corrupt the first (or worse, a frame already handed to a client).
func TestEncodeDoesNotAliasTheCachedPrefix(t *testing.T) {
	hub := NewHub()

	first := hub.encode(Event{Topic: "statusngin_hoststatus", Payload: []byte(`{"n":1}`)})
	firstCopy := append([]byte(nil), first...)

	hub.encode(Event{Topic: "statusngin_hoststatus", Payload: []byte(`{"n":22222222}`)})

	if !bytes.Equal(first, firstCopy) {
		t.Fatalf("first frame was mutated by a later encode: got %s, want %s", first, firstCopy)
	}
}

// TestHasClientsTracksRegistrations covers the gate queue.publish uses to
// skip encoding entirely: it must be false before anyone connects, true
// while a client is registered, and false again once the Hub shuts down -
// otherwise the pipeline either wastes a marshal per event forever or, far
// worse, silently stops broadcasting to real clients.
func TestHasClientsTracksRegistrations(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	if hub.HasClients() {
		t.Fatal("a fresh Hub reports clients")
	}

	c := newTestClient(hub, 1)
	waitFor(t, hub.HasClients, "HasClients to become true after register")

	hub.unregister <- c
	waitFor(t, func() bool { return !hub.HasClients() }, "HasClients to become false after unregister")

	newTestClient(hub, 1)
	waitFor(t, hub.HasClients, "HasClients to become true for the second client")

	cancel()
	waitFor(t, func() bool { return !hub.HasClients() }, "HasClients to become false after shutdown")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

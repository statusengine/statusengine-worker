package main

import (
	"net/http"
	"testing"
)

// TestWebsocketServerTimeouts pins the deliberate asymmetry between the
// two HTTP servers. A bare &http.Server{} lets a client that never
// finishes sending its request headers hold a goroutine and a file
// descriptor indefinitely, so both servers need header and idle timeouts -
// but the /ws server must *not* get whole-request deadlines, because a
// WebSocket connection is a request that is supposed to last for hours.
// Someone tidying up "the missing timeouts" later would break every
// long-lived subscriber; this test is the tripwire.
func TestWebsocketServerTimeouts(t *testing.T) {
	srv := newWebsocketServer("127.0.0.1:8080", http.NewServeMux())

	if srv.Addr != "127.0.0.1:8080" {
		t.Errorf("Addr = %q, want 127.0.0.1:8080", srv.Addr)
	}
	if srv.ReadHeaderTimeout != wsReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, wsReadHeaderTimeout)
	}
	if srv.IdleTimeout != wsIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", srv.IdleTimeout, wsIdleTimeout)
	}
	if srv.ReadTimeout != 0 {
		t.Errorf("ReadTimeout = %v, want 0: a whole-request deadline would cut off long-lived WebSocket connections", srv.ReadTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0: the write pump sets its own per-write deadline instead", srv.WriteTimeout)
	}
}

// TestMetricsServerTimeouts covers the other side: /metrics serves
// ordinary short-lived requests, so every timeout applies.
func TestMetricsServerTimeouts(t *testing.T) {
	srv := newMetricsServer("127.0.0.1:9105", http.NewServeMux())

	if srv.ReadHeaderTimeout != metricsReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, metricsReadHeaderTimeout)
	}
	if srv.ReadTimeout != metricsReadTimeout {
		t.Errorf("ReadTimeout = %v, want %v", srv.ReadTimeout, metricsReadTimeout)
	}
	if srv.WriteTimeout != metricsWriteTimeout {
		t.Errorf("WriteTimeout = %v, want %v", srv.WriteTimeout, metricsWriteTimeout)
	}
	if srv.IdleTimeout != metricsIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", srv.IdleTimeout, metricsIdleTimeout)
	}
}

// TestResolveAPIKeysGeneratesWhenUnconfigured covers the guarantee /ws
// now rests on: an unconfigured worker still gets a key, so the endpoint
// is never served unauthenticated (see internal/websocket/auth.go's
// authorized, which treats an empty set as "no auth").
func TestResolveAPIKeysGeneratesWhenUnconfigured(t *testing.T) {
	keys := resolveAPIKeys("")
	if len(keys) != 1 {
		t.Fatalf("resolveAPIKeys(\"\") returned %d keys, want exactly 1 generated key", len(keys))
	}
	for k := range keys {
		if len(k) != 64 { // 32 random bytes, hex-encoded
			t.Errorf("generated key is %d chars, want 64 hex chars", len(k))
		}
	}

	// Two runs must not agree, or "random" isn't.
	other := resolveAPIKeys("")
	for k := range keys {
		if _, same := other[k]; same {
			t.Error("two generated keys were identical")
		}
	}
}

// TestResolveAPIKeysPrefersConfigured makes sure the generator only ever
// acts as a fallback and never quietly adds itself alongside real keys.
func TestResolveAPIKeysPrefersConfigured(t *testing.T) {
	keys := resolveAPIKeys(" key-one , key-two ")

	if len(keys) != 2 {
		t.Fatalf("got %d keys, want the 2 configured ones", len(keys))
	}
	for _, want := range []string{"key-one", "key-two"} {
		if _, ok := keys[want]; !ok {
			t.Errorf("configured key %q missing from the resolved set", want)
		}
	}
}

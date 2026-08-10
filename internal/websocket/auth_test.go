package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	gorillaws "github.com/gorilla/websocket"
)

func TestExtractAPIKey(t *testing.T) {
	tests := []struct {
		name   string
		header map[string]string
		query  string
		want   string
	}{
		{name: "no key anywhere", want: ""},
		{name: "Authorization Bearer prefix", header: map[string]string{"Authorization": "Bearer secret123"}, want: "secret123"},
		{name: "Authorization bare key (no Bearer prefix)", header: map[string]string{"Authorization": "secret123"}, want: "secret123"},
		{name: "X-Api-Key header", header: map[string]string{"X-Api-Key": "secret123"}, want: "secret123"},
		{name: "api_key query parameter", query: "api_key=secret123", want: "secret123"},
		{
			name:   "Authorization header wins over query parameter",
			header: map[string]string{"Authorization": "Bearer header-key"},
			query:  "api_key=query-key",
			want:   "header-key",
		},
		{
			name:   "X-Api-Key header wins over query parameter",
			header: map[string]string{"X-Api-Key": "header-key"},
			query:  "api_key=query-key",
			want:   "header-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := "/ws"
			if tt.query != "" {
				u += "?" + tt.query
			}
			r := httptest.NewRequest(http.MethodGet, u, nil)
			for k, v := range tt.header {
				r.Header.Set(k, v)
			}
			if got := extractAPIKey(r); got != tt.want {
				t.Fatalf("extractAPIKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAuthorized(t *testing.T) {
	validKeys := map[string]struct{}{"good-key": {}}

	tests := []struct {
		name      string
		validKeys map[string]struct{}
		header    map[string]string
		query     string
		want      bool
	}{
		{name: "no configured keys allows every request", validKeys: nil, want: true},
		{name: "valid key via header", validKeys: validKeys, header: map[string]string{"X-Api-Key": "good-key"}, want: true},
		{name: "valid key via query parameter", validKeys: validKeys, query: "api_key=good-key", want: true},
		{name: "wrong key rejected", validKeys: validKeys, header: map[string]string{"X-Api-Key": "wrong-key"}, want: false},
		{name: "missing key rejected when keys are configured", validKeys: validKeys, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := "/ws"
			if tt.query != "" {
				u += "?" + tt.query
			}
			r := httptest.NewRequest(http.MethodGet, u, nil)
			for k, v := range tt.header {
				r.Header.Set(k, v)
			}
			if got := authorized(r, tt.validKeys); got != tt.want {
				t.Fatalf("authorized() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServeWSRejectsUnauthorizedBeforeUpgrading(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	validKeys := map[string]struct{}{"good-key": {}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWS(hub, w, r, validKeys)
	}))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	if _, resp, err := gorillaws.DefaultDialer.Dial(wsURL, nil); err == nil {
		t.Fatal("expected the handshake to fail without a valid API key")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got response %+v (dial err: %v)", resp, err)
	}
}

func TestServeWSAcceptsValidAPIKey(t *testing.T) {
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	validKeys := map[string]struct{}{"good-key": {}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWS(hub, w, r, validKeys)
	}))
	t.Cleanup(srv.Close)

	// Header-based auth (recommended path for real clients).
	headerURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	header := http.Header{"Authorization": []string{"Bearer good-key"}}
	conn, _, err := gorillaws.DefaultDialer.Dial(headerURL, header)
	if err != nil {
		t.Fatalf("dial with Authorization header: %v", err)
	}
	conn.Close()

	// Query-parameter-based auth (the browser demo client's fallback).
	queryURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?api_key=" + url.QueryEscape("good-key")
	conn, _, err = gorillaws.DefaultDialer.Dial(queryURL, nil)
	if err != nil {
		t.Fatalf("dial with api_key query parameter: %v", err)
	}
	conn.Close()
}

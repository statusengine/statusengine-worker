package websocket

import (
	"net/http"
	"strings"
)

// extractAPIKey pulls the caller-supplied API key out of r. Real clients
// should send it via the Authorization header - either "Authorization:
// Bearer <key>" or a bare "Authorization: <key>" - or via X-Api-Key; the
// api_key query parameter exists only for the browser-based demo client
// (web/ws-test-client.html), since browser JavaScript's WebSocket API
// cannot set custom headers on the handshake request. A header always
// takes precedence over the query parameter when both are present.
func extractAPIKey(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	if key := r.Header.Get("X-Api-Key"); key != "" {
		return key
	}
	return r.URL.Query().Get("api_key")
}

// authorized reports whether r carries one of validKeys. An empty/nil
// validKeys disables authentication entirely - every request is
// authorized.
//
// The running worker never takes that branch: cmd/app's resolveAPIKeys
// always hands ServeWS a non-empty set, generating a random per-run key
// when none is configured, so /ws is not served unauthenticated. The
// branch remains for tests, which pass nil to exercise the Hub without
// building a key set for every case.
func authorized(r *http.Request, validKeys map[string]struct{}) bool {
	if len(validKeys) == 0 {
		return true
	}
	key := extractAPIKey(r)
	if key == "" {
		return false
	}
	_, ok := validKeys[key]
	return ok
}

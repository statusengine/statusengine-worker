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
// authorized - which is the worker's default (opt in via the -api-keys
// flag/STATUSENGINE_API_KEYS env var), consistent with every other
// security-relevant CLAUDE.md option (e.g. enableOpenITCockpitTweaks).
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

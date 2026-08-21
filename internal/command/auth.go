package command

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// extractAPIKey pulls the caller-supplied key out of r's headers, accepting
// "Authorization: Bearer <key>", a bare "Authorization: <key>", or
// "X-Api-Key: <key>".
//
// Note what is *not* accepted, unlike internal/websocket/auth.go: the
// api_key query parameter. That exists on /ws only because browser
// JavaScript cannot set headers on a WebSocket handshake, which is a real
// constraint with no equivalent here - every client of this endpoint sends
// an ordinary POST and can set a header. A secret in a URL ends up in proxy
// logs, access logs and browser history, and this key is considerably worse
// to leak than the read-only one: it controls the monitoring core.
func extractAPIKey(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	return strings.TrimSpace(r.Header.Get("X-Api-Key"))
}

// authorized reports whether r carries one of validKeys.
//
// Unlike the /ws check, an empty validKeys authorizes nothing. There, an
// empty set means "auth disabled" and cmd/app makes sure that never
// happens; here the same situation is handled one level up - with no keys
// configured the endpoint is not registered at all - so this function's
// safe answer for an empty set is no.
//
// The comparison is constant-time. A map lookup would leak key content
// through timing, and unlike the /ws key this one is worth the attempt: a
// caller can probe it as fast as it can send requests.
func authorized(r *http.Request, validKeys map[string]struct{}) bool {
	presented := extractAPIKey(r)
	if presented == "" || len(validKeys) == 0 {
		return false
	}
	// Every key is compared even after a match, so the work done does not
	// depend on which key matched or whether one did.
	var match int
	for key := range validKeys {
		match |= subtle.ConstantTimeCompare([]byte(presented), []byte(key))
	}
	return match == 1
}

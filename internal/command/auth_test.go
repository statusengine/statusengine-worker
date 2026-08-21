package command

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func keySet(keys ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return set
}

func TestAuthorizedAcceptsEveryHeaderForm(t *testing.T) {
	keys := keySet("secret", "second-secret")
	for name, apply := range map[string]func(*http.Request){
		"Authorization: Bearer": func(r *http.Request) { r.Header.Set("Authorization", "Bearer secret") },
		"Authorization: bare":   func(r *http.Request) { r.Header.Set("Authorization", "secret") },
		"X-Api-Key":             func(r *http.Request) { r.Header.Set("X-Api-Key", "secret") },
		"second key in the set": func(r *http.Request) { r.Header.Set("X-Api-Key", "second-secret") },
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, Path, nil)
			apply(r)
			if !authorized(r, keys) {
				t.Error("authorized() = false, want true")
			}
		})
	}
}

func TestAuthorizedRejects(t *testing.T) {
	keys := keySet("secret")
	for name, apply := range map[string]func(*http.Request){
		"no key at all": func(*http.Request) {},
		"wrong key":     func(r *http.Request) { r.Header.Set("X-Api-Key", "wrong") },
		"empty header":  func(r *http.Request) { r.Header.Set("X-Api-Key", "") },
		"key prefix":    func(r *http.Request) { r.Header.Set("X-Api-Key", "sec") },
		"key with tail": func(r *http.Request) { r.Header.Set("X-Api-Key", "secretary") },
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, Path, nil)
			apply(r)
			if authorized(r, keys) {
				t.Error("authorized() = true, want false")
			}
		})
	}
}

// The /ws handler accepts ?api_key= because browser JavaScript cannot set
// headers on a WebSocket handshake. No such constraint exists for a POST,
// and a URL carrying this key - which controls the monitoring core - lands
// in proxy logs, access logs and browser history. Copying the /ws auth
// helper wholesale is the obvious way to reintroduce it, so this test
// exists to fail loudly if that happens.
func TestQueryParameterIsNotAcceptedAsAKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, Path+"?api_key=secret", nil)
	if authorized(r, keySet("secret")) {
		t.Fatal("the api_key query parameter must not authenticate a command request")
	}
}

// The /ws check treats an empty key set as "authentication disabled". Here
// the same set must mean the opposite: cmd/app does not register the
// endpoint without keys, so if this is ever reached with an empty set,
// something has gone wrong and open is the one answer that must not follow.
func TestEmptyKeySetAuthorizesNothing(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, Path, nil)
	r.Header.Set("X-Api-Key", "anything")
	if authorized(r, nil) {
		t.Fatal("an empty key set must authorize nothing")
	}
	if authorized(r, map[string]struct{}{}) {
		t.Fatal("an empty key set must authorize nothing")
	}
}

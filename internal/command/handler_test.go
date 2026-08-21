package command

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakePublisher records what the handler hands it, and can be told to fail.
type fakePublisher struct {
	mu       sync.Mutex
	payloads [][]byte
	err      error
}

func (f *fakePublisher) Publish(_ context.Context, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.payloads = append(f.payloads, append([]byte(nil), payload...))
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func (f *fakePublisher) published() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.payloads
}

const testKey = "test-command-key"

func post(t *testing.T, h http.Handler, body string, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, Path, strings.NewReader(body))
	if key != "" {
		r.Header.Set("X-Api-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func newTestHandler() (*Handler, *fakePublisher) {
	pub := &fakePublisher{}
	return NewHandler(pub, keySet(testKey)), pub
}

func TestHandlerAcceptsASingleCommand(t *testing.T) {
	h, pub := newTestHandler()
	w := post(t, h, `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}`, testKey)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusAccepted, w.Body)
	}
	var got response
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if got.Accepted != 1 {
		t.Errorf("accepted = %d, want 1", got.Accepted)
	}
	if len(pub.published()) != 1 {
		t.Fatalf("published %d messages, want 1", len(pub.published()))
	}
}

// 202 rather than 200 is load-bearing. Publishing puts the command on a
// queue; whether Naemon acts on it is something this process cannot observe
// and will never learn, because the broker module has no reply path and
// logs nothing for a command it does not recognise. A 200 would be read as
// "done".
func TestHandlerAnswers202NotOK(t *testing.T) {
	h, _ := newTestHandler()
	w := post(t, h, `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}`, testKey)
	if w.Code == http.StatusOK {
		t.Fatal("status is 200, which reads as 'executed' - it must be 202 Accepted")
	}
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
}

// One request is one message on the queue, whatever its bulk size - the
// broker unpacks "messages" itself. Publishing per command instead would
// multiply the round trips for exactly the payload shape that exists to
// avoid them.
func TestHandlerPublishesOneMessagePerRequest(t *testing.T) {
	h, pub := newTestHandler()
	body := `{"messages":[
		{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h1"},
		{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h2"},
		{"Command":"schedule_check","Data":{"host_name":"h","schedule_time":1}}
	]}`
	w := post(t, h, body, testKey)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", w.Code, w.Body)
	}

	var got response
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.Accepted != 3 {
		t.Errorf("accepted = %d, want 3 (the commands, not the requests)", got.Accepted)
	}
	if n := len(pub.published()); n != 1 {
		t.Fatalf("published %d messages, want exactly 1 for the whole bulk", n)
	}

	// What reaches the broker must still be the shape it parses.
	var sent Envelope
	if err := json.Unmarshal(pub.published()[0], &sent); err != nil {
		t.Fatalf("published payload is not valid JSON: %v", err)
	}
	if len(sent.Messages) != 3 {
		t.Errorf("published bulk carries %d messages, want 3", len(sent.Messages))
	}
	if sent.Command != "" {
		t.Errorf("published bulk also carries a top-level Command %q, which the broker would ignore", sent.Command)
	}
}

// The broker's field names are case-sensitive: "Command" and "Data"
// capitalised, "messages" not. A round trip through Go structs is exactly
// where that gets quietly normalised.
func TestPublishedPayloadKeepsTheBrokersFieldNames(t *testing.T) {
	h, pub := newTestHandler()
	post(t, h, `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}`, testKey)

	sent := string(pub.published()[0])
	for _, key := range []string{`"Command"`, `"Data"`} {
		if !strings.Contains(sent, key) {
			t.Errorf("published payload %s is missing %s", sent, key)
		}
	}

	h2, pub2 := newTestHandler()
	post(t, h2, `{"messages":[{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}]}`, testKey)
	if sent := string(pub2.published()[0]); !strings.Contains(sent, `"messages"`) {
		t.Errorf(`published bulk %s is missing the lower-case "messages" key`, sent)
	}
}

func TestHandlerRejectsBadRequests(t *testing.T) {
	cases := map[string]struct {
		body string
		key  string
		want int
	}{
		"no key":           {`{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}`, "", http.StatusUnauthorized},
		"wrong key":        {`{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}`, "nope", http.StatusUnauthorized},
		"broken JSON":      {`{"Command":`, testKey, http.StatusBadRequest},
		"unknown command":  {`{"Command":"nope","Data":"x"}`, testKey, http.StatusBadRequest},
		"missing Data":     {`{"Command":"raw"}`, testKey, http.StatusBadRequest},
		"denied command":   {`{"Command":"raw","Data":"SHUTDOWN_PROGRAM"}`, testKey, http.StatusForbidden},
		"denied alias":     {`{"Command":"raw","Data":"SHUTDOWN_PROCESS"}`, testKey, http.StatusForbidden},
		"denied bypass":    {`{"Command":"raw","Data":"PROCESS_FILE;/tmp/x;0"}`, testKey, http.StatusForbidden},
		"newline injected": {`{"Command":"raw","Data":"ADD_HOST_COMMENT;h;1;a;c\nSHUTDOWN_PROGRAM"}`, testKey, http.StatusBadRequest},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h, pub := newTestHandler()
			w := post(t, h, tc.body, tc.key)
			if w.Code != tc.want {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, tc.want, w.Body)
			}
			if len(pub.published()) != 0 {
				t.Error("a rejected request must not reach the broker")
			}
		})
	}
}

func TestHandlerRejectsNonPost(t *testing.T) {
	h, _ := newTestHandler()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		r := httptest.NewRequest(method, Path, nil)
		r.Header.Set("X-Api-Key", testKey)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: status = %d, want 405", method, w.Code)
		}
		if allow := w.Header().Get("Allow"); allow != http.MethodPost {
			t.Errorf("%s: Allow = %q, want POST", method, allow)
		}
	}
}

func TestHandlerRejectsOversizedBody(t *testing.T) {
	h, pub := newTestHandler()
	// A single raw command longer than the body limit.
	huge := `{"Command":"raw","Data":"` + strings.Repeat("A", MaxBodyBytes+1) + `"}`
	w := post(t, h, huge, testKey)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413 (body: %s)", w.Code, w.Body)
	}
	if len(pub.published()) != 0 {
		t.Error("an oversized request must not reach the broker")
	}
}

// A broker that cannot be reached is not the caller's mistake, and the same
// request will work once it is back - which is what 503 tells a client that
// retries, and 500 does not.
func TestHandlerAnswers503WhenPublishingFails(t *testing.T) {
	pub := &fakePublisher{err: errors.New("broker is down")}
	h := NewHandler(pub, keySet(testKey))
	w := post(t, h, `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}`, testKey)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body: %s)", w.Code, w.Body)
	}
	// The broker's error text can name hosts and credentials; the caller
	// gets the fact, not the detail.
	if strings.Contains(w.Body.String(), "broker is down") {
		t.Error("the underlying broker error must not be echoed to the caller")
	}
}

func TestErrorResponsesAreJSON(t *testing.T) {
	h, _ := newTestHandler()
	w := post(t, h, `{"Command":"nope","Data":"x"}`, testKey)
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got response
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("error response is not JSON: %v", err)
	}
	if got.Error == "" {
		t.Error("error response carries no message")
	}
}

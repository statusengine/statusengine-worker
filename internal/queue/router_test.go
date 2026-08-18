package queue

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"statusengine-worker/internal/types"
	"statusengine-worker/internal/websocket"
)

// fakeEnqueuer stands in for a *db.BulkInserter[P] in tests that don't need
// a real database: it just records what was enqueued.
type fakeEnqueuer[P any] struct {
	mu    sync.Mutex
	items []P
	err   error
}

func (f *fakeEnqueuer[P]) Enqueue(_ context.Context, item P) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.items = append(f.items, item)
	return nil
}

func (f *fakeEnqueuer[P]) snapshot() []P {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]P, len(f.items))
	copy(out, f.items)
	return out
}

// dialTopic spins up a real Hub-backed websocket endpoint via httptest,
// subscribes a real client connection to topic and returns it alongside a
// cleanup func. Used to prove Handlers really reach WebSocket subscribers,
// not just a mocked Publish call.
func dialTopic(t *testing.T, hub *websocket.Hub, topic string) *gorillaws.Conn {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWS(hub, w, r, nil)
	}))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?topics=" + topic
	conn, _, err := gorillaws.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", wsURL, err)
	}
	t.Cleanup(func() { conn.Close() })

	// Give the Hub's Run loop time to process the registration before the
	// test publishes anything.
	time.Sleep(50 * time.Millisecond)
	return conn
}

func readTopicMessage(t *testing.T, conn *gorillaws.Conn) (topic string, payload json.RawMessage) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket message: %v", err)
	}
	var out struct {
		Topic   string          `json:"topic"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal outbound message: %v", err)
	}
	return out.Topic, out.Payload
}

// decodeBatch unmarshals a frame's payload, which is always a JSON array
// of events - one frame per queue job, never one per event. See
// publishBatch.
func decodeBatch[T any](t *testing.T, payload json.RawMessage) []T {
	t.Helper()
	var batch []T
	if err := json.Unmarshal(payload, &batch); err != nil {
		t.Fatalf("unmarshal payload batch: %v (payload: %s)", err, payload)
	}
	return batch
}

// expectNoFurtherMessage fails if another frame turns up. Together with a
// length assertion on the first frame it is what distinguishes "one frame
// holding N events" from "N frames holding one event each" - the latter
// would satisfy every other assertion in these tests, since a one-element
// array decodes into a one-element slice perfectly happily.
func expectNoFurtherMessage(t *testing.T, conn *gorillaws.Conn) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, raw, err := conn.ReadMessage(); err == nil {
		t.Fatalf("expected exactly one frame for the whole job, got a second: %s", raw)
	}
}

func TestNewHandlerPersistsAndBroadcasts(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	conn := dialTopic(t, hub, QueueHostChecks)

	fake := &fakeEnqueuer[hostCheckEvent]{}
	handler := NewHandler(hub, QueueHostChecks, fake, decodeHostCheck)

	if err := handler(ctx, readFixture(t, "statusngin_hostchecks.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	items := fake.snapshot()
	if len(items) != 1 || items[0].HostName != "localhost" {
		t.Fatalf("db side: unexpected enqueued items: %+v", items)
	}

	topic, payload := readTopicMessage(t, conn)
	if topic != QueueHostChecks {
		t.Fatalf("ws side: topic = %q, want %q", topic, QueueHostChecks)
	}
	got := decodeBatch[types.HostCheckPayload](t, payload)
	if len(got) != 1 || got[0].HostName != "localhost" {
		t.Fatalf("ws side: unexpected payload batch: %+v", got)
	}
}

// TestOneFrameCarriesTheWholeJob is the test the batching change exists
// for. Every other websocket assertion in this package is satisfied just
// as well by a handler that sends one frame per event, because a
// single-element array is indistinguishable from a batch of one - so this
// feeds a job holding three events and insists on seeing one frame with
// three elements and nothing after it.
func TestOneFrameCarriesTheWholeJob(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	conn := dialTopic(t, hub, QueueHostChecks)

	// The fixture holds one message; repeat it to get a bulk job, which is
	// what the broker actually delivers (CLAUDE.md rule 1).
	var job struct {
		Messages []json.RawMessage `json:"messages"`
		Format   json.RawMessage   `json:"format,omitempty"`
	}
	if err := json.Unmarshal(readFixture(t, "statusngin_hostchecks.json"), &job); err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	job.Messages = []json.RawMessage{job.Messages[0], job.Messages[0], job.Messages[0]}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}

	fake := &fakeEnqueuer[hostCheckEvent]{}
	handler := NewHandler(hub, QueueHostChecks, fake, decodeHostCheck)
	if err := handler(ctx, payload); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if items := fake.snapshot(); len(items) != 3 {
		t.Fatalf("db side: enqueued %d items, want 3", len(items))
	}

	topic, framePayload := readTopicMessage(t, conn)
	if topic != QueueHostChecks {
		t.Fatalf("topic = %q, want %q", topic, QueueHostChecks)
	}
	if got := decodeBatch[types.HostCheckPayload](t, framePayload); len(got) != 3 {
		t.Fatalf("frame carried %d events, want all 3 of the job's", len(got))
	}
	expectNoFurtherMessage(t, conn)
}

func TestNewHandlerPropagatesEnqueueError(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	fake := &fakeEnqueuer[hostCheckEvent]{err: context.DeadlineExceeded}
	handler := NewHandler(hub, QueueHostChecks, fake, decodeHostCheck)

	if err := handler(ctx, readFixture(t, "statusngin_hostchecks.json")); err == nil {
		t.Fatal("expected handler to propagate the enqueue error")
	}
}

func TestNewHandlerPropagatesDecodeError(t *testing.T) {
	hub := websocket.NewHub()
	fake := &fakeEnqueuer[hostCheckEvent]{}
	handler := NewHandler(hub, QueueHostChecks, fake, decodeHostCheck)

	if err := handler(context.Background(), []byte("not json")); err == nil {
		t.Fatal("expected handler to propagate the decode error")
	}
}

func TestNewBroadcastHandlerSkipsDB(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	conn := dialTopic(t, hub, QueueHostStatus)

	handler := NewBroadcastHandler(hub, QueueHostStatus, decodeHostStatus)
	if err := handler(ctx, readFixture(t, "statusngin_hoststatus.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	topic, payload := readTopicMessage(t, conn)
	if topic != QueueHostStatus {
		t.Fatalf("topic = %q, want %q", topic, QueueHostStatus)
	}
	// The fixture is a bulk job of two hosts, and both arrive in this one
	// frame - the batch is the job, not one event out of it.
	got := decodeBatch[types.HostStatusPayload](t, payload)
	if len(got) != 2 || got[0].Name != "demo.statusengine.org" {
		t.Fatalf("unexpected payload batch: %+v", got)
	}
	expectNoFurtherMessage(t, conn)
}

func TestStateChangeHandlerRoutesHostVsService(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[stateChangeEvent]{}
	serviceIns := &fakeEnqueuer[stateChangeEvent]{}
	handler := newStateChangeHandler(hub, QueueStateChanges, hostIns, serviceIns)

	if err := handler(ctx, readFixture(t, "statusngin_statechanges.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Both fixture entries carry statechange_type 1, so both should land in
	// serviceIns and none in hostIns.
	if got := len(hostIns.snapshot()); got != 0 {
		t.Fatalf("hostIns got %d items, want 0", got)
	}
	if got := len(serviceIns.snapshot()); got != 2 {
		t.Fatalf("serviceIns got %d items, want 2", got)
	}
}

func TestStateChangeHandlerRoutesHostOnlyEvent(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[stateChangeEvent]{}
	serviceIns := &fakeEnqueuer[stateChangeEvent]{}
	handler := newStateChangeHandler(hub, QueueStateChanges, hostIns, serviceIns)

	bulk := types.StateChangeBulk{
		Messages: []types.StateChangeMessage{
			{
				Envelope:    types.Envelope{Timestamp: 1234},
				StateChange: types.StateChangePayload{HostName: "localhost", State: 0},
			},
		},
	}
	payload, err := json.Marshal(bulk)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := handler(ctx, payload); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(hostIns.snapshot()); got != 1 {
		t.Fatalf("hostIns got %d items, want 1", got)
	}
	if got := len(serviceIns.snapshot()); got != 0 {
		t.Fatalf("serviceIns got %d items, want 0", got)
	}
}

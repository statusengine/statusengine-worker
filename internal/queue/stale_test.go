package queue

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/websocket"
)

// hostStatusPayloadAt builds a statusngin_hoststatus bulk message whose
// envelope timestamps are the given ages relative to now - one message per
// age, so a single payload can mix fresh and stale items the way a real
// bulk job does at the edge of a backlog.
func hostStatusPayloadAt(t *testing.T, ages ...time.Duration) []byte {
	t.Helper()

	messages := make([]string, 0, len(ages))
	for i, age := range ages {
		ts := time.Now().Add(-age).Unix()
		messages = append(messages, fmt.Sprintf(`{
			"type": 1201, "flags": 0, "attr": 0,
			"timestamp": %d, "timestamp_usec": 0,
			"hoststatus": {"name": "host-%d", "plugin_output": "OK", "current_state": 0}
		}`, ts, i))
	}
	return []byte(`{"messages": [` + strings.Join(messages, ",") + `]}`)
}

// staleDiscardsFor reads this queue's discard counter out of the default
// registry, reporting 0 for a series that does not exist yet. Absence is
// not a failure here: the series is pre-created by NewRouter, and the
// tests that build a handler directly never go through it. That
// pre-creation is asserted separately, in TestOnlyStatusQueuesDiscardOnAge.
func staleDiscardsFor(t *testing.T, queueName string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "statusengine_queue_events_discarded_stale_total" {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, label := range series.GetLabel() {
				if label.GetName() == "queue_name" && label.GetValue() == queueName {
					return series.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// hasDiscardSeries reports whether the discard counter carries a series
// for queueName at all, which is what InitStaleDiscards is for.
func hasDiscardSeries(t *testing.T, queueName string) bool {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "statusengine_queue_events_discarded_stale_total" {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, label := range series.GetLabel() {
				if label.GetName() == "queue_name" && label.GetValue() == queueName {
					return true
				}
			}
		}
	}
	return false
}

// TestStaleStatusEventsReachNeitherDestination is the whole point: a
// superseded snapshot must cost nothing - no row, no broadcast.
func TestStaleStatusEventsReachNeitherDestination(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	conn := dialTopic(t, hub, QueueHostStatus)

	fake := &fakeEnqueuer[hostStatusEvent]{}
	handler := NewStaleDroppingHandler(hub, QueueHostStatus, fake, decodeHostStatus, 5*time.Minute)

	before := staleDiscardsFor(t, QueueHostStatus)

	// Ten minutes old: twice the limit, no ambiguity about clock jitter.
	if err := handler(ctx, hostStatusPayloadAt(t, 10*time.Minute)); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if items := fake.snapshot(); len(items) != 0 {
		t.Fatalf("db side: stale event was enqueued anyway: %+v", items)
	}
	if got := staleDiscardsFor(t, QueueHostStatus) - before; got != 1 {
		t.Fatalf("discard counter rose by %v, want 1", got)
	}

	// The websocket side needs a positive signal that nothing arrived, so
	// publish a fresh event afterwards: if the stale one had been
	// broadcast it would be sitting in front of this one in the same
	// connection's stream, and the assertion below would see host-0 with
	// the stale timestamp instead.
	if err := handler(ctx, hostStatusPayloadAt(t, time.Second)); err != nil {
		t.Fatalf("handler (fresh): %v", err)
	}
	_, payload := readTopicMessage(t, conn)
	batch := decodeBatch[struct {
		Timestamp int64  `json:"timestamp"`
		Name      string `json:"name"`
	}](t, payload)
	if len(batch) != 1 {
		t.Fatalf("ws side: first frame carried %d events, want only the fresh one", len(batch))
	}
	if age := time.Since(time.Unix(batch[0].Timestamp, 0)); age > time.Minute {
		t.Fatalf("ws side: first message received was %s old - the stale event was broadcast", age)
	}
}

// TestFreshStatusEventsAreUnaffected is the counter-proof: with the filter
// active, a current event must still take both paths exactly as before.
func TestFreshStatusEventsAreUnaffected(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	conn := dialTopic(t, hub, QueueHostStatus)

	fake := &fakeEnqueuer[hostStatusEvent]{}
	handler := NewStaleDroppingHandler(hub, QueueHostStatus, fake, decodeHostStatus, 5*time.Minute)

	before := staleDiscardsFor(t, QueueHostStatus)

	if err := handler(ctx, hostStatusPayloadAt(t, 30*time.Second)); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if items := fake.snapshot(); len(items) != 1 {
		t.Fatalf("db side: got %d items, want the fresh event", len(items))
	}
	if got := staleDiscardsFor(t, QueueHostStatus) - before; got != 0 {
		t.Fatalf("discard counter rose by %v for a fresh event, want 0", got)
	}
	if topic, _ := readTopicMessage(t, conn); topic != QueueHostStatus {
		t.Fatalf("ws side: topic = %q", topic)
	}
}

// TestBulkPayloadIsFilteredPerItem covers the boundary between a backlog
// and live traffic, where one job legitimately carries both.
func TestBulkPayloadIsFilteredPerItem(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	fake := &fakeEnqueuer[hostStatusEvent]{}
	handler := NewStaleDroppingHandler(hub, QueueHostStatus, fake, decodeHostStatus, 5*time.Minute)

	before := staleDiscardsFor(t, QueueHostStatus)

	// host-0 and host-2 are stale, host-1 and host-3 are not.
	payload := hostStatusPayloadAt(t, 10*time.Minute, time.Second, 6*time.Minute, 2*time.Second)
	if err := handler(ctx, payload); err != nil {
		t.Fatalf("handler: %v", err)
	}

	items := fake.snapshot()
	if len(items) != 2 {
		t.Fatalf("got %d items, want the 2 fresh ones: %+v", len(items), items)
	}
	for _, item := range items {
		if item.Name == "host-0" || item.Name == "host-2" {
			t.Fatalf("stale item %q survived the filter", item.Name)
		}
	}
	if got := staleDiscardsFor(t, QueueHostStatus) - before; got != 2 {
		t.Fatalf("discard counter rose by %v, want 2", got)
	}
}

// TestZeroMaxAgeDisablesTheFilter is the escape hatch: an operator who
// suspects clock skew must be able to turn this off and get every event
// back, including ones from years ago.
func TestZeroMaxAgeDisablesTheFilter(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	fake := &fakeEnqueuer[hostStatusEvent]{}
	handler := NewStaleDroppingHandler(hub, QueueHostStatus, fake, decodeHostStatus, 0)

	before := staleDiscardsFor(t, QueueHostStatus)

	if err := handler(ctx, hostStatusPayloadAt(t, 30*24*time.Hour)); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if items := fake.snapshot(); len(items) != 1 {
		t.Fatalf("got %d items, want the month-old event processed anyway", len(items))
	}
	if got := staleDiscardsFor(t, QueueHostStatus) - before; got != 0 {
		t.Fatalf("discard counter rose by %v with the filter disabled, want 0", got)
	}
}

// TestFutureTimestampsAreNeverDiscarded pins down the asymmetry. A core
// whose clock runs ahead produces events "from the future"; those are the
// newest state there is and must not be dropped. Only the past is stale.
func TestFutureTimestampsAreNeverDiscarded(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	fake := &fakeEnqueuer[hostStatusEvent]{}
	handler := NewStaleDroppingHandler(hub, QueueHostStatus, fake, decodeHostStatus, 5*time.Minute)

	if err := handler(ctx, hostStatusPayloadAt(t, -time.Hour)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if items := fake.snapshot(); len(items) != 1 {
		t.Fatalf("got %d items, want the future-dated event processed", len(items))
	}
}

// TestOnlyStatusQueuesDiscardOnAge guards the blast radius, through the
// real NewRouter rather than a hand-built handler. Every non-status queue
// carries history - a check result, a state change, a notification - where
// an old event is still the only record of something that happened, so
// applying the filter there would be data loss rather than a saving.
//
// The fixtures make this test cheap: they all carry fixed timestamps from
// 2026-07, so relative to now every one of them is "stale". With the
// filter set to five minutes, the hoststatus fixture must vanish and the
// hostchecks fixture must not.
func TestOnlyStatusQueuesDiscardOnAge(t *testing.T) {
	sqlDB := openTestDB(t)
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	router, _ := NewRouter(sqlDB, hub, graphite.NewClient("127.0.0.1:2003"), PerfdataRouteMySQL,
		"statusengine-test", "statusengine-test", false, 5*time.Minute, testBatchSize)

	// NewRouter must have pre-created both series at zero, so a dashboard
	// panel reads 0 rather than "No data" on a worker that has not
	// discarded anything yet.
	for _, name := range []string{QueueHostStatus, QueueServiceStatus} {
		if !hasDiscardSeries(t, name) {
			t.Fatalf("NewRouter did not pre-create the discard series for %q", name)
		}
	}

	beforeStatus := staleDiscardsFor(t, QueueHostStatus)

	if err := router[QueueHostStatus](ctx, readFixture(t, "statusngin_hoststatus.json")); err != nil {
		t.Fatalf("hoststatus handler: %v", err)
	}
	if got := staleDiscardsFor(t, QueueHostStatus) - beforeStatus; got == 0 {
		t.Fatal("the 2026-07 hoststatus fixture was not discarded - the filter is not wired up in NewRouter")
	}

	// The negative half. statusngin_hostchecks is just as old, and must be
	// processed regardless; a discard series for it must not even exist,
	// since NewRouter only pre-creates the two status queues.
	if err := router[QueueHostChecks](ctx, readFixture(t, "statusngin_hostchecks.json")); err != nil {
		t.Fatalf("hostchecks handler: %v", err)
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "statusengine_queue_events_discarded_stale_total" {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, label := range series.GetLabel() {
				if label.GetName() != "queue_name" {
					continue
				}
				if q := label.GetValue(); q != QueueHostStatus && q != QueueServiceStatus {
					t.Fatalf("queue %q can discard events on age, but only the status queues may", q)
				}
			}
		}
	}
}

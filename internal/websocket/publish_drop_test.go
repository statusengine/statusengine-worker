package websocket

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// counterValue reads one unlabeled counter back out of the default
// registry. It gathers rather than reaching into the metric, which is the
// only way to see what a scrape would actually report - and it mirrors
// what internal/metrics/metrics_test.go does, deliberately without
// prometheus/testutil so go.mod stays untouched for a test-only
// convenience.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		metrics := family.GetMetric()
		if len(metrics) != 1 {
			t.Fatalf("%s: expected exactly one unlabeled series, got %d", name, len(metrics))
		}
		return metrics[0].GetCounter().GetValue()
	}
	t.Fatalf("%s: no such metric family in the default registry", name)
	return 0
}

// TestPublishDropsAreCountedWhenTheHubBufferIsFull covers the one branch
// in Publish that gives up on an event entirely.
//
// Run is deliberately never started, which is what makes the buffer fill:
// nothing drains h.broadcast, so the first broadcastBufferSize events are
// buffered and every one after that hits the default arm. That models the
// real failure this metric exists for - a Run goroutine that cannot keep
// up - without needing to actually outrun it.
func TestPublishDropsAreCountedWhenTheHubBufferIsFull(t *testing.T) {
	const metricName = "statusengine_websocket_publish_dropped_total"

	// A delta, not an absolute: the registry is process-global, so any
	// other test that drops a publish would otherwise break this one.
	before := counterValue(t, metricName)

	hub := NewHub()

	const overflow = 5
	for i := 0; i < broadcastBufferSize+overflow; i++ {
		hub.Publish("statusngin_hoststatus", []byte(`{}`))
	}

	if got := hub.publishDropped.Load(); got != overflow {
		t.Fatalf("hub counted %d dropped publishes, want %d", got, overflow)
	}

	if got := counterValue(t, metricName) - before; got != overflow {
		t.Fatalf("%s advanced by %v, want %d", metricName, got, overflow)
	}

	// The events that did fit must still be there - a full buffer must
	// not cost the ones already accepted.
	if got := len(hub.broadcast); got != broadcastBufferSize {
		t.Fatalf("hub buffered %d events, want %d", got, broadcastBufferSize)
	}
}

// TestPublishBelowCapacityDropsNothing is the counter-proof for the test
// above: without it, a Publish that dropped every event would satisfy the
// overflow assertion just as well.
func TestPublishBelowCapacityDropsNothing(t *testing.T) {
	const metricName = "statusengine_websocket_publish_dropped_total"

	before := counterValue(t, metricName)

	hub := NewHub()
	for i := 0; i < broadcastBufferSize; i++ {
		hub.Publish("statusngin_hoststatus", []byte(`{}`))
	}

	if got := hub.publishDropped.Load(); got != 0 {
		t.Fatalf("hub dropped %d publishes while still under capacity, want 0", got)
	}
	if got := counterValue(t, metricName) - before; got != 0 {
		t.Fatalf("%s advanced by %v while under capacity, want 0", metricName, got)
	}
}

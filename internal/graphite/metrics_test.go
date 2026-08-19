package graphite

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// The Graphite path was the one part of the pipeline with no metrics of
// its own - only pipeline_errors_total{component="graphite"}, which counts
// failed flushes rather than lost metrics. That mattered more here than
// anywhere else, because this is the only component that *drops* data by
// design: an unreachable MySQL holds its batch and backpressures to the
// broker (CLAUDE.md rule 3), an unreachable Carbon loses it outright. The
// trade is deliberate; being unable to see what it cost was not.
//
// These read back through prometheus.DefaultGatherer rather than the
// counter objects, for the same reason as internal/metrics' own tests:
// what a dashboard sees is the exposition output, and "the object was
// incremented" is a weaker claim than "the series is scraped".

func gatherValue(t *testing.T, name string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		metric := family.GetMetric()
		if len(metric) != 1 {
			t.Fatalf("%s has %d series, want exactly 1", name, len(metric))
		}
		if c := metric[0].GetCounter(); c != nil {
			return c.GetValue()
		}
		return metric[0].GetGauge().GetValue()
	}
	t.Fatalf("%s is not exported at all", name)
	return 0
}

// TestSuccessfulFlushCountsMetricsAndFlushes pins the counter-ratio pair.
// A flush counted per metric (or metrics counted per flush) would leave
// written/flushes reporting a plausible but wrong average batch size,
// which is the only reason the pair exists - openITCOCKPIT ingests no
// histograms.
func TestSuccessfulFlushCountsMetricsAndFlushes(t *testing.T) {
	const (
		flushes         = 3
		metricsPerFlush = 4
	)

	r := newCarbonReceiver(t)
	c := NewClient(r.addr)

	beforeMetrics := gatherValue(t, "statusengine_graphite_metrics_written_total")
	beforeFlushes := gatherValue(t, "statusengine_graphite_flushes_total")

	flushN(t, c, r, flushes, metricsPerFlush)

	if got := gatherValue(t, "statusengine_graphite_metrics_written_total") - beforeMetrics; got != flushes*metricsPerFlush {
		t.Errorf("metrics_written_total rose by %v, want %v", got, flushes*metricsPerFlush)
	}
	if got := gatherValue(t, "statusengine_graphite_flushes_total") - beforeFlushes; got != flushes {
		t.Errorf("flushes_total rose by %v, want %v", got, flushes)
	}
	if got := gatherValue(t, "statusengine_graphite_available"); got != 1 {
		t.Errorf("graphite_available = %v after a successful flush, want 1", got)
	}
}

// TestFailedFlushCountsEveryLostMetric is the point of this file. The
// batch is dropped - that behaviour is deliberate and unchanged - but the
// number of metrics in it has to be visible, because nothing else can
// supply it. pipeline_errors_total counts one per failed flush, so it
// cannot distinguish losing one metric from losing a thousand.
func TestFailedFlushCountsEveryLostMetric(t *testing.T) {
	const lost = 7

	// Port 1: nothing listens, so ensureConn's dial fails and the batch is
	// dropped before a single byte is written.
	c := NewClient("127.0.0.1:1")

	before := gatherValue(t, "statusengine_graphite_metrics_dropped_total")

	for i := 0; i < lost; i++ {
		c.buffer = append(c.buffer, Metric{Path: "a.b.c", Value: float64(i), Timestamp: 1})
	}
	if err := c.flushBuffer(context.Background()); err == nil {
		t.Fatal("flushBuffer against a dead address returned no error")
	}

	if got := gatherValue(t, "statusengine_graphite_metrics_dropped_total") - before; got != lost {
		t.Errorf("metrics_dropped_total rose by %v, want %v - a dashboard cannot see how much was lost", got, lost)
	}
	if got := gatherValue(t, "statusengine_graphite_available"); got != 0 {
		t.Errorf("graphite_available = %v while metrics are being dropped, want 0", got)
	}
	if len(c.buffer) != 0 {
		t.Errorf("buffer still holds %d metrics; the batch must still be dropped, not retried", len(c.buffer))
	}
}

// TestAvailableRecoversAfterCarbonReturns: the gauge has to come back on
// its own, or it reports an outage that ended hours ago and nobody trusts
// it again.
func TestAvailableRecoversAfterCarbonReturns(t *testing.T) {
	dead := NewClient("127.0.0.1:1")
	dead.buffer = append(dead.buffer, Metric{Path: "a.b.c", Value: 1, Timestamp: 1})
	dead.flushBuffer(context.Background())
	if got := gatherValue(t, "statusengine_graphite_available"); got != 0 {
		t.Fatalf("graphite_available = %v after a failed flush, want 0", got)
	}

	r := newCarbonReceiver(t)
	alive := NewClient(r.addr)
	flushN(t, alive, r, 1, 1)

	if got := gatherValue(t, "statusengine_graphite_available"); got != 1 {
		t.Errorf("graphite_available = %v after Carbon came back, want 1", got)
	}
}

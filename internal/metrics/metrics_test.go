package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Everything below reads back through prometheus.DefaultGatherer rather
// than asking a *Vec what children it holds. That is the point of these
// tests: what a dashboard sees is the exposition output, and "the child
// object exists" is not the same claim as "the series is scraped".
//
// Note these deliberately avoid prometheus/promhttp's testutil package -
// importing it would promote client_model to a direct dependency and
// require a go.mod change for test-only convenience, when Gather() plus a
// few accessors does the same job.

// gatherFamily returns the gathered metric family called name, or nil.
func gatherFamily(t *testing.T, name string) *familyView {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}

		view := &familyView{}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}

			// Counters carry their value directly; a histogram's
			// equivalent "has anything been recorded" number is its
			// sample count.
			value := metric.GetCounter().GetValue()
			if h := metric.GetHistogram(); h != nil {
				value = float64(h.GetSampleCount())
			}

			view.series = append(view.series, seriesView{labels: labels, value: value})
		}
		return view
	}
	return nil
}

type seriesView struct {
	labels map[string]string
	value  float64
}

type familyView struct {
	series []seriesView
}

// find returns the value of the series carrying label=value.
func (f *familyView) find(label, value string) (float64, bool) {
	if f == nil {
		return 0, false
	}
	for _, s := range f.series {
		if s.labels[label] == value {
			return s.value, true
		}
	}
	return 0, false
}

func mustFindZero(t *testing.T, metricName, label, value string) {
	t.Helper()

	got, ok := gatherFamily(t, metricName).find(label, value)
	if !ok {
		t.Errorf("%s has no series for %s=%q - a dashboard would show \"No data\"", metricName, label, value)
		return
	}
	if got != 0 {
		t.Errorf("%s{%s=%q} = %v, want 0", metricName, label, value, got)
	}
}

var queueMetricNames = []string{
	"statusengine_queue_messages_received_total",
	"statusengine_queue_payloads_repaired_total",
	"statusengine_queue_handler_duration_seconds",
}

// TestComponentSeriesExistAtZero pins the package's init: all four
// component series must be exported before anything has failed, since the
// healthy state is exactly the state in which nothing ever increments
// them - and that is when someone is looking at the dashboard to confirm
// things are fine.
func TestComponentSeriesExistAtZero(t *testing.T) {
	for _, component := range Components {
		mustFindZero(t, "statusengine_pipeline_errors_total", "component", component)
	}
}

func TestInitQueueCreatesAllThreeSeries(t *testing.T) {
	const queueName = "metrics_test_init_queue"

	for _, name := range queueMetricNames {
		if _, ok := gatherFamily(t, name).find("queue_name", queueName); ok {
			t.Fatalf("%s already had a series for %q before InitQueue", name, queueName)
		}
	}

	InitQueue(queueName)

	for _, name := range queueMetricNames {
		mustFindZero(t, name, "queue_name", queueName)
	}
}

func TestInitTableCreatesSeries(t *testing.T) {
	const table = "metrics_test_init_table"

	if _, ok := gatherFamily(t, "statusengine_db_events_written_total").find("table", table); ok {
		t.Fatalf("a series for %q existed before InitTable", table)
	}

	InitTable(table)

	mustFindZero(t, "statusengine_db_events_written_total", "table", table)
}

// TestInitIsIdempotentAndNonDestructive is the property that makes it safe
// to call these from a constructor: a second call must hand back the
// existing child rather than a fresh zero one. Resetting a counter would
// make it go backwards, which corrupts every rate() spanning that window,
// not just one sample.
func TestInitIsIdempotentAndNonDestructive(t *testing.T) {
	const (
		queueName = "metrics_test_idempotent_queue"
		table     = "metrics_test_idempotent_table"
	)

	InitQueue(queueName)
	InitTable(table)

	QueueMessagesReceivedTotal.WithLabelValues(queueName).Add(7)
	QueueHandlerDurationSeconds.WithLabelValues(queueName).Observe(0.5)
	DBEventsWrittenTotal.WithLabelValues(table).Add(11)

	InitQueue(queueName)
	InitTable(table)

	if got, _ := gatherFamily(t, "statusengine_queue_messages_received_total").find("queue_name", queueName); got != 7 {
		t.Errorf("queue counter = %v after a second InitQueue, want 7", got)
	}
	if got, _ := gatherFamily(t, "statusengine_queue_handler_duration_seconds").find("queue_name", queueName); got != 1 {
		t.Errorf("histogram sample count = %v after a second InitQueue, want 1", got)
	}
	if got, _ := gatherFamily(t, "statusengine_db_events_written_total").find("table", table); got != 11 {
		t.Errorf("table counter = %v after a second InitTable, want 11", got)
	}
}

// TestUnlabeledMetricsNeedNoInit records why only five of the eleven
// metrics are handled above: an unlabeled collector is exported from the
// first scrape on its own, so there is nothing to pre-create. If one of
// these ever grows a label, this test starts failing and points at the
// Init* functions as the thing to extend.
func TestUnlabeledMetricsNeedNoInit(t *testing.T) {
	unlabeled := []string{
		"statusengine_queue_jobs_in_flight",
		"statusengine_db_batch_flush_duration_seconds",
		"statusengine_db_batch_size_at_flush",
		"statusengine_websocket_clients_active",
		"statusengine_websocket_messages_broadcasted_total",
		"statusengine_websocket_messages_dropped_total",
	}

	for _, name := range unlabeled {
		family := gatherFamily(t, name)
		if family == nil || len(family.series) == 0 {
			t.Errorf("%s is not exported at all", name)
			continue
		}
		if len(family.series) != 1 {
			t.Errorf("%s has %d series, want exactly 1 - has it gained a label?", name, len(family.series))
			continue
		}
		if len(family.series[0].labels) != 0 {
			t.Errorf("%s carries labels %v - it now needs an Init* entry", name, family.series[0].labels)
		}
	}
}

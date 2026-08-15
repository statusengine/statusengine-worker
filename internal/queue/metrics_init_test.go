package queue

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/websocket"
)

// gatheredLabelValues returns the values of label across every series
// currently exported under the metric family name, read back through the
// gatherer - the same output a Prometheus scrape would see.
func gatheredLabelValues(t *testing.T, name, label string) map[string]bool {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	values := map[string]bool{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == label {
					values[pair.GetValue()] = true
				}
			}
		}
	}
	return values
}

// TestDowntimeMetricsTablesCoverEveryAction ties the enumeration used for
// pre-creating the series to the function that produces the label at
// write time. Without this, a third target table (or a renamed one) would
// still get its rows written and counted, but only ever appear in
// /metrics after the first downtime of that kind - which is exactly the
// gap the pre-creation exists to close.
func TestDowntimeMetricsTablesCoverEveryAction(t *testing.T) {
	enumerated := map[string]bool{}
	for _, table := range downtimeMetricsTables() {
		if enumerated[table] {
			t.Errorf("downtimeMetricsTables lists %q twice", table)
		}
		enumerated[table] = true
	}

	produced := map[string]bool{}
	for _, target := range []DowntimeTargetTable{ScheduledDowntimesTable, DowntimeHistoryTable} {
		for _, isHost := range []bool{true, false} {
			action := DowntimeAction{
				Table: target,
				Data:  DowntimeRowData{IsHostDowntime: isHost},
			}

			got := downtimeMetricsTable(action)
			produced[got] = true

			if !enumerated[got] {
				t.Errorf("downtimeMetricsTable produces %q, which downtimeMetricsTables does not list", got)
			}
		}
	}

	for table := range enumerated {
		if !produced[table] {
			t.Errorf("downtimeMetricsTables lists %q, which no action can produce", table)
		}
	}

	if len(enumerated) != 4 {
		t.Errorf("got %d downtime tables, want 4 (host/service x scheduleddowntimes/downtimehistory)", len(enumerated))
	}
}

// TestNewRouterPreCreatesMetricSeries is the end-to-end version: after
// wiring the router, a worker that has not yet seen a single message must
// already export a zero for every queue it listens on and every table it
// can write to.
func TestNewRouterPreCreatesMetricSeries(t *testing.T) {
	sqlDB := openTestDB(t)
	hub := websocket.NewHub()

	router, _ := NewRouter(sqlDB, hub, graphite.NewClient("127.0.0.1:2003"),
		PerfdataRouteMySQL, "statusengine-test", "statusengine-test", false, noAgeFilter)

	// Every queue in the router, on all three per-queue metrics.
	for _, name := range []string{
		"statusengine_queue_messages_received_total",
		"statusengine_queue_payloads_repaired_total",
		"statusengine_queue_handler_duration_seconds",
	} {
		exported := gatheredLabelValues(t, name, "queue_name")
		for queueName := range router {
			if !exported[queueName] {
				t.Errorf("%s has no series for queue_name=%q", name, queueName)
			}
		}
	}

	// Every table, including the four downtime ones that bypass
	// BulkInserter and therefore miss its InitTable call.
	tables := gatheredLabelValues(t, "statusengine_db_events_written_total", "table")
	want := []string{
		"statusengine_hoststatus", "statusengine_servicestatus",
		"statusengine_hostchecks", "statusengine_servicechecks",
		"statusengine_logentries", "statusengine_perfdata",
		"statusengine_host_statehistory", "statusengine_service_statehistory",
		"statusengine_host_acknowledgements", "statusengine_service_acknowledgements",
		"statusengine_host_notifications", "statusengine_service_notifications",
		"statusengine_host_notifications_log", "statusengine_service_notifications_log",
		"statusengine_host_scheduleddowntimes", "statusengine_service_scheduleddowntimes",
		"statusengine_host_downtimehistory", "statusengine_service_downtimehistory",
	}
	for _, table := range want {
		if !tables[table] {
			t.Errorf("statusengine_db_events_written_total has no series for table=%q", table)
		}
	}
}

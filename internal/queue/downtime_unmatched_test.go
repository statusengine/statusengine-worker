package queue

import (
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"

	"statusengine-worker/internal/db"
	"statusengine-worker/internal/types"
)

// gatheredCounter reads one counter series back through the gatherer,
// returning 0 when the series does not exist. Read this way rather than
// from the collector so the test sees what a scrape would.
func gatheredCounter(t *testing.T, name, label, value string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == label && pair.GetValue() == value {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// startMessage is a STARTed host downtime whose UPDATE targets
// statusengine_host_downtimehistory - the exact statement that went missing
// for six downtimes across one job-server outage.
func startMessage() types.DowntimeMessage {
	return types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeStart, Timestamp: 2000},
		Downtime: hostDowntimePayload,
	}
}

// expectExistsCheck queues the follow-up SELECT that disambiguates a
// zero-row UPDATE, answering with found or not-found.
func expectExistsCheck(mock sqlmock.Sqlmock, action DowntimeAction, found bool) {
	query, args := db.DowntimeHistoryExistsQuery(toDBRow(action.Data))
	expectation := mock.ExpectQuery("^" + regexp.QuoteMeta(query) + "$").WithArgs(toDriverArgs(args)...)

	rows := sqlmock.NewRows([]string{"1"})
	if found {
		rows.AddRow(1)
	}
	expectation.WillReturnRows(rows)
}

// TestDowntimeUpdateWithoutItsRowIsReported is the visibility half of the
// downtime ordering fix.
//
// START and STOP are bare UPDATE ... WHERE <PK> against the row the
// downtime's ADD created. When that row is absent the statement succeeds,
// affects nothing, and the event is gone - was_started stays 0 on a
// downtime that demonstrably ran. That is how six downtimes were corrupted
// without a single log line, found only weeks later by diffing against the
// legacy PHP worker. Serializing the queue removes the cause; this counter
// is what would make a recurrence visible on its own.
func TestDowntimeUpdateWithoutItsRowIsReported(t *testing.T) {
	const table = "statusengine_host_downtimehistory"

	handler, mock, _, ctx := setupDowntimeHandler(t)
	msg := startMessage()

	before := gatheredCounter(t, "statusengine_queue_downtime_updates_unmatched_total", "table", table)

	for _, a := range DetermineDowntimeActions(msg, testDowntimeNodeName) {
		query, args := buildDowntimeQuery(a)
		// Zero rows affected: nothing was there to update.
		mock.ExpectExec("^" + regexp.QuoteMeta(query) + "$").
			WithArgs(toDriverArgs(args)...).
			WillReturnResult(sqlmock.NewResult(0, 0))

		if a.Table == DowntimeHistoryTable && a.Action == DowntimeActionUpdateStarted {
			expectExistsCheck(mock, a, false)
		}
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}

	if got := gatheredCounter(t, "statusengine_queue_downtime_updates_unmatched_total", "table", table); got != before+1 {
		t.Errorf("unmatched-update counter = %v, want %v - a START whose row does not exist is a lost "+
			"event and has to be countable", got, before+1)
	}

}

// TestRedeliveredDowntimeUpdateIsNotReported is why that counter costs a
// second query instead of trusting RowsAffected.
//
// MySQL counts rows *changed*, not matched - go-sql-driver leaves
// CLIENT_FOUND_ROWS off - so a redelivered START rewriting the values it
// already wrote reports zero affected rows too. Redelivery is normal here
// (CLAUDE.md rule 6), so treating that as a lost event would put a steady
// trickle of false findings into the one series that is supposed to mean
// "data went missing".
func TestRedeliveredDowntimeUpdateIsNotReported(t *testing.T) {
	const table = "statusengine_host_downtimehistory"

	handler, mock, _, ctx := setupDowntimeHandler(t)
	msg := startMessage()

	before := gatheredCounter(t, "statusengine_queue_downtime_updates_unmatched_total", "table", table)

	for _, a := range DetermineDowntimeActions(msg, testDowntimeNodeName) {
		query, args := buildDowntimeQuery(a)
		mock.ExpectExec("^" + regexp.QuoteMeta(query) + "$").
			WithArgs(toDriverArgs(args)...).
			WillReturnResult(sqlmock.NewResult(0, 0))

		if a.Table == DowntimeHistoryTable && a.Action == DowntimeActionUpdateStarted {
			// The row is there, it simply already held these values.
			expectExistsCheck(mock, a, true)
		}
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}

	if got := gatheredCounter(t, "statusengine_queue_downtime_updates_unmatched_total", "table", table); got != before {
		t.Errorf("unmatched-update counter = %v, want it unchanged at %v - the row existed, so this was "+
			"a redelivery rewriting identical values, not a lost event", got, before)
	}
}

// TestDeleteMatchingNothingIsNotReported pins the exclusion that keeps the
// counter meaningful. Every ordinary downtime ends with STOP removing the
// scheduleddowntimes row and DELETE then finding it already gone, so a
// zero-row DELETE is the normal case, not a finding - and counting it would
// bury the real signal under one increment per downtime.
func TestDeleteMatchingNothingIsNotReported(t *testing.T) {
	handler, mock, _, ctx := setupDowntimeHandler(t)

	// A DELETE for a downtime that had already started: scheduleddowntimes
	// only, no downtimehistory delete (the "never started" case needs
	// start_time > timestamp).
	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeDelete, Timestamp: 3000},
		Downtime: hostDowntimePayload,
	}

	actions := DetermineDowntimeActions(msg, testDowntimeNodeName)
	if len(actions) == 0 {
		t.Fatal("test setup: a DELETE produced no actions")
	}

	var before float64
	for _, table := range downtimeHistoryTables() {
		before += gatheredCounter(t, "statusengine_queue_downtime_updates_unmatched_total", "table", table)
	}

	for _, a := range actions {
		if a.Action != DowntimeActionDelete {
			t.Fatalf("test setup: expected only DELETEs from this message, got %s on %s", a.Action, a.Table)
		}
		query, args := buildDowntimeQuery(a)
		mock.ExpectExec("^" + regexp.QuoteMeta(query) + "$").
			WithArgs(toDriverArgs(args)...).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	// No follow-up SELECT may have been issued: ExpectationsWereMet is what
	// proves the DELETE path asked the database nothing extra.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}

	var after float64
	for _, table := range downtimeHistoryTables() {
		after += gatheredCounter(t, "statusengine_queue_downtime_updates_unmatched_total", "table", table)
	}
	if after != before {
		t.Errorf("unmatched-update counter moved from %v to %v on a DELETE - a DELETE that matches "+
			"nothing happens on every ordinary downtime and is not a finding", before, after)
	}
}

// TestDowntimeHistoryTablesAreTheUpdatableSubset ties the pre-created
// series to the tables a downtime UPDATE can actually target. Only
// downtimehistory is ever updated; the scheduleddowntimes pair is upserted
// or deleted, so a series for those two would advertise a failure mode they
// cannot have.
func TestDowntimeHistoryTablesAreTheUpdatableSubset(t *testing.T) {
	all := map[string]bool{}
	for _, table := range downtimeMetricsTables() {
		all[table] = true
	}

	history := downtimeHistoryTables()
	if len(history) != 2 {
		t.Fatalf("got %d downtimehistory tables, want 2 (host and service)", len(history))
	}
	for _, table := range history {
		if !all[table] {
			t.Errorf("%s is not among the downtime tables - the two lists have drifted apart", table)
		}
	}

	// Derived from the decision engine rather than asserted by name: every
	// UPDATE it can produce must land on one of those two tables.
	updatable := map[string]bool{}
	for _, msg := range []types.DowntimeMessage{
		{Envelope: types.Envelope{Type: types.EventTypeDowntimeStart, Timestamp: 2000}, Downtime: hostDowntimePayload},
		{Envelope: types.Envelope{Type: types.EventTypeDowntimeStop, Timestamp: 2600}, Downtime: hostDowntimePayload},
		{Envelope: types.Envelope{Type: types.EventTypeDowntimeStart, Timestamp: 2000}, Downtime: serviceDowntimePayload},
		{Envelope: types.Envelope{Type: types.EventTypeDowntimeStop, Timestamp: 2600}, Downtime: serviceDowntimePayload},
	} {
		for _, action := range DetermineDowntimeActions(msg, testDowntimeNodeName) {
			if action.Action == DowntimeActionUpdateStarted || action.Action == DowntimeActionUpdateStopped {
				updatable[downtimeMetricsTable(action)] = true
			}
		}
	}

	if len(updatable) != len(history) {
		t.Errorf("the decision engine produces UPDATEs on %d tables, but %d are pre-created: %v vs %v",
			len(updatable), len(history), updatable, history)
	}
	for _, table := range history {
		if !updatable[table] {
			t.Errorf("%s has a pre-created unmatched-update series but no UPDATE ever targets it", table)
		}
	}
}

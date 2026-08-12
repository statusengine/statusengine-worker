package cleanup

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// fixedNow pins the clock so a cutoff can be asserted exactly.
var fixedNow = time.Date(2026, 8, 12, 14, 30, 0, 0, time.UTC)

func nowFunc() time.Time { return fixedNow }

// TestDaysZeroIssuesNoStatement is the most important test in this file.
// Zero means "never clean this table", and the failure mode if that is
// mishandled is not a crash but silent data loss at whatever the default
// retention happens to be - so it is pinned explicitly, with the opposite
// case right next to it as a counter-proof that the setup would otherwise
// have deleted something.
func TestDaysZeroIssuesNoStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// No ExpectExec at all: any statement reaching the driver fails the
	// test, because sqlmock rejects calls it was not told to expect.
	tables := []Table{{Name: "statusengine_hostchecks", Column: "start_time", Days: 0}}

	if err := Run(context.Background(), db, tables, Options{BatchSize: 100, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDaysOneIssuesAStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("DELETE FROM `statusengine_hostchecks`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tables := []Table{{Name: "statusengine_hostchecks", Column: "start_time", Days: 1}}

	if err := Run(context.Background(), db, tables, Options{BatchSize: 100, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestNegativeDaysIsTreatedAsDisabled guards the direction that would be
// dangerous if it were wrong: a negative age must never turn into a
// cutoff in the future, which would delete the entire table.
func TestNegativeDaysIsTreatedAsDisabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	tables := []Table{{Name: "statusengine_hostchecks", Column: "start_time", Days: -5}}

	if err := Run(context.Background(), db, tables, Options{BatchSize: 100, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestCutoffAndQuery pins both halves of the generated statement: the SQL
// text (table, column, LIMIT) and the bound cutoff argument.
func TestCutoffAndQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	wantCutoff := fixedNow.AddDate(0, 0, -60).Unix()

	mock.ExpectExec(regexpQuote("DELETE FROM `statusengine_host_notifications` WHERE `start_time` < ? LIMIT 250")).
		WithArgs(wantCutoff).
		WillReturnResult(sqlmock.NewResult(0, 3))

	tables := []Table{{Name: "statusengine_host_notifications", Column: "start_time", Days: 60}}

	if err := Run(context.Background(), db, tables, Options{BatchSize: 250, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestBatchLoopStopsOnShortBatch pins the loop's termination condition:
// keep going while a batch comes back full, stop as soon as one comes
// back short. Getting this wrong in the "keep going" direction is an
// infinite loop against a live database.
func TestBatchLoopStopsOnShortBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	const batch = 100

	// Full, full, short - so exactly three statements and no fourth.
	mock.ExpectExec("DELETE FROM `statusengine_servicechecks`").WillReturnResult(sqlmock.NewResult(0, batch))
	mock.ExpectExec("DELETE FROM `statusengine_servicechecks`").WillReturnResult(sqlmock.NewResult(0, batch))
	mock.ExpectExec("DELETE FROM `statusengine_servicechecks`").WillReturnResult(sqlmock.NewResult(0, batch-1))

	tables := []Table{{Name: "statusengine_servicechecks", Column: "start_time", Days: 5}}

	if err := Run(context.Background(), db, tables, Options{BatchSize: batch, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestZeroRowsStopsImmediately covers the common case of an already-clean
// table: one statement, no rows, done.
func TestZeroRowsStopsImmediately(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("DELETE FROM `statusengine_logentries`").WillReturnResult(sqlmock.NewResult(0, 0))

	tables := []Table{{Name: "statusengine_logentries", Column: "entry_time", Days: 5}}

	if err := Run(context.Background(), db, tables, Options{BatchSize: 100, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestFailingTableDoesNotStopTheRest is the reason Run collects errors
// instead of returning on the first one: statusengine_perfdata does not
// exist on Graphite-only installations, and that must not cost the other
// thirteen tables their cleanup.
func TestFailingTableDoesNotStopTheRest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	tableMissing := errors.New("Error 1146: Table 'statusengine_perfdata' doesn't exist")

	mock.ExpectExec("DELETE FROM `statusengine_hostchecks`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DELETE FROM `statusengine_perfdata`").WillReturnError(tableMissing)
	mock.ExpectExec("DELETE FROM `statusengine_logentries`").WillReturnResult(sqlmock.NewResult(0, 0))

	tables := []Table{
		{Name: "statusengine_hostchecks", Column: "start_time", Days: 5},
		{Name: "statusengine_perfdata", Column: "timestamp_unix", Days: 90},
		{Name: "statusengine_logentries", Column: "entry_time", Days: 5},
	}

	err = Run(context.Background(), db, tables, Options{BatchSize: 100, Now: nowFunc})
	if err == nil {
		t.Fatal("Run returned nil, want the failing table's error")
	}
	if !errors.Is(err, tableMissing) {
		t.Fatalf("error %v does not wrap the driver error", err)
	}
	if !strings.Contains(err.Error(), "statusengine_perfdata") {
		t.Errorf("error %q does not name the failing table", err)
	}

	// The third table's expectation being met is the actual assertion:
	// the run continued past the failure.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations - the run stopped at the failing table: %v", err)
	}
}

// TestContextCancelStopsBetweenBatches pins the graceful-stop behaviour a
// systemd timer's SIGTERM relies on: no further statement is issued, and
// the interruption is not reported as a failure.
func TestContextCancelStopsBetweenBatches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const batch = 100

	// A full batch would normally be followed by another. Cancelling
	// while this one "runs" must end the loop instead - so exactly one
	// statement is expected, and a second would fail the test.
	mock.ExpectExec("DELETE FROM `statusengine_servicechecks`").
		WillReturnResult(sqlmock.NewResult(0, batch)).
		WillDelayFor(time.Millisecond)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	tables := []Table{
		{Name: "statusengine_servicechecks", Column: "start_time", Days: 5},
	}

	// Pause long enough that the cancel lands while the loop waits
	// between batches, which is the exact window being tested.
	err = Run(ctx, db, tables, Options{BatchSize: batch, Pause: time.Second, Now: nowFunc})
	if err != nil {
		t.Fatalf("an interrupted run must not be reported as a failure, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestContextCancelSkipsRemainingTables checks the outer loop's exit: once
// cancelled, later tables are not started at all.
func TestContextCancelSkipsRemainingTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tables := []Table{
		{Name: "statusengine_hostchecks", Column: "start_time", Days: 5},
		{Name: "statusengine_logentries", Column: "entry_time", Days: 5},
	}

	if err := Run(ctx, db, tables, Options{BatchSize: 100, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPauseIsAppliedBetweenBatches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	const batch = 10
	const pause = 40 * time.Millisecond

	mock.ExpectExec("DELETE FROM `statusengine_hostchecks`").WillReturnResult(sqlmock.NewResult(0, batch))
	mock.ExpectExec("DELETE FROM `statusengine_hostchecks`").WillReturnResult(sqlmock.NewResult(0, 0))

	tables := []Table{{Name: "statusengine_hostchecks", Column: "start_time", Days: 5}}

	started := time.Now()
	if err := Run(context.Background(), db, tables, Options{BatchSize: batch, Pause: pause, Now: nowFunc}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(started)

	if elapsed < pause {
		t.Errorf("run took %v, want at least one %v pause between the two batches", elapsed, pause)
	}
}

// TestNoPauseAfterLastBatch: the pause exists to give the database room
// between batches, so paying it after the final one would just make every
// table's cleanup needlessly slower.
func TestNoPauseAfterLastBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("DELETE FROM `statusengine_hostchecks`").WillReturnResult(sqlmock.NewResult(0, 0))

	tables := []Table{{Name: "statusengine_hostchecks", Column: "start_time", Days: 5}}

	started := time.Now()
	if err := Run(context.Background(), db, tables, Options{
		BatchSize: 100,
		Pause:     2 * time.Second,
		Now:       nowFunc,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("run took %v - the pause was applied after the final batch", elapsed)
	}
}

func TestInvalidBatchSizeIsRejected(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	tables := []Table{{Name: "statusengine_hostchecks", Column: "start_time", Days: 5}}

	for _, size := range []int{0, -1} {
		if err := Run(context.Background(), db, tables, Options{BatchSize: size, Now: nowFunc}); err == nil {
			t.Errorf("BatchSize %d was accepted, want an error", size)
		}
	}
}

// TestNilNowDefaultsToTimeNow makes sure the zero Options value is usable
// and does not panic on a nil clock.
func TestNilNowDefaultsToTimeNow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	before := time.Now().AddDate(0, 0, -5).Unix()

	mock.ExpectExec("DELETE FROM `statusengine_hostchecks`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tables := []Table{{Name: "statusengine_hostchecks", Column: "start_time", Days: 5}}

	if err := Run(context.Background(), db, tables, Options{BatchSize: 100}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	after := time.Now().AddDate(0, 0, -5).Unix()
	if before > after {
		t.Fatal("clock went backwards")
	}
}

// regexpQuote escapes a literal SQL string for sqlmock, whose default
// query matcher treats its argument as a regular expression - and these
// statements are full of characters that means something in one.
func regexpQuote(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(`\.+*?()|[]{}^$`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

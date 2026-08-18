package db

import (
	"context"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
)

// TestRunFlushesOnConfiguredBatchSize is the point of the whole change: an
// inserter built with WithMaxBatchSize must flush at that number, not at
// DefaultMaxBatchSize. A size well above the default is used so a regression
// that ignored the option would flush early and fail here rather than pass by
// coincidence.
func TestRunFlushesOnConfiguredBatchSize(t *testing.T) {
	const batchSize = 250

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, batchSize))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow,
		WithMaxBatchSize(batchSize))

	if got := inserter.maxBatchSize; got != batchSize {
		t.Fatalf("maxBatchSize = %d, want %d", got, batchSize)
	}
	// in and buffer scale off the same number - drainPending's overshoot
	// bound depends on it, which is what MaxConfigurableBatchSize is derived
	// from.
	if got := cap(inserter.in); got != batchSize {
		t.Errorf("cap(in) = %d, want %d", got, batchSize)
	}
	if got := cap(inserter.buffer); got != 2*batchSize {
		t.Errorf("cap(buffer) = %d, want %d", got, 2*batchSize)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go inserter.Run(ctx)

	for i := 0; i < batchSize; i++ {
		if err := inserter.Enqueue(ctx, row{id: i}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Well under the 250ms ticker, so a pass proves the size trigger fired
	// at 250 rather than the ticker flushing whatever had accumulated.
	waitForExpectations(t, mock, 100*time.Millisecond)
}

// TestDefaultBatchSizeIsUnchanged pins the behaviour of an inserter built
// without the option, since every existing call site relies on it.
func TestDefaultBatchSizeIsUnchanged(t *testing.T) {
	inserter := NewBulkInserter[row](nil, "events", []string{"id", "name"}, toRow)

	if got := inserter.maxBatchSize; got != DefaultMaxBatchSize {
		t.Errorf("maxBatchSize = %d, want %d", got, DefaultMaxBatchSize)
	}
	if got := DefaultMaxBatchSize; got != 100 {
		t.Errorf("DefaultMaxBatchSize = %d, want 100 - CLAUDE.md rule 3 and the README quote this number", got)
	}
}

// TestWithMaxBatchSizeClamps covers the last line of defence. cmd/app rejects
// out-of-range values outright, so this is about a caller inside the process
// getting it wrong, not about configuration.
func TestWithMaxBatchSizeClamps(t *testing.T) {
	tests := []struct {
		name string
		give int
		want int
	}{
		{"zero becomes one", 0, 1},
		{"negative becomes one", -5, 1},
		{"above the ceiling is capped", 5000, MaxConfigurableBatchSize},
		{"the ceiling itself is kept", MaxConfigurableBatchSize, MaxConfigurableBatchSize},
		{"a normal value is kept", 250, 250},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inserter := NewBulkInserter[row](nil, "events", []string{"id", "name"}, toRow,
				WithMaxBatchSize(tt.give))
			if got := inserter.maxBatchSize; got != tt.want {
				t.Errorf("WithMaxBatchSize(%d) -> %d, want %d", tt.give, got, tt.want)
			}
		})
	}
}

// TestConstructionRejectsTooManyPlaceholders proves the guard in
// NewBulkInserter fires on the case it exists for: a table wide enough that
// the *drain overshoot* - not the batch size - would exceed MySQL's 65535
// placeholders. Without it that statement is only ever built on a shutdown or
// core-restart flush, fails with Error 1390, is not retried because 1390 is
// deterministic, and the batch is dropped with nothing but one log line.
func TestConstructionRejectsTooManyPlaceholders(t *testing.T) {
	// Just wide enough that 2n-1 rows overflow while n rows alone would not:
	// at n=700 an ordinary batch of 700x60 = 42000 fits comfortably, but the
	// drain's 1399x60 = 83940 does not. A guard written against the batch
	// size instead of the overshoot would pass this and ship the bug.
	const columns = 60

	if DefaultMaxBatchSize*columns > MaxPlaceholdersPerStatement {
		t.Fatalf("test setup is wrong: %d columns already overflow at one batch", columns)
	}

	cols := make([]string, columns)
	for i := range cols {
		cols[i] = "c"
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewBulkInserter accepted a table whose drain flush exceeds 65535 placeholders")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panicked with %T, want a string explaining the limit", r)
		}
		// The message has to name the table and the numbers - it is the only
		// thing whoever added the column will see.
		for _, want := range []string{"widetable", "65535"} {
			if !strings.Contains(msg, want) {
				t.Errorf("panic message %q does not mention %q", msg, want)
			}
		}
	}()

	NewBulkInserter[row](nil, "widetable", cols, toRow, WithMaxBatchSize(MaxConfigurableBatchSize))
}

// TestMaxConfigurableBatchSizeLeavesRoomForTheWidestTable is the arithmetic
// behind the ceiling, kept where the constant lives. internal/queue has the
// counterpart that checks the tables actually written.
func TestMaxConfigurableBatchSizeLeavesRoomForTheWidestTable(t *testing.T) {
	// statusengine_servicestatus, the widest table in the pipeline.
	const widestTableColumns = 43

	worst := (2*MaxConfigurableBatchSize - 1) * widestTableColumns
	if worst > MaxPlaceholdersPerStatement {
		t.Fatalf("a drain flush of %d columns at batch size %d needs %d placeholders, over the limit of %d",
			widestTableColumns, MaxConfigurableBatchSize, worst, MaxPlaceholdersPerStatement)
	}

	headroom := MaxPlaceholdersPerStatement/(2*MaxConfigurableBatchSize-1) - widestTableColumns
	if headroom < 1 {
		t.Errorf("no headroom left: the next column added to the widest table breaks the flush (worst case %d of %d placeholders)",
			worst, MaxPlaceholdersPerStatement)
	}
	t.Logf("batch size %d, worst case %d of %d placeholders, room for %d more columns",
		MaxConfigurableBatchSize, worst, MaxPlaceholdersPerStatement, headroom)
}

// labeledCounterValue reads one series of a labeled counter family back
// through the gatherer, the same way a Prometheus scrape would see it.
func labeledCounterValue(t *testing.T, name, label, value string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, pair := range series.GetLabel() {
				if pair.GetName() == label && pair.GetValue() == value {
					return series.GetCounter().GetValue()
				}
			}
		}
		t.Fatalf("%s: no series with %s=%q", name, label, value)
	}
	t.Fatalf("%s: no such metric family in the default registry", name)
	return 0
}

// TestFlushesTotalIsTheDenominatorOfTheBatchSize is the property the counter
// exists for: rows / flushes must come out as the average batch size, per
// table and without touching a histogram bucket. That is what lets a
// monitoring system which ingests only counters compute
//
//	rate(db_events_written_total[1m]) / rate(db_flushes_total[1m])
//
// Three flushes of a known, deliberately uneven size, so a counter that
// merely tracked rows or merely counted 1 per row would produce the wrong
// ratio rather than the right one by accident.
func TestFlushesTotalIsTheDenominatorOfTheBatchSize(t *testing.T) {
	const table = "flushes_ratio_events"

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	inserter := NewBulkInserter[row](mockDB, table, []string{"id", "name"}, toRow)

	beforeRows := labeledCounterValue(t, "statusengine_db_events_written_total", "table", table)
	beforeFlushes := labeledCounterValue(t, "statusengine_db_flushes_total", "table", table)

	// Pre-created at zero by the constructor, so a dashboard reads 0 rather
	// than "No data" on a worker that has not flushed this table yet.
	if beforeFlushes != 0 {
		t.Fatalf("flushes_total started at %v, want a freshly pre-created 0", beforeFlushes)
	}

	ctx := context.Background()
	sizes := []int{7, 20, 3}
	for _, size := range sizes {
		mock.ExpectExec(`^INSERT INTO ` + table).WillReturnResult(sqlmock.NewResult(0, int64(size)))
		for i := 0; i < size; i++ {
			inserter.buffer = append(inserter.buffer, row{id: i})
		}
		if err := inserter.flushBuffer(ctx); err != nil {
			t.Fatalf("flushBuffer: %v", err)
		}
	}

	gotRows := labeledCounterValue(t, "statusengine_db_events_written_total", "table", table) - beforeRows
	gotFlushes := labeledCounterValue(t, "statusengine_db_flushes_total", "table", table) - beforeFlushes

	if wantRows := float64(7 + 20 + 3); gotRows != wantRows {
		t.Errorf("events_written_total rose by %v, want %v", gotRows, wantRows)
	}
	if gotFlushes != float64(len(sizes)) {
		t.Errorf("flushes_total rose by %v, want %v", gotFlushes, len(sizes))
	}
	if got, want := gotRows/gotFlushes, 10.0; got != want {
		t.Errorf("rows/flushes = %v, want %v - the ratio is the whole point of this counter", got, want)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestFailedFlushIsNotCounted keeps numerator and denominator describing the
// same set of statements. A failed flush writes no rows, so counting it here
// would drag the average batch size down for a reason that has nothing to do
// with batching - and the failure already has its own counter,
// pipeline_errors_total{component="mysql"}.
func TestFailedFlushIsNotCounted(t *testing.T) {
	const table = "flushes_failed_events"

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	inserter := NewBulkInserter[row](mockDB, table, []string{"id", "name"}, toRow)
	mock.ExpectExec(`^INSERT INTO ` + table).WillReturnError(duplicateEntry())

	inserter.buffer = append(inserter.buffer, row{id: 1}, row{id: 2})
	if err := inserter.flushBuffer(context.Background()); err == nil {
		t.Fatal("flushBuffer returned nil for a failing exec")
	}

	if got := labeledCounterValue(t, "statusengine_db_flushes_total", "table", table); got != 0 {
		t.Errorf("flushes_total = %v after a failed flush, want 0", got)
	}
	if got := labeledCounterValue(t, "statusengine_db_events_written_total", "table", table); got != 0 {
		t.Errorf("events_written_total = %v after a failed flush, want 0", got)
	}
}

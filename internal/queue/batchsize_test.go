package queue

import (
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"statusengine-worker/internal/db"
	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/websocket"
)

// sizedInserter is what every db.BulkInserter in runners satisfies. Declared
// as an interface because runners is []Runner and the inserters have
// different type parameters, so there is no single concrete type to assert
// to.
type sizedInserter interface {
	MaxBatchSize() int
	ColumnCount() int
}

// TestBatchSizeStaysUnderPlaceholderLimit is the guard that keeps
// db.MaxConfigurableBatchSize honest across all 14 tables at once, and it is
// the reason that ceiling can be a single number rather than one per table.
//
// It does the arithmetic itself rather than relying on the panic in
// db.NewBulkInserter: a test that only asserted "constructing the router did
// not panic" would pass just as happily with a guard that computed the wrong
// bound, which is exactly the mistake worth catching. The bound is the drain
// overshoot of 2n-1 rows, not n - drainPending deliberately tops the buffer
// up past the batch size, and flushBuffer turns all of it into one statement.
// That happens on every shutdown and every core restart under load.
//
// If this fails, someone added columns to an already-wide table (the widest,
// statusengine_servicestatus, has 43 of the 46 that fit). The fix is to lower
// db.MaxConfigurableBatchSize, not to relax this: the alternative is Error
// 1390 on the shutdown flush, which is deterministic, therefore not retried
// by execWithRetry, and drops the batch with nothing but one log line.
func TestBatchSizeStaysUnderPlaceholderLimit(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	// At the ceiling, not at testBatchSize: the other NewRouter tests run at
	// the default of 100 and would sail past a table that has grown too wide.
	hub := websocket.NewHub()
	_, runners := NewRouter(mockDB, hub, graphite.NewClient("127.0.0.1:2003"), PerfdataRouteMySQL,
		"statusengine-test", "statusengine-test", false, noAgeFilter, db.MaxConfigurableBatchSize)

	var checked, widest int
	for _, r := range runners {
		// The Graphite client is a Runner too, with its own unrelated batch
		// size, so pick out the BulkInserters.
		bi, ok := r.(sizedInserter)
		if !ok {
			continue
		}
		checked++

		// Also the proof that NewRouter passes the size on at all: an
		// inserter left on the default would quietly run at 100, and the
		// placeholder check below would then be testing nothing.
		if got := bi.MaxBatchSize(); got != db.MaxConfigurableBatchSize {
			t.Errorf("an inserter runs at batch size %d, want %d - a constructor is missing the batch option",
				got, db.MaxConfigurableBatchSize)
			continue
		}

		if cols := bi.ColumnCount(); cols > widest {
			widest = cols
		}
		if worst := (2*bi.MaxBatchSize() - 1) * bi.ColumnCount(); worst > db.MaxPlaceholdersPerStatement {
			t.Errorf("a %d-column table needs %d placeholders for a drain flush at batch size %d, over MySQL's limit of %d",
				bi.ColumnCount(), worst, bi.MaxBatchSize(), db.MaxPlaceholdersPerStatement)
		}
	}

	if checked != 14 {
		t.Errorf("checked %d inserters, want 14 - a table was added without the batch option, or one was removed", checked)
	}
	t.Logf("widest table: %d columns, worst case %d of %d placeholders at batch size %d",
		widest, (2*db.MaxConfigurableBatchSize-1)*widest, db.MaxPlaceholdersPerStatement, db.MaxConfigurableBatchSize)
}

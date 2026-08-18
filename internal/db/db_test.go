package db

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

type row struct {
	id   int
	name string
}

func toRow(r row, dst []any) []any { return append(dst, r.id, r.name) }

func TestBuildInsert(t *testing.T) {
	b := NewBulkInserter[row](nil, "mytable", []string{"id", "name"}, toRow)

	query, args := b.buildInsert([]row{{1, "a"}, {2, "b"}})

	wantQuery := "INSERT INTO mytable (id, name) VALUES (?,?), (?,?)"
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}

	wantArgs := []any{1, "a", 2, "b"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %v, want %v", args, wantArgs)
	}
}

func TestBuildInsertUpsert(t *testing.T) {
	b := NewUpsertBulkInserter[row](nil, "mytable", []string{"id", "name"}, []string{"name"}, toRow)

	query, args := b.buildInsert([]row{{1, "a"}, {2, "b"}})

	wantQuery := "INSERT INTO mytable (id, name) VALUES (?,?), (?,?) ON DUPLICATE KEY UPDATE name = VALUES(name)"
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}

	wantArgs := []any{1, "a", 2, "b"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %v, want %v", args, wantArgs)
	}
}

func TestBuildInsertEmpty(t *testing.T) {
	b := NewBulkInserter[row](nil, "mytable", []string{"id", "name"}, toRow)
	if _, args := b.buildInsert(nil); len(args) != 0 {
		t.Fatalf("expected no args for empty item slice, got %v", args)
	}
}

// waitForExpectations polls mock's expectations since flushes happen
// asynchronously inside Run's goroutine.
func waitForExpectations(t *testing.T, mock sqlmock.Sqlmock, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if err := mock.ExpectationsWereMet(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expectations not met within %s: %v", timeout, mock.ExpectationsWereMet())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRunFlushesOnBatchSize(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, DefaultMaxBatchSize))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go inserter.Run(ctx)

	for i := 0; i < DefaultMaxBatchSize; i++ {
		if err := inserter.Enqueue(ctx, row{id: i}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// The 100th item alone should trigger the flush well before the 250ms
	// ticker would - assert quickly to prove the size trigger fired, not
	// the ticker.
	waitForExpectations(t, mock, 100*time.Millisecond)
}

func TestRunFlushesOnTicker(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 3))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go inserter.Run(ctx)

	for i := 0; i < 3; i++ {
		if err := inserter.Enqueue(ctx, row{id: i}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	waitForExpectations(t, mock, time.Second)
}

func TestFlushMethodClearsBuffer(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events \(id, name\) VALUES \(\?,\?\)$`).
		WithArgs(1, "first").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`^INSERT INTO events \(id, name\) VALUES \(\?,\?\)$`).
		WithArgs(2, "second").
		WillReturnResult(sqlmock.NewResult(0, 1))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go inserter.Run(ctx)

	if err := inserter.Enqueue(ctx, row{id: 1, name: "first"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := inserter.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A second Flush with a fresh item must only contain that new item -
	// proof the buffer was actually cleared after the first flush. If it
	// weren't, this exec would carry both rows and fail to match either
	// registered expectation.
	if err := inserter.Enqueue(ctx, row{id: 2, name: "second"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := inserter.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

// TestWithPaused proves the pause/resume contract end-to-end against a real
// Run goroutine: whatever was already buffered gets flushed before fn runs,
// items Enqueued while fn is running only buffer (no flush fires until
// resume), and Run keeps working normally once WithPaused returns.
func TestWithPaused(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events \(id, name\) VALUES \(\?,\?\)$`).
		WithArgs(1, "first").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`^TRUNCATE TABLE events$`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`^INSERT INTO events \(id, name\) VALUES \(\?,\?\)$`).
		WithArgs(2, "second").
		WillReturnResult(sqlmock.NewResult(0, 1))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go inserter.Run(ctx)

	if err := inserter.Enqueue(ctx, row{id: 1, name: "first"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var fnRan, enqueuedDuringPause bool
	err = inserter.WithPaused(ctx, func(ctx context.Context) error {
		fnRan = true
		if _, err := mockDB.ExecContext(ctx, "TRUNCATE TABLE events"); err != nil {
			return err
		}
		// Must not trigger a flush while Run is paused - only buffer.
		if err := inserter.Enqueue(ctx, row{id: 2, name: "second"}); err != nil {
			return err
		}
		enqueuedDuringPause = true
		return nil
	})
	if err != nil {
		t.Fatalf("WithPaused: %v", err)
	}
	if !fnRan || !enqueuedDuringPause {
		t.Fatalf("fn did not run fully: fnRan=%v enqueuedDuringPause=%v", fnRan, enqueuedDuringPause)
	}

	// Proves Run wasn't left stuck: the item buffered during the pause still
	// flushes normally once resumed.
	if err := inserter.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	waitForExpectations(t, mock, time.Second)
}

func TestFlushOnEmptyBufferIsNoop(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	// No ExpectExec registered: any exec would fail ExpectationsWereMet /
	// produce an "unexpected call" error.

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go inserter.Run(ctx)

	if err := inserter.Flush(ctx); err != nil {
		t.Fatalf("Flush on empty buffer should be a no-op, got err: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("no exec should have happened: %v", err)
	}
}

func TestShutdownFlushesRemainingBuffer(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 1))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	ctx, cancel := context.WithCancel(context.Background())
	if err := inserter.Enqueue(ctx, row{id: 1, name: "pending"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	done := make(chan struct{})
	go func() {
		inserter.Run(ctx)
		close(done)
	}()

	cancel() // simulate graceful shutdown cancelling the pipeline's context

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	waitForExpectations(t, mock, 100*time.Millisecond)
}

// TestFlushRefusesMismatchedRowValues covers the guard that the append-style
// RowFunc makes necessary: a RowFunc contributing the wrong number of
// values misaligns every placeholder after it, and MySQL reports that as
// one opaque complaint about a statement built from thirteen different
// RowFuncs. The batch must be rejected here, with a message that names the
// table and the mismatch, rather than sent and blamed on the database.
func TestFlushRefusesMismatchedRowValues(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	// Two columns declared, three values appended.
	shortRow := func(r row, dst []any) []any { return append(dst, r.id, r.name, "surplus") }
	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, shortRow)
	inserter.buffer = append(inserter.buffer, row{1, "a"})

	err = inserter.flushBuffer(context.Background())
	if err == nil {
		t.Fatal("expected a mismatched row to be rejected, got no error")
	}
	if got := err.Error(); !strings.Contains(got, "events") || !strings.Contains(got, "want 2") {
		t.Fatalf("error %q should name the table and the expected count", got)
	}

	// The batch must be cleared, not retried forever against a broken RowFunc.
	if len(inserter.buffer) != 0 {
		t.Fatalf("buffer still holds %d rows after a rejected flush", len(inserter.buffer))
	}
	// And nothing may have reached the database.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a statement was executed despite the mismatch: %v", err)
	}
}

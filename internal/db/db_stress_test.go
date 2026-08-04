package db

import (
	"context"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestBulkInserterConcurrentStress hammers Enqueue and Flush from many
// goroutines concurrently while Run batches/flushes in the background, to
// give `go test -race` (CLAUDE.md's test command) a much larger
// concurrency surface than the sequential existing tests exercise -
// proving buffer access really is confined to Run's single goroutine (see
// BulkInserter's doc comment) even under concurrent producers.
func TestBulkInserterConcurrentStress(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	// Any number of INSERTs of any shape are fine here - concurrency safety
	// is what's under test, not exact batching.
	mock.MatchExpectationsInOrder(false)
	for i := 0; i < 20000; i++ {
		mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 1))
	}

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go inserter.Run(ctx)

	const numEnqueuers = 30
	const itemsPerGoroutine = 200
	const numFlushers = 5

	var enqueueWG sync.WaitGroup
	for i := 0; i < numEnqueuers; i++ {
		enqueueWG.Add(1)
		go func(i int) {
			defer enqueueWG.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				if err := inserter.Enqueue(ctx, row{id: i*itemsPerGoroutine + j, name: "x"}); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
			}
		}(i)
	}

	stopFlushers := make(chan struct{})
	var flushWG sync.WaitGroup
	for i := 0; i < numFlushers; i++ {
		flushWG.Add(1)
		go func() {
			defer flushWG.Done()
			for {
				select {
				case <-stopFlushers:
					return
				default:
					inserter.Flush(ctx)
				}
			}
		}()
	}

	enqueueDone := make(chan struct{})
	go func() {
		defer close(enqueueDone)
		enqueueWG.Wait()
	}()

	select {
	case <-enqueueDone:
	case <-time.After(10 * time.Second):
		t.Fatal("enqueuers did not finish in time")
	}

	close(stopFlushers)
	flushWG.Wait()

	if err := inserter.Flush(ctx); err != nil {
		t.Fatalf("final Flush: %v", err)
	}
}

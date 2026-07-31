// Package db implements the throttled, ticker- and batch-driven bulk-insert
// buffer that persists queue events into MySQL (see CLAUDE.md rule 3).
package db

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"
)

const (
	// MaxBatchSize is the buffer capacity that triggers an immediate flush.
	MaxBatchSize = 100
	// FlushInterval is the ticker period that triggers a flush of whatever
	// is buffered, even if MaxBatchSize hasn't been reached.
	FlushInterval = 250 * time.Millisecond
)

// RowFunc converts a single buffered item into the ordered column values
// for one VALUES(...) tuple of a bulk INSERT.
type RowFunc[T any] func(item T) []any

type flushRequest struct {
	ctx   context.Context
	reply chan error
}

// BulkInserter batches items received via Enqueue and flushes them to a
// single MySQL table as one multi-row INSERT statement as soon as EITHER
// the buffer reaches MaxBatchSize OR FlushInterval elapses - whichever
// happens first (CLAUDE.md rule 3). One BulkInserter instance handles
// exactly one destination table; the pipeline runs one per event type.
type BulkInserter[T any] struct {
	db      *sql.DB
	table   string
	columns []string
	toRow   RowFunc[T]

	in       chan T
	flushReq chan flushRequest

	buffer []T
}

// NewBulkInserter creates a BulkInserter for table, using toRow to turn a
// buffered item into that row's column values (in the same order as
// columns). Run must be started in its own goroutine before items are
// actually flushed to db.
func NewBulkInserter[T any](db *sql.DB, table string, columns []string, toRow RowFunc[T]) *BulkInserter[T] {
	return &BulkInserter[T]{
		db:       db,
		table:    table,
		columns:  columns,
		toRow:    toRow,
		in:       make(chan T, MaxBatchSize),
		flushReq: make(chan flushRequest),
		buffer:   make([]T, 0, MaxBatchSize),
	}
}

// Enqueue hands an item to the inserter's buffer, blocking only until
// either it is accepted or ctx is cancelled.
func (b *BulkInserter[T]) Enqueue(ctx context.Context, item T) error {
	select {
	case b.in <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush requests an immediate bulk-insert of whatever is currently
// buffered and waits for it to complete, using ctx for both the request
// and the resulting query. This is the method a graceful-shutdown sequence
// calls to drain the buffer before the process exits (CLAUDE.md rule 6).
func (b *BulkInserter[T]) Flush(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case b.flushReq <- flushRequest{ctx: ctx, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run owns the buffer and drives it until ctx is cancelled or the input
// channel is closed, at which point it performs one last best-effort flush
// (using a fresh, short-lived context, since ctx is already done at that
// point) before returning. It must run in exactly one goroutine.
func (b *BulkInserter[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// ctx.Done() can become ready in the same instant an item is
			// sitting in b.in (already accepted by Enqueue but not yet
			// read here), and select picks pseudo-randomly between ready
			// cases - so drain whatever is buffered in the channel before
			// the final flush to avoid silently dropping it.
			b.drainPending()
			b.finalFlush()
			return

		case item, ok := <-b.in:
			if !ok {
				b.drainPending()
				b.finalFlush()
				return
			}
			b.buffer = append(b.buffer, item)
			if len(b.buffer) >= MaxBatchSize {
				b.flushBuffer(ctx)
				ticker.Reset(FlushInterval)
			}

		case <-ticker.C:
			b.flushBuffer(ctx)
			ticker.Reset(FlushInterval)

		case req := <-b.flushReq:
			// Same race as ctx.Done() above: an item already accepted by
			// Enqueue may still be sitting in b.in rather than b.buffer.
			// Drain it first so Flush() truly flushes everything handed
			// to Enqueue before it was called.
			b.drainPending()
			err := b.flushBuffer(req.ctx)
			ticker.Reset(FlushInterval)
			req.reply <- err
		}
	}
}

// drainPending moves any items already sitting in the input channel's
// buffer into b.buffer without blocking, so a shutdown racing with an
// in-flight Enqueue never silently loses that item.
func (b *BulkInserter[T]) drainPending() {
	for {
		select {
		case item := <-b.in:
			b.buffer = append(b.buffer, item)
		default:
			return
		}
	}
}

// finalFlush is used on shutdown, when Run's own ctx is already cancelled
// and therefore unusable for a query - it gives the last batch a bounded
// window of its own instead of dropping it silently.
func (b *BulkInserter[T]) finalFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.flushBuffer(ctx)
}

// flushBuffer executes the buffered rows as a single bulk INSERT and
// clears the buffer, reusing its underlying array. A failed insert is
// logged and the batch is dropped rather than retried indefinitely, since
// retrying here would either block the pipeline or grow the buffer
// unbounded.
func (b *BulkInserter[T]) flushBuffer(ctx context.Context) error {
	if len(b.buffer) == 0 {
		return nil
	}

	query, args := b.buildInsert(b.buffer)
	_, err := b.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Printf("db: bulk insert into %s failed (%d rows dropped): %v", b.table, len(b.buffer), err)
	}

	b.buffer = b.buffer[:0]
	return err
}

// buildInsert renders "INSERT INTO table (cols...) VALUES (?,...), (?,...), ..."
// for items, along with the flattened argument list in matching order.
func (b *BulkInserter[T]) buildInsert(items []T) (string, []any) {
	rowPlaceholder := "(" + strings.TrimSuffix(strings.Repeat("?,", len(b.columns)), ",") + ")"

	var query strings.Builder
	query.WriteString("INSERT INTO ")
	query.WriteString(b.table)
	query.WriteString(" (")
	query.WriteString(strings.Join(b.columns, ", "))
	query.WriteString(") VALUES ")

	args := make([]any, 0, len(items)*len(b.columns))
	for i, item := range items {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(rowPlaceholder)
		args = append(args, b.toRow(item)...)
	}

	return query.String(), args
}

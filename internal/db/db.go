// Package db implements the throttled, ticker- and batch-driven bulk-insert
// buffer that persists queue events into MySQL (see CLAUDE.md rule 3).
package db

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync/atomic"
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

// pauseRequest asks Run's goroutine to stop flushing until resume is closed:
// paused is closed once Run has actually stopped (so the caller knows it's
// safe to run a statement against the table outside the BulkInserter), then
// Run blocks until resume is closed.
type pauseRequest struct {
	paused chan struct{}
	resume chan struct{}
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

	// updateColumns, when non-empty, turns every flush into an
	// "INSERT ... ON DUPLICATE KEY UPDATE" upsert instead of a plain INSERT:
	// on a primary-key collision, each listed column is refreshed from the
	// row that would have been inserted (MySQL's VALUES(col)). Set via
	// NewUpsertBulkInserter for tables that receive repeated snapshots of
	// the same row (e.g. hoststatus/servicestatus, keyed on hostname).
	updateColumns []string

	in       chan T
	flushReq chan flushRequest
	pauseReq chan pauseRequest

	buffer []T

	// processed is the running total of rows successfully flushed to db,
	// reported on every flush log line so operators can see throughput
	// without needing to scrape metrics separately. Only ever incremented
	// from flushBuffer, which always runs inside Run's single goroutine -
	// atomic only because it's convenient to read from outside that
	// goroutine too (e.g. future health/metrics endpoints), not because
	// concurrent writers exist.
	processed atomic.Uint64
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
		pauseReq: make(chan pauseRequest),
		buffer:   make([]T, 0, MaxBatchSize),
	}
}

// NewUpsertBulkInserter creates a BulkInserter exactly like NewBulkInserter,
// except every flush is executed as a single
// "INSERT INTO table (...) VALUES (...), ... ON DUPLICATE KEY UPDATE ..."
// statement: on a primary-key collision, each column named in updateColumns
// is refreshed from the row that would have been inserted. Use this for
// tables that receive repeated snapshots of the same logical row (e.g.
// statusengine_hoststatus/statusengine_servicestatus, keyed on hostname).
func NewUpsertBulkInserter[T any](db *sql.DB, table string, columns []string, updateColumns []string, toRow RowFunc[T]) *BulkInserter[T] {
	b := NewBulkInserter(db, table, columns, toRow)
	b.updateColumns = updateColumns
	return b
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

// WithPaused flushes whatever is currently buffered, then blocks Run's
// goroutine from touching the table (no more Enqueue-triggered or
// ticker-triggered flushes) until fn returns. Use this to run a statement
// against the same table from outside the BulkInserter - e.g. the
// core-restart hoststatus/servicestatus TRUNCATE/DELETE cleanup - without
// racing an in-flight bulk INSERT and risking a lock-wait/deadlock against
// MySQL. Items can still be Enqueued while paused (they just buffer in the
// input channel up to MaxBatchSize), so ingestion is never blocked, only
// briefly delayed.
func (b *BulkInserter[T]) WithPaused(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := b.Flush(ctx); err != nil {
		return err
	}

	req := pauseRequest{paused: make(chan struct{}), resume: make(chan struct{})}
	select {
	case b.pauseReq <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Run has now received req and is guaranteed to reach its close(req.paused)
	// (see the pauseReq case in Run) independent of anything below, so it's
	// always safe - and necessary - to unblock it via resume before returning.
	defer close(req.resume)

	select {
	case <-req.paused:
	case <-ctx.Done():
		return ctx.Err()
	}

	return fn(ctx)
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

		case req := <-b.pauseReq:
			// The caller already called Flush before sending req (see
			// WithPaused), so the buffer is empty here; just stop touching
			// the table until resume is closed. ctx.Done() is watched too,
			// so a shutdown mid-pause still exits cleanly instead of hanging
			// forever waiting for a resume that will never come.
			close(req.paused)
			select {
			case <-req.resume:
				ticker.Reset(FlushInterval)
			case <-ctx.Done():
				b.drainPending()
				b.finalFlush()
				return
			}
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
//
// Every flush is logged exactly once, at most every FlushInterval (or
// MaxBatchSize rows) - never per row - so structured logging never adds
// per-message overhead to the hot ingestion path (CLAUDE.md rule 2).
func (b *BulkInserter[T]) flushBuffer(ctx context.Context) error {
	if len(b.buffer) == 0 {
		return nil
	}
	rows := len(b.buffer)

	query, args := b.buildInsert(b.buffer)

	start := time.Now()
	_, err := b.db.ExecContext(ctx, query, args...)
	duration := time.Since(start)

	if err != nil {
		slog.Error("db: bulk insert failed, rows dropped",
			"table", b.table, "rows", rows, "duration", duration, "error", err)
	} else {
		total := b.processed.Add(uint64(rows))
		slog.Info("db: bulk insert flushed",
			"table", b.table, "rows", rows, "duration", duration, "total_processed", total)
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

	if len(b.updateColumns) > 0 {
		query.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, col := range b.updateColumns {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(col)
			query.WriteString(" = VALUES(")
			query.WriteString(col)
			query.WriteString(")")
		}
	}

	return query.String(), args
}

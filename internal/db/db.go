// Package db implements the throttled, ticker- and batch-driven bulk-insert
// buffer that persists queue events into MySQL (see CLAUDE.md rule 3).
package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"statusengine-worker/internal/metrics"
)

const (
	// MaxBatchSize is the buffer capacity that triggers an immediate flush.
	MaxBatchSize = 100
	// FlushInterval is the ticker period that triggers a flush of whatever
	// is buffered, even if MaxBatchSize hasn't been reached.
	FlushInterval = 250 * time.Millisecond
)

// RowFunc appends a single buffered item's column values, in the order
// declared by the inserter's columns, to dst and returns the extended
// slice - the ordinary Go append contract:
//
//	func myRow(ev myEvent, dst []any) []any {
//		return append(dst, ev.Host, ev.Timestamp, ev.State)
//	}
//
// It must append exactly as many values as there are columns. Appending
// into dst rather than returning a fresh []any is what keeps the hot path
// cheap: buildInsert already sizes one args slice for the whole batch, so
// a RowFunc that allocated its own would have every row allocate a slice
// only to be copied out of and thrown away. At MaxBatchSize rows of a
// 40-column table that measured ~39% of the bytes and ~29% of the time
// spent building a statement.
type RowFunc[T any] func(item T, dst []any) []any

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

	// rowPlaceholder ("(?, ?, ...)") and updateClause
	// (" ON DUPLICATE KEY UPDATE col = VALUES(col), ...") are fixed for
	// the lifetime of an inserter, since table, columns and updateColumns
	// never change after construction. Built once here rather than
	// re-derived on every flush - at one flush per 250ms per table across
	// the whole pipeline, that adds up to a lot of identical string
	// building.
	rowPlaceholder string
	updateClause   string

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
	// Register this table's series at zero right away, so /metrics reports
	// events_written_total{table="..."} 0 from the first scrape instead of
	// omitting the table until its first successful flush.
	metrics.InitTable(table)

	return &BulkInserter[T]{
		db:             db,
		table:          table,
		columns:        columns,
		toRow:          toRow,
		rowPlaceholder: buildRowPlaceholder(len(columns)),
		in:             make(chan T, MaxBatchSize),
		flushReq:       make(chan flushRequest),
		pauseReq:       make(chan pauseRequest),
		// 2*MaxBatchSize, not MaxBatchSize: drainPending can top up an
		// almost-full buffer with everything sitting in b.in, which holds
		// MaxBatchSize itself. Sizing for the real maximum keeps the flush
		// path free of reallocations - the alternative is a one-time grow
		// to exactly this size the first time a drain lands on a full
		// buffer, after which the backing array stays this large anyway.
		buffer: make([]T, 0, 2*MaxBatchSize),
	}
}

// buildRowPlaceholder renders one VALUES tuple, "(?,?,...)", for n
// columns. The exact spelling is load-bearing: existing tests match the
// generated statement literally, so it must stay byte-for-byte what the
// per-flush version produced.
func buildRowPlaceholder(n int) string {
	if n == 0 {
		return "()"
	}
	return "(" + strings.TrimSuffix(strings.Repeat("?,", n), ",") + ")"
}

// buildUpdateClause renders the "ON DUPLICATE KEY UPDATE col = VALUES(col)"
// tail that turns a flush into an upsert, or "" when there is nothing to
// update on collision.
func buildUpdateClause(updateColumns []string) string {
	if len(updateColumns) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, col := range updateColumns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
		b.WriteString(" = VALUES(")
		b.WriteString(col)
		b.WriteString(")")
	}
	return b.String()
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
	b.updateClause = buildUpdateClause(updateColumns)
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
//
// This deliberately appends past MaxBatchSize - CLAUDE.md rule 3 governs
// when a flush is triggered, not how large a single rescued batch may be.
// The overshoot is bounded by b.in's own capacity, so b.buffer peaks just
// under 2*MaxBatchSize, which is what NewBulkInserter sizes it for.
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
// logged and the batch is dropped, with two exceptions that are retried
// instead: a transient locking failure and an unreachable server (see
// execWithRetry). Blocking the pipeline is the intended behaviour for the
// second of those, not a side effect.
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

	// A RowFunc that contributes the wrong number of values misaligns
	// every subsequent row's placeholders, and MySQL reports that as an
	// opaque complaint about the statement as a whole - with no hint as
	// to which of the 13 RowFuncs is at fault. Catch it here, where the
	// message can actually name the table and the mismatch, and refuse to
	// send a statement that is already known to be wrong.
	if want := rows * len(b.columns); len(args) != want {
		slog.Error("db: row values do not match column count, batch dropped",
			"table", b.table, "rows", rows, "columns", len(b.columns),
			"got_args", len(args), "want_args", want)
		metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentMySQL).Inc()
		b.buffer = b.buffer[:0]
		return fmt.Errorf("db: %s: got %d row values, want %d", b.table, len(args), want)
	}

	// duration covers retries as well, because that is what the pipeline
	// actually waited for. A retried flush therefore shows up as a single
	// slow observation rather than several fast ones, and
	// db_batch_retries_total is what explains the outlier.
	start := time.Now()
	err := b.execWithRetry(ctx, query, args...)
	duration := time.Since(start)

	metrics.DBBatchFlushDurationSeconds.Observe(duration.Seconds())
	metrics.DBBatchSizeAtFlush.Observe(float64(rows))

	if err != nil {
		slog.Error("db: bulk insert failed, rows dropped",
			"table", b.table, "rows", rows, "duration", duration, "error", err)
		metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentMySQL).Inc()
	} else {
		total := b.processed.Add(uint64(rows))
		metrics.DBEventsWrittenTotal.WithLabelValues(b.table).Add(float64(rows))
		slog.Info("db: bulk insert flushed",
			"table", b.table, "rows", rows, "duration", duration, "total_processed", total)
	}

	b.buffer = b.buffer[:0]
	return err
}

// MySQL error numbers that mean "the statement lost a race, try again",
// as opposed to "the statement is wrong".
const (
	mysqlErrTooManyConnections = 1040 // ER_CON_COUNT_ERROR
	mysqlErrServerShutdown     = 1053 // ER_SERVER_SHUTDOWN
	mysqlErrLockWaitTimeout    = 1205 // ER_LOCK_WAIT_TIMEOUT
	mysqlErrDeadlock           = 1213 // ER_LOCK_DEADLOCK
	mysqlErrConnectionKilled   = 1927 // ER_CONNECTION_KILLED
)

// flushRetryBackoff is how long to wait before each retry of a *lock*
// failure; its length therefore also caps the number of those retries, so
// a flush makes at most len+1 attempts. The values are deliberately short
// and not configurable: a deadlock clears in milliseconds, and Shutdown
// flushes under a 5s context.
var flushRetryBackoff = []time.Duration{50 * time.Millisecond, 200 * time.Millisecond}

// Connection failures are retried on a different schedule, because they
// are a different kind of transient: a MySQL restart takes tens of
// seconds, not milliseconds. The wait therefore backs off up to a cap and
// then keeps going for as long as the context allows, rather than being
// limited to a fixed number of attempts.
const (
	connRetryInitialBackoff = 100 * time.Millisecond
	connRetryMaxBackoff     = 5 * time.Second
	// connRetryLogInterval throttles the "still unreachable" log line. A
	// 20-minute outage must not produce one line per attempt.
	connRetryLogInterval = 10 * time.Second
)

// execWithRetry runs the bulk INSERT, re-executing the identical statement
// for the two classes of failure that are about timing rather than about
// the statement's content. Everything else is returned on the first
// attempt, and flushBuffer then drops the batch.
//
// A *lock* failure (deadlock, lock wait timeout) rolls back only this
// statement and is caused by another transaction's timing, so it clears in
// milliseconds; it gets len(flushRetryBackoff)+1 attempts.
//
// A *connection* failure means MySQL is unreachable - typically someone
// restarted it. It gets retried with a capped backoff for as long as ctx
// allows, which in practice means until the server is back or the worker
// is shut down. This is the deliberate part: while this call is blocked,
// Run's goroutine is blocked, so Enqueue blocks, so the queue handlers
// block, so the consumer's concurrency cap fills and the backlog stays at
// the broker, where it survives a worker restart and is visible in
// gearadmin --status (CLAUDE.md rule 2 - backpressure belongs at the
// broker). Dropping instead was measured at 29,400 of 150,000 events lost
// for a five-second outage, because a job is acknowledged as soon as
// Enqueue returns and there is no redelivery to fall back on.
//
// A permanently unreachable MySQL therefore stalls the pipeline rather
// than draining it into nowhere. That is the intended trade: a growing,
// visible, recoverable backlog beats a silent, unrecoverable hole.
//
// Everything else is deterministic - a truncated value, a NOT NULL
// violation, a column count mismatch - so a retry fails identically and
// only buys a slower shutdown and a triplicated log line.
//
// Retrying at all is only safe because the writes are idempotent
// (CLAUDE.md rule 6). Note the one gap that remains: a connection lost
// *mid*-statement leaves it unknown whether MySQL applied it, so the
// retry can duplicate a row in statusengine_logentries and
// statusengine_perfdata - the same two tables that already accept silent
// duplicates on a redelivery, for the same reason (no colliding key) and
// with the same reasoning (a duplicate in a retention-managed history
// table is less harmful than a missing event).
func (b *BulkInserter[T]) execWithRetry(ctx context.Context, query string, args ...any) error {
	var (
		lockAttempts  int
		connAttempts  int
		outageStart   time.Time
		lastOutageLog time.Time
		connBackoff   = connRetryInitialBackoff
	)

	for {
		_, err := b.db.ExecContext(ctx, query, args...)
		if err == nil {
			if connAttempts > 0 {
				metrics.DBAvailable.Set(1)
				slog.Info("db: MySQL is reachable again, held batch written",
					"table", b.table, "unavailable_for", time.Since(outageStart).Round(time.Millisecond),
					"attempts", connAttempts+1)
			}
			return nil
		}

		// A cancelled context means shutdown or an expired flush deadline,
		// never something a retry can fix.
		if ctx.Err() != nil {
			return err
		}

		switch {
		case isTransientLockError(err):
			if lockAttempts >= len(flushRetryBackoff) {
				return err
			}
			metrics.DBBatchRetriesTotal.Inc()
			slog.Warn("db: bulk insert hit a transient lock failure, retrying",
				"table", b.table, "attempt", lockAttempts+1,
				"attempts_allowed", len(flushRetryBackoff)+1, "error", err)
			if !sleepContext(ctx, flushRetryBackoff[lockAttempts]) {
				return err
			}
			lockAttempts++

		case isConnectionError(err):
			if connAttempts == 0 {
				outageStart = time.Now()
				metrics.DBAvailable.Set(0)
				slog.Warn("db: MySQL is unreachable, holding the batch until it returns",
					"table", b.table, "rows", len(b.buffer), "error", err)
				lastOutageLog = time.Now()
			} else if time.Since(lastOutageLog) >= connRetryLogInterval {
				slog.Warn("db: MySQL still unreachable, pipeline is stalled",
					"table", b.table, "unavailable_for", time.Since(outageStart).Round(time.Second),
					"attempts", connAttempts+1, "error", err)
				lastOutageLog = time.Now()
			}
			metrics.DBConnectionRetriesTotal.Inc()
			if !sleepContext(ctx, connBackoff) {
				return err
			}
			connAttempts++
			if connBackoff *= 2; connBackoff > connRetryMaxBackoff {
				connBackoff = connRetryMaxBackoff
			}

		default:
			return err
		}
	}
}

// sleepContext waits for d, reporting false if ctx was cancelled first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// isTransientLockError reports whether err is a MySQL deadlock or lock
// wait timeout. Matching on the numeric code rather than the message text
// keeps this independent of the server's language and version.
func isTransientLockError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == mysqlErrDeadlock || mysqlErr.Number == mysqlErrLockWaitTimeout
}

// isConnectionError reports whether err means "the server could not be
// reached" rather than "the server rejected this statement".
//
// There is deliberately no check for error 2006 ("MySQL server has gone
// away") or 2013 ("Lost connection during query"): those are client-side
// numbers from libmysqlclient and this driver never produces them. What it
// produces instead is driver.ErrBadConn when it can prove nothing was
// written, mysql.ErrInvalidConn when the connection broke at any other
// point, and a plain net error while the server is down and the dial
// itself fails. Note that database/sql already retries ErrBadConn up to
// three times on its own - it is listed here only for the case where
// those are exhausted, which is exactly what a restart does to a pool.
func isConnectionError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, io.EOF) {
		return true
	}

	// Covers dial failures ("connection refused" while the server is down)
	// as well as read/write failures on an established connection.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// The server is up enough to answer, but not to accept this work.
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case mysqlErrServerShutdown, mysqlErrTooManyConnections, mysqlErrConnectionKilled:
			return true
		}
	}

	return false
}

// buildInsert renders "INSERT INTO table (cols...) VALUES (?,...), (?,...), ..."
// for items, along with the flattened argument list in matching order.
func (b *BulkInserter[T]) buildInsert(items []T) (string, []any) {
	const prefix, infix = "INSERT INTO ", ") VALUES "

	columnList := strings.Join(b.columns, ", ")

	// Size the builder up front. Growing a strings.Builder reallocates and
	// copies everything written so far, and at MaxBatchSize rows of a wide
	// table (statusengine_hoststatus has 40 columns) that is several
	// doublings per flush, every flush.
	var query strings.Builder
	query.Grow(len(prefix) + len(b.table) + 2 + len(columnList) + len(infix) +
		len(items)*(len(b.rowPlaceholder)+2) + len(b.updateClause))

	query.WriteString(prefix)
	query.WriteString(b.table)
	query.WriteString(" (")
	query.WriteString(columnList)
	query.WriteString(infix)

	args := make([]any, 0, len(items)*len(b.columns))
	for i, item := range items {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(b.rowPlaceholder)
		args = b.toRow(item, args)
	}

	query.WriteString(b.updateClause)

	return query.String(), args
}

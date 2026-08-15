package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

const retriesMetric = "statusengine_db_batch_retries_total"

// counterValue reads one unlabeled counter back out of the default
// registry, by gathering rather than reaching into the metric - that is
// the only way to see what a scrape would actually report. Mirrors the
// helper of the same name in internal/websocket, deliberately without
// prometheus/testutil so go.mod stays untouched for a test-only
// convenience.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		series := family.GetMetric()
		if len(series) != 1 {
			t.Fatalf("%s: expected exactly one unlabeled series, got %d", name, len(series))
		}
		return series[0].GetCounter().GetValue()
	}
	t.Fatalf("%s: no such metric family in the default registry", name)
	return 0
}

// The tests below call flushBuffer directly with a pre-filled buffer.
// The retry lives entirely inside that one call, so driving it through
// Enqueue and a Run goroutine would only add timing to the test.

func deadlock() error {
	return &mysql.MySQLError{Number: mysqlErrDeadlock, Message: "Deadlock found when trying to get lock"}
}

func lockWaitTimeout() error {
	return &mysql.MySQLError{Number: mysqlErrLockWaitTimeout, Message: "Lock wait timeout exceeded"}
}

func duplicateEntry() error {
	return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
}

// TestFlushRetriesTransientLockFailure is the point of the whole change:
// a deadlock must cost a few hundred milliseconds, not a batch of rows.
func TestFlushRetriesTransientLockFailure(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnError(deadlock())
	mock.ExpectExec(`^INSERT INTO events`).WillReturnError(lockWaitTimeout())
	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 1))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	inserter.buffer = append(inserter.buffer, row{id: 1, name: "first"})

	before := counterValue(t, retriesMetric)

	if err := inserter.flushBuffer(context.Background()); err != nil {
		t.Fatalf("flushBuffer: expected the third attempt to succeed, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
	if got := counterValue(t, retriesMetric) - before; got != 2 {
		t.Fatalf("%s rose by %v, want 2", retriesMetric, got)
	}
	if len(inserter.buffer) != 0 {
		t.Fatalf("buffer holds %d rows after a successful flush, want 0", len(inserter.buffer))
	}
	// The rows must be counted as written exactly once, not once per
	// attempt - a retry that inflated db_events_written_total would make
	// the metric useless for exactly the incident it is watched during.
	if got := inserter.processed.Load(); got != 1 {
		t.Fatalf("processed = %d after one retried flush of one row, want 1", got)
	}
}

// TestFlushGivesUpAfterRetryLimit is the counter-proof for the test above:
// the retry must be bounded, or a table that is genuinely locked would
// stall the pipeline instead of dropping one batch.
func TestFlushGivesUpAfterRetryLimit(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	wantAttempts := len(flushRetryBackoff) + 1
	for i := 0; i < wantAttempts; i++ {
		mock.ExpectExec(`^INSERT INTO events`).WillReturnError(deadlock())
	}

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	inserter.buffer = append(inserter.buffer, row{id: 1, name: "first"})

	err = inserter.flushBuffer(context.Background())
	if err == nil {
		t.Fatal("flushBuffer: expected the persistent deadlock to be reported")
	}
	if !isTransientLockError(err) {
		t.Fatalf("flushBuffer returned %v, want the underlying MySQL error", err)
	}

	// ExpectationsWereMet only proves the registered execs happened; it
	// says nothing about a fourth one. sqlmock fails an unexpected exec,
	// so an unbounded retry would surface as flushBuffer returning that
	// complaint instead of the deadlock - which the check above catches.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
	if len(inserter.buffer) != 0 {
		t.Fatalf("buffer holds %d rows after a dropped batch, want 0", len(inserter.buffer))
	}
}

// TestFlushDoesNotRetryPermanentErrors is the guard against the lazy
// version of this change ("retry on any error"). A duplicate key, a
// truncated value or a NOT NULL violation fails identically every time, so
// retrying only delays the shutdown and triplicates the log line.
func TestFlushDoesNotRetryPermanentErrors(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	// Exactly one expectation: a second attempt has nothing to match and
	// fails the test.
	mock.ExpectExec(`^INSERT INTO events`).WillReturnError(duplicateEntry())

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	inserter.buffer = append(inserter.buffer, row{id: 1, name: "first"})

	before := counterValue(t, retriesMetric)

	var mysqlErr *mysql.MySQLError
	if err := inserter.flushBuffer(context.Background()); !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
		t.Fatalf("flushBuffer returned %v, want the 1062 straight back", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
	if got := counterValue(t, retriesMetric) - before; got != 0 {
		t.Fatalf("%s rose by %v for a permanent error, want 0", retriesMetric, got)
	}
}

// TestFlushStopsRetryingWhenTheContextExpires covers shutdown: Shutdown
// flushes under a 5s context, and a worker being stopped must not sit out
// its backoff.
//
// The context must expire *during* the backoff, not before the flush - an
// already-cancelled context is rejected by database/sql before the driver
// is reached, so the retry path would never run. Hence the deadline is
// generous relative to the one exec and the backoff is made absurd: the
// exec lands well inside 200ms, the wait afterwards would take an hour,
// and only the ctx.Done arm can end it. If the machine ever stalls long
// enough for the deadline to beat the exec, the unmatched expectation
// fails the test rather than passing it for the wrong reason.
func TestFlushStopsRetryingWhenTheContextExpires(t *testing.T) {
	restore := flushRetryBackoff
	flushRetryBackoff = []time.Duration{time.Hour, time.Hour}
	defer func() { flushRetryBackoff = restore }()

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnError(deadlock())

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	inserter.buffer = append(inserter.buffer, row{id: 1, name: "first"})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- inserter.flushBuffer(ctx) }()

	select {
	case err := <-done:
		// The MySQL error, not the context error: that is what the
		// operator needs in the log to know why the batch was dropped.
		if !isTransientLockError(err) {
			t.Fatalf("flushBuffer returned %v, want the underlying deadlock", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("flushBuffer sat out its backoff instead of honouring the expired context")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

// TestIsTransientLockError pins down which codes are retried, so widening
// the set is a deliberate edit rather than a side effect.
func TestIsTransientLockError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"deadlock", deadlock(), true},
		{"lock wait timeout", lockWaitTimeout(), true},
		{"duplicate entry", duplicateEntry(), false},
		{"not a MySQL error", errors.New("connection refused"), false},
		{"nil", nil, false},
		// errors.As, not a type assertion: a driver or a future wrapper
		// here must not silently turn a retryable error into a dropped
		// batch.
		{"wrapped deadlock", fmt.Errorf("db: events: %w", deadlock()), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientLockError(tc.err); got != tc.want {
				t.Fatalf("isTransientLockError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

const (
	connRetriesMetric = "statusengine_db_connection_retries_total"
	availableMetric   = "statusengine_db_available"
)

// gaugeValue is counterValue's counterpart for DBAvailable.
func gaugeValue(t *testing.T, name string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		series := family.GetMetric()
		if len(series) != 1 {
			t.Fatalf("%s: expected exactly one unlabeled series, got %d", name, len(series))
		}
		return series[0].GetGauge().GetValue()
	}
	t.Fatalf("%s: no such metric family in the default registry", name)
	return 0
}

// serverGoneAway is what the driver actually reports while mysqld is down:
// the dial fails, so database/sql hands back a net error. Note it is NOT a
// *mysql.MySQLError with 2006 - that number is libmysqlclient's and this
// driver never produces it.
func serverGoneAway() error {
	return &net.OpError{
		Op:   "dial",
		Net:  "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3306},
		Err:  errors.New("connect: connection refused"),
	}
}

// TestFlushWaitsOutAnUnreachableServer is the measured case: a MySQL
// restart used to cost every batch that fell inside it (29,400 of 150,000
// events for a five-second outage). The batch must survive instead.
func TestFlushWaitsOutAnUnreachableServer(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnError(serverGoneAway())
	mock.ExpectExec(`^INSERT INTO events`).WillReturnError(mysql.ErrInvalidConn)
	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 1))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	inserter.buffer = append(inserter.buffer, row{id: 1, name: "first"})

	before := counterValue(t, connRetriesMetric)
	beforeLockRetries := counterValue(t, retriesMetric)

	if err := inserter.flushBuffer(context.Background()); err != nil {
		t.Fatalf("flushBuffer: expected the batch to be written once MySQL returned, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
	if got := counterValue(t, connRetriesMetric) - before; got != 2 {
		t.Fatalf("%s rose by %v, want 2", connRetriesMetric, got)
	}
	if got := inserter.processed.Load(); got != 1 {
		t.Fatalf("processed = %d, want the row counted exactly once", got)
	}
	// Back to 1 on recovery, or the alert would stay latched after the
	// outage it was meant to report.
	if got := gaugeValue(t, availableMetric); got != 1 {
		t.Fatalf("%s = %v after recovery, want 1", availableMetric, got)
	}
	// The lock-retry counter must not move: the two are separate signals,
	// and a MySQL restart showing up as "deadlocks" would be misleading.
	if got := counterValue(t, retriesMetric) - beforeLockRetries; got != 0 {
		t.Fatalf("%s rose by %v during a connection outage, want 0", retriesMetric, got)
	}
}

// TestUnreachableServerMarksTheWorkerUnavailable proves the gauge goes to
// 0 *during* the outage, not only afterwards - a gauge that is only ever
// observed as 1 is useless for alerting.
func TestUnreachableServerMarksTheWorkerUnavailable(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	observed := make(chan float64, 1)
	mock.ExpectExec(`^INSERT INTO events`).WillReturnError(serverGoneAway())
	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 1))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	inserter.buffer = append(inserter.buffer, row{id: 1, name: "first"})

	done := make(chan error, 1)
	go func() { done <- inserter.flushBuffer(context.Background()) }()

	// The first backoff is connRetryInitialBackoff, so there is a window
	// between the failed attempt and the successful one in which the gauge
	// must already read 0.
	go func() {
		time.Sleep(connRetryInitialBackoff / 2)
		observed <- gaugeValue(t, availableMetric)
	}()

	if err := <-done; err != nil {
		t.Fatalf("flushBuffer: %v", err)
	}
	if got := <-observed; got != 0 {
		t.Fatalf("%s = %v during the outage, want 0", availableMetric, got)
	}
	if got := gaugeValue(t, availableMetric); got != 1 {
		t.Fatalf("%s = %v after recovery, want 1", availableMetric, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

// TestUnreachableServerStopsRetryingOnShutdown is the bound on the
// unbounded retry: waiting forever is correct while the worker is running,
// but Shutdown flushes under a 5s context and must not be held past it.
func TestUnreachableServerStopsRetryingOnShutdown(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	// Far more attempts than the context allows; the point is that the
	// loop stops on its own, not that it exhausts these.
	for i := 0; i < 50; i++ {
		mock.ExpectExec(`^INSERT INTO events`).WillReturnError(serverGoneAway())
	}

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	inserter.buffer = append(inserter.buffer, row{id: 1, name: "first"})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- inserter.flushBuffer(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("flushBuffer: expected the outage to be reported once the context expired")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("flushBuffer kept retrying past its context - a shutdown would hang")
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("flushBuffer took %s to honour a 250ms context", elapsed)
	}
	if len(inserter.buffer) != 0 {
		t.Fatalf("buffer holds %d rows after giving up, want 0", len(inserter.buffer))
	}
}

// TestIsConnectionError pins the classification down. Widening it is the
// dangerous direction: an error wrongly classified as "connection lost"
// would be retried forever and stall the pipeline on a statement that can
// never succeed.
func TestIsConnectionError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"dial failure while the server is down", serverGoneAway(), true},
		{"connection broke mid-statement", mysql.ErrInvalidConn, true},
		{"stale pooled connection", driver.ErrBadConn, true},
		{"unexpected EOF", io.EOF, true},
		{"server shutting down", &mysql.MySQLError{Number: mysqlErrServerShutdown}, true},
		{"too many connections", &mysql.MySQLError{Number: mysqlErrTooManyConnections}, true},
		{"connection killed", &mysql.MySQLError{Number: mysqlErrConnectionKilled}, true},
		{"wrapped dial failure", fmt.Errorf("db: events: %w", serverGoneAway()), true},

		// The dangerous direction: none of these may stall the pipeline.
		{"duplicate entry", duplicateEntry(), false},
		{"deadlock", deadlock(), false},
		{"data too long", &mysql.MySQLError{Number: 1406}, false},
		{"context cancelled", context.Canceled, false},
		{"nil", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isConnectionError(tc.err); got != tc.want {
				t.Fatalf("isConnectionError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

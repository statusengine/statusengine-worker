// Package cleanup implements the data-retention pass that keeps the
// history tables from growing without bound: for every table it is given,
// it deletes the rows whose timestamp column is older than a configured
// number of days.
//
// It is written for a one-shot run from cron or a systemd timer rather
// than for the worker process, and it is deliberately conservative about
// how it gets there - see Run for why the deletes are batched and why the
// tables are processed one after another rather than concurrently.
package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Table describes one table to clean: the column its retention is
// measured on, and how many days of history to keep.
//
// Name and Column are compile-time constants from the schema (see
// .claude/specs/mysql_schema.sql), never user input. That matters because
// SQL placeholders cannot stand in for identifiers, so both are
// interpolated into the DELETE statement - a caller that ever wires these
// to configurable strings would be introducing an injection point that
// does not exist today.
type Table struct {
	Name   string
	Column string

	// Days of history to keep. Zero disables cleanup of this table
	// entirely, matching the legacy PHP worker's "Set 0 to disable
	// automatic cleanup of a particular table".
	Days int
}

// Options tunes how aggressively Run deletes.
type Options struct {
	// BatchSize is the LIMIT on each DELETE statement. Must be >= 1.
	BatchSize int

	// Pause is how long to wait between two batches of the same table.
	// Zero means no pause.
	Pause time.Duration

	// Now returns the current time, and exists so tests can pin the
	// cutoff. Defaults to time.Now.
	Now func() time.Time
}

// Run cleans every table in tables, in the order given, and returns the
// errors of the tables that failed joined together.
//
// A failing table does not abort the run: a database where one table is
// missing (statusengine_perfdata simply does not exist on installations
// that route perfdata to Graphite only) or temporarily locked must not
// stop the other thirteen from being cleaned. The caller learns about it
// from the returned error and can exit non-zero.
//
// The tables are processed sequentially, never concurrently. This runs
// against a database the worker keeps writing to, and several parallel
// delete streams would multiply the I/O pressure and lock contention
// without finishing any sooner - the bottleneck is the disk, not the
// number of statements in flight.
//
// Cancelling ctx stops the run cleanly between two batches rather than
// aborting mid-statement. That is not an error: whatever was deleted
// stays deleted and the next run picks up where this one left off, so a
// SIGTERM during a long pass is a normal, resumable outcome.
func Run(ctx context.Context, db *sql.DB, tables []Table, opts Options) error {
	if opts.BatchSize < 1 {
		return fmt.Errorf("cleanup: batch size must be >= 1, got %d", opts.BatchSize)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	var errs []error
	for _, table := range tables {
		if ctx.Err() != nil {
			slog.Info("cleanup: interrupted, stopping", "next_table", table.Name)
			break
		}

		if table.Days <= 0 {
			slog.Info("cleanup: table disabled, skipping", "table", table.Name)
			continue
		}

		if err := cleanTable(ctx, db, table, opts, now()); err != nil {
			// Logged here as well as returned: on a 14-table run the
			// operator wants to see which one broke at the moment it
			// breaks, not only in a joined error at the very end.
			slog.Error("cleanup: table failed", "table", table.Name, "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", table.Name, err))
		}
	}

	return errors.Join(errs...)
}

// cleanTable deletes table's rows older than the cutoff derived from now,
// in batches of opts.BatchSize, until a batch comes back short - which
// means there was nothing left to match.
//
// Deleting in batches rather than issuing one unbounded DELETE is the
// whole point. A single statement covering millions of rows holds one
// transaction (and its locks) open for minutes, inflates the undo log,
// and produces a binlog event that replicas then chew on in one piece.
// With a LIMIT, each batch is its own transaction that commits
// immediately, so the worker's inserts keep flowing in between.
func cleanTable(ctx context.Context, db *sql.DB, table Table, opts Options, now time.Time) error {
	// AddDate rather than Add(24h * days): over a DST boundary the two
	// disagree by an hour, and "keep 60 days" is a calendar statement.
	cutoffTime := now.AddDate(0, 0, -table.Days)
	cutoff := cutoffTime.Unix()

	slog.Info("cleanup: starting table",
		"table", table.Name, "column", table.Column,
		"age_days", table.Days, "cutoff", cutoffTime.Format(time.RFC3339))

	// Identifiers cannot be bound as parameters, so they are interpolated
	// - safe here because both are constants from the schema (see Table).
	// The cutoff itself is bound.
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` < ? LIMIT %d",
		table.Name, table.Column, opts.BatchSize)

	var (
		started = time.Now()
		deleted int64
		batches int
	)

	for {
		if ctx.Err() != nil {
			slog.Info("cleanup: table interrupted",
				"table", table.Name, "deleted", deleted, "batches", batches,
				"duration", time.Since(started).Round(time.Millisecond))
			return nil
		}

		res, err := db.ExecContext(ctx, query, cutoff)
		if err != nil {
			if ctx.Err() != nil {
				// The statement was cut short by our own shutdown, not
				// by a database problem - report it as the clean stop
				// it is instead of a failure the timer would alert on.
				slog.Info("cleanup: table interrupted",
					"table", table.Name, "deleted", deleted, "batches", batches,
					"duration", time.Since(started).Round(time.Millisecond))
				return nil
			}
			return fmt.Errorf("delete batch %d: %w", batches+1, err)
		}

		// Free: the count comes back in the statement's OK packet, so
		// reporting it costs nothing. There is deliberately no COUNT(*)
		// anywhere - that would scan the very rows we are about to
		// delete, just to print a number.
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("rows affected after batch %d: %w", batches+1, err)
		}

		deleted += affected
		batches++

		// A short batch means the DELETE ran out of matching rows, so
		// this was the last one.
		if affected < int64(opts.BatchSize) {
			break
		}

		if opts.Pause > 0 {
			timer := time.NewTimer(opts.Pause)
			select {
			case <-ctx.Done():
				timer.Stop()
				slog.Info("cleanup: table interrupted",
					"table", table.Name, "deleted", deleted, "batches", batches,
					"duration", time.Since(started).Round(time.Millisecond))
				return nil
			case <-timer.C:
			}
		}
	}

	slog.Info("cleanup: table finished",
		"table", table.Name, "deleted", deleted, "batches", batches,
		"duration", time.Since(started).Round(time.Millisecond))

	return nil
}

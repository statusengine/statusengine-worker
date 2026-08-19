package db

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// captureLogs swaps in a JSON handler at the given level for the duration
// of fn and returns one decoded record per line. JSON rather than text so
// the assertions read fields instead of substrings.
func captureLogs(t *testing.T, level slog.Level, fn func()) []map[string]any {
	t.Helper()

	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level})))
	defer slog.SetDefault(original)

	fn()

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

// TestFlushesAreNotLoggedAtInfo is the guard for a log volume problem
// rather than a correctness one, which is why it needs stating: a flush
// happens whenever the batch fills or the 250ms ticker expires with
// anything buffered, so at Info this line alone is tens of entries a
// second across fourteen tables. Under systemd that is a journal that
// grows by megabytes an hour and buries every line worth reading.
//
// The detail itself is not deleted, only demoted - the second half of this
// test insists it is still there at Debug, so "quieter" cannot quietly
// become "gone".
func TestFlushesAreNotLoggedAtInfo(t *testing.T) {
	const flushes = 5

	run := func(level slog.Level) []map[string]any {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		for i := 0; i < flushes; i++ {
			mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 1))
		}

		inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

		return captureLogs(t, level, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			for i := 0; i < flushes; i++ {
				inserter.buffer = append(inserter.buffer, row{id: i})
				inserter.flushBuffer(ctx)
			}
		})
	}

	for _, rec := range run(slog.LevelInfo) {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "bulk insert flushed") {
			t.Fatalf("per-flush line %q is still logged at Info - "+
				"one entry per flush per table floods a systemd journal", msg)
		}
	}

	var debugFlushes int
	for _, rec := range run(slog.LevelDebug) {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "bulk insert flushed") {
			debugFlushes++
		}
	}
	if debugFlushes != flushes {
		t.Fatalf("found %d per-flush lines at Debug, want %d - the detail was dropped, not demoted",
			debugFlushes, flushes)
	}
}

// TestWriteStatsSummarisesAtInfo covers what replaced the per-flush line.
// Demoting it alone would leave an operator watching the journal with no
// evidence at all that rows are reaching MySQL, which is a worse trade
// than the noise it fixes.
func TestWriteStatsSummarisesAtInfo(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	// Three flushes of four rows each: the reported average has to be 4,
	// which a summary that counted flushes as rows (or vice versa) misses.
	const (
		flushes      = 3
		rowsPerFlush = 4
	)
	for i := 0; i < flushes; i++ {
		mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, rowsPerFlush))
	}

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	records := captureLogs(t, slog.LevelInfo, func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for i := 0; i < flushes; i++ {
			for j := 0; j < rowsPerFlush; j++ {
				inserter.buffer = append(inserter.buffer, row{id: j})
			}
			inserter.flushBuffer(ctx)
		}
		inserter.logStats()
	})

	var stats map[string]any
	for _, rec := range records {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "write stats") {
			stats = rec
		}
	}
	if stats == nil {
		t.Fatal("no Info-level summary was logged - demoting the per-flush line left nothing behind")
	}

	for field, want := range map[string]float64{
		"flushes":         flushes,
		"rows":            flushes * rowsPerFlush,
		"rows_per_flush":  rowsPerFlush,
		"total_processed": flushes * rowsPerFlush,
	} {
		if got, _ := stats[field].(float64); got != want {
			t.Errorf("summary %s = %v, want %v", field, stats[field], want)
		}
	}
}

// TestWriteStatsIsSilentWithoutTraffic is what keeps an idle worker's
// journal idle. There are fourteen inserters and most tables see no
// traffic on most installations; an unconditional summary would mean 28
// lines a minute reporting that nothing happened, which is the same
// problem in slower motion.
func TestWriteStatsIsSilentWithoutTraffic(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	records := captureLogs(t, slog.LevelInfo, func() {
		inserter.logStats()
		inserter.logStats()
	})

	for _, rec := range records {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "write stats") {
			t.Fatalf("an inserter that wrote nothing still logged %q", msg)
		}
	}
}

// TestWriteStatsResetsBetweenIntervals: the counters report one interval,
// not a running total. Without the reset every line would repeat the
// previous one's rows, and "rows in the last 30s" - the only thing a
// summary is for - would be wrong in a way that looks plausible.
func TestWriteStatsResetsBetweenIntervals(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 2))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)
	ctx := context.Background()

	flushTwoRows := func() {
		inserter.buffer = append(inserter.buffer, row{id: 1}, row{id: 2})
		inserter.flushBuffer(ctx)
	}

	records := captureLogs(t, slog.LevelInfo, func() {
		flushTwoRows()
		inserter.logStats()
		flushTwoRows()
		inserter.logStats()
	})

	var seen []float64
	for _, rec := range records {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "write stats") {
			rows, _ := rec["rows"].(float64)
			seen = append(seen, rows)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("got %d summaries, want 2", len(seen))
	}
	if seen[0] != 2 || seen[1] != 2 {
		t.Fatalf("summaries reported %v rows, want [2 2] - the interval counters are not being reset", seen)
	}
}

// TestRunLogsStatsOnShutdown: Run's interval is 30s, so without a final
// summary on the way out a worker that ran for less than that - or that
// shut down mid-interval, which is every shutdown - would report nothing
// about the rows it did write.
func TestRunLogsStatsOnShutdown(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^INSERT INTO events`).WillReturnResult(sqlmock.NewResult(0, 1))

	inserter := NewBulkInserter[row](mockDB, "events", []string{"id", "name"}, toRow)

	records := captureLogs(t, slog.LevelInfo, func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			inserter.Run(ctx)
		}()

		if err := inserter.Enqueue(context.Background(), row{id: 1}); err != nil {
			t.Errorf("enqueue: %v", err)
		}
		// Long enough for the 250ms flush ticker, far short of the 30s
		// stats ticker - so only the shutdown path can produce a summary.
		time.Sleep(400 * time.Millisecond)
		cancel()
		<-done
	})

	for _, rec := range records {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "write stats") {
			return
		}
	}
	t.Fatal("Run produced no summary on shutdown - rows written since the last tick go unreported")
}

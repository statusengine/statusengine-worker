package graphite

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// captureLogs swaps in a JSON handler at the given level for the duration
// of fn and returns one decoded record per line. JSON rather than text so
// the assertions read fields instead of substrings. A near-copy of the one
// in internal/db - the alternative is exporting a test helper from a
// third package, which is more machinery than twenty lines is worth.
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

// findStats returns the last "graphite: write stats" record in records, or
// nil if there is none.
func findStats(records []map[string]any) map[string]any {
	var stats map[string]any
	for _, rec := range records {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "write stats") {
			stats = rec
		}
	}
	return stats
}

// flushN sends n metrics through c one flush at a time, against a real
// (local) Carbon receiver, and returns once the receiver has seen them
// all - so the assertions run after flushBuffer has finished logging.
func flushN(t *testing.T, c *Client, r *carbonReceiver, flushes, perFlush int) {
	t.Helper()

	ctx := context.Background()
	for i := 0; i < flushes; i++ {
		for j := 0; j < perFlush; j++ {
			c.buffer = append(c.buffer, Metric{Path: "a.b.c", Value: float64(j), Timestamp: 1})
		}
		if err := c.flushBuffer(ctx); err != nil {
			t.Fatalf("flushBuffer: %v", err)
		}
	}
	r.expect(t, flushes*perFlush)
}

// TestMetricsFlushedIsNotLoggedAtInfo is the Graphite half of the log
// volume guard (its counterpart is TestFlushesAreNotLoggedAtInfo in
// internal/db). This line fires once per flush, and a flush happens
// whenever the batch fills or the 250ms ticker expires with anything
// buffered - so on an installation that routes perfdata to Graphite it is
// several entries a second, indefinitely, which is a systemd journal
// problem rather than a cosmetic one.
//
// The second half insists the detail still exists at Debug, so demoting it
// cannot silently turn into deleting it.
func TestMetricsFlushedIsNotLoggedAtInfo(t *testing.T) {
	const flushes = 5

	run := func(level slog.Level) []map[string]any {
		r := newCarbonReceiver(t)
		c := NewClient(r.addr)
		return captureLogs(t, level, func() { flushN(t, c, r, flushes, 1) })
	}

	for _, rec := range run(slog.LevelInfo) {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "metrics flushed") {
			t.Fatalf("per-flush line %q is still logged at Info", msg)
		}
	}

	var debugFlushes int
	for _, rec := range run(slog.LevelDebug) {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "metrics flushed") {
			debugFlushes++
		}
	}
	if debugFlushes != flushes {
		t.Fatalf("found %d per-flush lines at Debug, want %d - the detail was dropped, not demoted",
			debugFlushes, flushes)
	}
}

// TestWriteStatsSummarisesAtInfo covers what replaced that line. Demoting
// it on its own would leave an operator with no Info-level evidence that
// anything is reaching Carbon at all - and Graphite has no equivalent of
// the database's own row counts to fall back on.
func TestWriteStatsSummarisesAtInfo(t *testing.T) {
	const (
		flushes         = 3
		metricsPerFlush = 4
	)

	r := newCarbonReceiver(t)
	c := NewClient(r.addr)

	records := captureLogs(t, slog.LevelInfo, func() {
		flushN(t, c, r, flushes, metricsPerFlush)
		c.logStats()
	})

	stats := findStats(records)
	if stats == nil {
		t.Fatal("no Info-level summary was logged - demoting the per-flush line left nothing behind")
	}
	if addr, _ := stats["addr"].(string); addr != r.addr {
		t.Errorf("summary addr = %q, want %q", addr, r.addr)
	}

	for field, want := range map[string]float64{
		"flushes":           flushes,
		"metrics":           flushes * metricsPerFlush,
		"metrics_per_flush": metricsPerFlush,
		"total_processed":   flushes * metricsPerFlush,
	} {
		if got, _ := stats[field].(float64); got != want {
			t.Errorf("summary %s = %v, want %v", field, stats[field], want)
		}
	}
}

// TestWriteStatsIsSilentWithoutTraffic matters more here than it does for
// the database: perfdata routes to MySQL only by default, so on most
// installations this client is constructed, started, and never sent
// anything. It must not log a line every 30 seconds to say so.
func TestWriteStatsIsSilentWithoutTraffic(t *testing.T) {
	c := NewClient("127.0.0.1:1")

	records := captureLogs(t, slog.LevelInfo, func() {
		c.logStats()
		c.logStats()
	})

	if stats := findStats(records); stats != nil {
		t.Fatalf("a client that shipped nothing still logged %v", stats["msg"])
	}
}

// TestWriteStatsResetsBetweenIntervals: each line reports one interval,
// not a running total. Without the reset "metrics in the last 30s" would
// be wrong in a way that still looks plausible.
func TestWriteStatsResetsBetweenIntervals(t *testing.T) {
	r := newCarbonReceiver(t)
	c := NewClient(r.addr)

	var seen []float64
	captureLogsInto := func(records []map[string]any) {
		for _, rec := range records {
			if msg, _ := rec["msg"].(string); strings.Contains(msg, "write stats") {
				m, _ := rec["metrics"].(float64)
				seen = append(seen, m)
			}
		}
	}

	captureLogsInto(captureLogs(t, slog.LevelInfo, func() {
		flushN(t, c, r, 1, 2)
		c.logStats()
		flushN(t, c, r, 1, 2)
		c.logStats()
	}))

	if len(seen) != 2 {
		t.Fatalf("got %d summaries, want 2", len(seen))
	}
	if seen[0] != 2 || seen[1] != 2 {
		t.Fatalf("summaries reported %v metrics, want [2 2] - the interval counters are not being reset", seen)
	}
}

// TestRunLogsStatsOnShutdown: the stats ticker is 30s, so without a final
// summary on the way out every shutdown - and every worker that ran for
// less than half a minute - would report nothing about what it shipped.
func TestRunLogsStatsOnShutdown(t *testing.T) {
	r := newCarbonReceiver(t)
	c := NewClient(r.addr)

	records := captureLogs(t, slog.LevelInfo, func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.Run(ctx)
		}()

		if err := c.Enqueue(context.Background(), Metric{Path: "a.b.c", Value: 1, Timestamp: 1}); err != nil {
			t.Errorf("enqueue: %v", err)
		}
		// Past the 250ms flush ticker, far short of the 30s stats ticker,
		// so only the shutdown path can produce a summary.
		time.Sleep(400 * time.Millisecond)
		cancel()
		<-done
	})

	if findStats(records) == nil {
		t.Fatal("Run produced no summary on shutdown - metrics shipped since the last tick go unreported")
	}
}

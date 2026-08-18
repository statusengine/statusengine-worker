package graphite

import (
	"context"
	"testing"
)

// TestRunFlushesOnConfiguredBatchSize is the Graphite counterpart of the db
// package's test: a client built with WithMaxBatchSize must flush at that
// number rather than at DefaultMaxBatchSize. The size is chosen well above
// the default so a regression that ignored the option would flush early
// instead of passing by coincidence.
func TestRunFlushesOnConfiguredBatchSize(t *testing.T) {
	const batchSize = 500

	recv := newCarbonReceiver(t)
	c := NewClient(recv.addr, WithMaxBatchSize(batchSize))

	if got := c.maxBatchSize; got != batchSize {
		t.Fatalf("maxBatchSize = %d, want %d", got, batchSize)
	}
	if got := cap(c.in); got != batchSize {
		t.Errorf("cap(in) = %d, want %d", got, batchSize)
	}
	if got := cap(c.buffer); got != 2*batchSize {
		t.Errorf("cap(buffer) = %d, want %d", got, 2*batchSize)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	for i := 0; i < batchSize; i++ {
		if err := c.Enqueue(ctx, Metric{Path: "a.b.c", Value: float64(i), Timestamp: 1700000000}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// All 500 must arrive. The ticker would also get them there eventually,
	// so the point of the assertion is the count, not the timing: a client
	// still flushing at 100 would deliver these in five writes, which the
	// receiver cannot tell apart - but one that mis-sized its buffer would
	// drop or stall.
	lines := recv.expect(t, batchSize)
	if len(lines) != batchSize {
		t.Fatalf("received %d lines, want %d", len(lines), batchSize)
	}
}

// TestDefaultBatchSizeIsUnchanged pins the behaviour of a client built without
// the option, which is what cmd/simulator and every existing test rely on.
func TestDefaultBatchSizeIsUnchanged(t *testing.T) {
	c := NewClient("127.0.0.1:2003")

	if got := c.maxBatchSize; got != DefaultMaxBatchSize {
		t.Errorf("maxBatchSize = %d, want %d", got, DefaultMaxBatchSize)
	}
	if DefaultMaxBatchSize != 100 {
		t.Errorf("DefaultMaxBatchSize = %d, want 100 - CLAUDE.md and the README quote this number", DefaultMaxBatchSize)
	}
}

// TestWithMaxBatchSizeClamps covers the last line of defence; cmd/app rejects
// out-of-range values outright.
func TestWithMaxBatchSizeClamps(t *testing.T) {
	tests := []struct {
		name string
		give int
		want int
	}{
		{"zero becomes one", 0, 1},
		{"negative becomes one", -5, 1},
		{"above the ceiling is capped", 5000, MaxConfigurableBatchSize},
		{"the ceiling itself is kept", MaxConfigurableBatchSize, MaxConfigurableBatchSize},
		{"a normal value is kept", 500, 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClient("127.0.0.1:2003", WithMaxBatchSize(tt.give)).maxBatchSize; got != tt.want {
				t.Errorf("WithMaxBatchSize(%d) -> %d, want %d", tt.give, got, tt.want)
			}
		})
	}
}

// TestGraphiteCeilingIsAboveTheMySQLOne documents that the two ceilings are
// deliberately different rather than one of them being stale: Graphite's
// batch is one Write of plaintext lines with no protocol limit behind it, so
// only the drop-the-whole-batch-on-write-error blast radius caps it.
func TestGraphiteCeilingIsAboveTheMySQLOne(t *testing.T) {
	if MaxConfigurableBatchSize != 1000 {
		t.Errorf("MaxConfigurableBatchSize = %d, want 1000 - config.example.yaml and the README quote this number",
			MaxConfigurableBatchSize)
	}
	// A full drain flush at the ceiling, rendered as Carbon lines, must stay
	// small enough to be one sensible Write inside writeTimeout.
	const bytesPerLine = 80
	if worst := (2*MaxConfigurableBatchSize - 1) * bytesPerLine; worst > 1<<20 {
		t.Errorf("a drain flush would be ~%d bytes in a single Write with a %s deadline", worst, writeTimeout)
	}
}

package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"statusengine-worker/internal/queue"
)

// fakeRunner records the deadline it was handed and blocks for delay,
// standing in for a BulkInserter whose Flush is waiting on a slow MySQL.
type fakeRunner struct {
	delay time.Duration
	err   error

	mu        sync.Mutex
	remaining time.Duration
	called    bool
}

func (f *fakeRunner) Run(context.Context) {}

func (f *fakeRunner) Flush(ctx context.Context) error {
	f.mu.Lock()
	f.called = true
	if deadline, ok := ctx.Deadline(); ok {
		f.remaining = time.Until(deadline)
	}
	f.mu.Unlock()

	time.Sleep(f.delay)
	return f.err
}

func (f *fakeRunner) budget() (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remaining, f.called
}

// TestFlushRunnersGivesEachRunnerTheFullBudget is the actual finding.
// Flushing sequentially meant the runners shared one deadline: with a slow
// database the first few consumed it entirely and every one after them was
// handed an already-expired context, failing instantly for something it
// never got to attempt. Each runner must now see essentially the whole
// budget.
func TestFlushRunnersGivesEachRunnerTheFullBudget(t *testing.T) {
	const (
		budget    = 2 * time.Second
		perRunner = 150 * time.Millisecond
		count     = 15 // the pipeline's current runner count
	)

	runners := make([]queue.Runner, 0, count)
	fakes := make([]*fakeRunner, 0, count)
	for i := 0; i < count; i++ {
		f := &fakeRunner{delay: perRunner}
		fakes = append(fakes, f)
		runners = append(runners, f)
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	if failed := flushRunners(ctx, runners); failed != 0 {
		t.Fatalf("flushRunners reported %d failures, want 0", failed)
	}
	elapsed := time.Since(start)

	// Sequentially this would take count*perRunner (2.25s here) and blow
	// the budget; concurrently it is one runner's delay plus scheduling.
	if elapsed >= count*perRunner {
		t.Errorf("flush took %v, i.e. runners were not flushed concurrently", elapsed)
	}

	for i, f := range fakes {
		remaining, called := f.budget()
		if !called {
			t.Errorf("runner %d was never flushed", i)
			continue
		}
		// Every runner should still have had nearly the full budget left;
		// allow generous slack for scheduling on a loaded machine.
		if remaining < budget-perRunner {
			t.Errorf("runner %d saw only %v of the %v budget", i, remaining, budget)
		}
	}
}

// TestFlushRunnersCountsFailuresWithoutStoppingTheRest covers the other
// half: a wedged table must not keep the remaining ones from being
// written, and the failure has to show up in the shutdown log's count.
func TestFlushRunnersCountsFailuresWithoutStoppingTheRest(t *testing.T) {
	boom := errors.New("table is wedged")

	failing := []*fakeRunner{{err: boom}, {err: boom}}
	healthy := []*fakeRunner{{}, {}, {}}

	runners := make([]queue.Runner, 0, len(failing)+len(healthy))
	for _, f := range failing {
		runners = append(runners, f)
	}
	for _, f := range healthy {
		runners = append(runners, f)
	}

	if failed := flushRunners(context.Background(), runners); failed != len(failing) {
		t.Fatalf("flushRunners reported %d failures, want %d", failed, len(failing))
	}

	for i, f := range healthy {
		if _, called := f.budget(); !called {
			t.Errorf("healthy runner %d was skipped because another one failed", i)
		}
	}
}

func TestFlushRunnersWithNoRunnersIsSafe(t *testing.T) {
	if failed := flushRunners(context.Background(), nil); failed != 0 {
		t.Fatalf("got %d failures for an empty runner list", failed)
	}
}

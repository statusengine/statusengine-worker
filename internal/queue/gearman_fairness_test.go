package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	gearmanClient "github.com/mikespook/gearman-go/client"
	gearman "github.com/mikespook/gearman-go/worker"
)

// TestOneQueueCannotConsumeTheWholeJobBudget is the reason this consumer
// opens one connection per queue instead of registering every function on
// a single worker.
//
// The library's Work loop is one goroutine ranging over one channel fed by
// every agent, and handleInPack does `worker.limit <- true` inside it
// (worker.go:198). That send blocks once the concurrency cap is reached -
// and while it blocks, the loop reads nothing further from any agent, for
// any function. So the cap is not a per-queue share, it is a single budget
// that whichever queue has a backlog will hold entirely.
//
// That is what makes it a liveness problem rather than a throughput one. A
// Handler does not return quickly under load: it blocks in Enqueue for as
// long as MySQL needs (CLAUDE.md rule 3), so a status backlog parks every
// slot on MySQL's write rate and notifications, downtimes and core
// restarts are not merely slow, they are not looked at at all.
//
// Note what this test does *not* depend on: gearmand's job-assignment
// policy. `--round-robin` changes which queue the server offers next, and
// would spread the budget around - but the coupling itself lives in this
// process, so it would soften the symptom rather than remove it. One
// worker per queue gives each its own Work loop and its own budget, which
// is why this test is deterministic and needs no server flag.
func TestOneQueueCannotConsumeTheWholeJobBudget(t *testing.T) {
	const perQueueLimit = 2

	// Unique per run: this is a shared dev job server, and a leftover job
	// from a killed earlier run must not be able to satisfy the assertion.
	run := time.Now().UnixNano()
	busyFn := fmt.Sprintf("queue_pkg_test_busy_%d", run)
	quietFn := fmt.Sprintf("queue_pkg_test_quiet_%d", run)

	busyStarted := make(chan struct{}, perQueueLimit)
	release := make(chan struct{})
	quietHandled := make(chan struct{}, 1)

	router := Router{
		// Stands in for a handler blocked in Enqueue behind MySQL. Held
		// rather than merely slow, so the assertion below cannot pass by
		// accident on a fast machine.
		busyFn: func(_ context.Context, _ []byte) error {
			select {
			case busyStarted <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
		quietFn: func(_ context.Context, _ []byte) error {
			select {
			case quietHandled <- struct{}{}:
			default:
			}
			return nil
		},
	}

	cli, err := gearmanClient.New(gearmanClient.Network, gearmanAddr)
	if err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}
	defer cli.Close()

	consumer := NewGearmanConsumer(gearmanAddr, router, perQueueLimit)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := consumer.Start(ctx); err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}
	// LIFO: release the held handlers first, so Stop's handlerWG.Wait can
	// return and no job is left queued on the shared dev server.
	defer consumer.Stop()
	defer close(release)

	// Exactly enough to exhaust the budget: the loop spawns each handler
	// and only then sends its token, so the cap-th assignment leaves cap
	// handlers running with the loop parked on the send.
	for i := 0; i < perQueueLimit; i++ {
		if _, err := cli.DoBg(busyFn, []byte(`{}`), gearmanClient.JobNormal); err != nil {
			t.Fatalf("submit busy job %d: %v", i, err)
		}
	}
	for i := 0; i < perQueueLimit; i++ {
		select {
		case <-busyStarted:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d busy handlers ever started", i, perQueueLimit)
		}
	}

	// Submitted after the budget is provably exhausted, so this job's fate
	// is the entire claim.
	if _, err := cli.DoBg(quietFn, []byte(`{}`), gearmanClient.JobNormal); err != nil {
		t.Fatalf("submit quiet job: %v", err)
	}

	select {
	case <-quietHandled:
	case <-time.After(3 * time.Second):
		t.Fatal("the quiet queue was never served while the busy queue held every job slot - " +
			"one queue's backlog is blocking every other queue")
	}
}

// TestCloseWorkersDrainsInParallel guards the one way the per-queue split
// could make shutdown worse than it was.
//
// Close waits for the jobs already running on its worker before dropping
// the connections - that draining is what stops a restart from handing the
// same jobs out twice (see Stop's comment) - and it is bounded by the
// fork's DrainTimeout. With one connection that bound was the shutdown's
// worst case. With one per queue, closing them one after another would
// make it len(workers) * DrainTimeout: twelve queues with a stuck handler
// would turn a 30-second bound into a six-minute hang, and a service
// manager would escalate to SIGKILL long before that - losing exactly the
// buffered rows the graceful shutdown exists to flush.
//
// Measured against a real job server with genuinely stuck handlers,
// because that is the only state in which Close actually spends its
// timeout: a handler that returns lets Close finish early, and then
// sequential and parallel are indistinguishable no matter how the timings
// are arranged.
func TestCloseWorkersDrainsInParallel(t *testing.T) {
	const (
		queues       = 6
		drainTimeout = 300 * time.Millisecond
	)

	cli, err := gearmanClient.New(gearmanClient.Network, gearmanAddr)
	if err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}
	defer cli.Close()

	original := gearman.DrainTimeout
	gearman.DrainTimeout = drainTimeout
	defer func() { gearman.DrainTimeout = original }()

	run := time.Now().UnixNano()
	release := make(chan struct{})
	started := make(chan struct{}, queues)

	fnNames := make([]string, queues)

	// LIFO: release the stuck handlers, then pick up the jobs gearmand
	// re-queued because Close never acknowledged them. Released only once
	// the measurement is over - while they are stuck, every Close must
	// spend the full DrainTimeout, which is what makes the difference
	// between sequential and parallel observable at all.
	defer func() { drainLeftoverJobs(t, fnNames) }()
	defer close(release)

	workers := make([]*gearman.Worker, 0, queues)
	for i := range fnNames {
		fnNames[i] = fmt.Sprintf("queue_pkg_test_drain_%d_%d", run, i)

		w := gearman.New(1)
		if err := w.AddServer(gearman.Network, gearmanAddr); err != nil {
			skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
		}
		w.ErrorHandler = func(error) {} // the drain timeout is expected here
		if err := w.AddFunc(fnNames[i], func(gearman.Job) ([]byte, error) {
			started <- struct{}{}
			<-release
			return nil, nil
		}, 0); err != nil {
			t.Fatalf("add func: %v", err)
		}
		if err := w.Ready(); err != nil {
			t.Fatalf("ready: %v", err)
		}
		go w.Work()
		workers = append(workers, w)
	}

	for i, fn := range fnNames {
		if _, err := cli.DoBg(fn, []byte(`{}`), gearmanClient.JobNormal); err != nil {
			t.Fatalf("submit job %d: %v", i, err)
		}
	}
	for i := 0; i < queues; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d handlers ever started", i, queues)
		}
	}

	start := time.Now()
	closeWorkers(workers)
	elapsed := time.Since(start)

	// Parallel costs one timeout, sequential costs six. Half the
	// sequential cost is a bound generous enough not to be flaky on a
	// loaded machine and still far below what serializing would produce.
	if limit := queues * drainTimeout / 2; elapsed > limit {
		t.Fatalf("closing %d workers took %s, want under %s - "+
			"they are being drained one after another, not at the same time",
			queues, elapsed, limit)
	}
}

// drainLeftoverJobs consumes and acknowledges whatever fnNames still hold.
//
// The point of the test above is that Close gives up on handlers that are
// still stuck, which means their jobs are never acknowledged and gearmand
// re-queues every one of them. This runs against a shared dev job server,
// so leaving them there would slowly fill it with work under function
// names nothing will ever register again - invisible to this suite and
// confusing to whoever looks at `gearadmin --status` next.
func drainLeftoverJobs(t *testing.T, fnNames []string) {
	t.Helper()

	drained := make(chan struct{}, len(fnNames))
	w := gearman.New(len(fnNames))
	if err := w.AddServer(gearman.Network, gearmanAddr); err != nil {
		t.Logf("could not drain leftover jobs: %v", err)
		return
	}
	w.ErrorHandler = func(error) {}
	for _, fn := range fnNames {
		if err := w.AddFunc(fn, func(gearman.Job) ([]byte, error) {
			drained <- struct{}{}
			return nil, nil
		}, 0); err != nil {
			t.Logf("could not drain leftover jobs: %v", err)
			return
		}
	}
	if err := w.Ready(); err != nil {
		t.Logf("could not drain leftover jobs: %v", err)
		return
	}
	go w.Work()
	defer w.Close()

	// Best-effort: a job that does not come back is not worth failing a
	// test that has already made its point, but it is worth saying so.
	for i := range fnNames {
		select {
		case <-drained:
		case <-time.After(5 * time.Second):
			t.Logf("only %d of %d leftover jobs came back for draining", i, len(fnNames))
			return
		}
	}
}

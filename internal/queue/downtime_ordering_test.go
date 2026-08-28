package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	gearmanClient "github.com/mikespook/gearman-go/client"
)

// TestInOrderQueuesAreExactlyTheOnesThatNeedIt states the rule the whole
// serialization rests on, because the cost of getting it wrong runs in both
// directions and neither shows up as a failing test anywhere else.
//
// Adding a queue that does not need it silently throws away that queue's
// throughput headroom. Leaving out one that does costs data: the messages
// on such a queue are only meaningful in sequence, and the writes that
// implement them (a bare UPDATE, a DELETE) report success while doing
// nothing at all when they arrive early.
//
// statusngin_downtimes is the only one because it is the only queue whose
// messages refer to a row an *earlier message* was supposed to create.
// Every other queue carries self-contained events - a check result, a
// notification, a state change - written as an insert or an upsert that
// stands on its own. The two status queues come closest, since they upsert
// one row per object and an older snapshot could in principle overwrite a
// newer one, but each is complete in itself and the next check interval
// corrects it; a downtime's lost START is never re-sent by anything.
func TestInOrderQueuesAreExactlyTheOnesThatNeedIt(t *testing.T) {
	if !RequiresInOrderProcessing(QueueDowntimes) {
		t.Errorf("%s must be processed in order: its ADD/START/STOP/DELETE build on one another, "+
			"and START/STOP are UPDATEs that silently do nothing if they run before the ADD", QueueDowntimes)
	}

	for queueName := range inOrderQueues {
		if queueName != QueueDowntimes {
			t.Errorf("%s was added to inOrderQueues: serializing a queue costs its concurrency, "+
				"so it needs the same justification statusngin_downtimes has - messages that are "+
				"only meaningful in sequence", queueName)
		}
	}
}

// TestDowntimeQueueIsHandledOneAtATime is the regression test for six
// downtimes corrupted across a single job-server outage.
//
// The Gearman consumer dispatches up to -gearman-max-concurrent-jobs-per-queue
// handlers at once, and a downtime's ADD, START, STOP and DELETE arrive as
// separate jobs on one queue. Live traffic spaces them minutes apart so they
// never overlap; a backlog does not, and then START runs before ADD, its
// UPDATE matches nothing, and the downtime is recorded as never having
// started - with no error and no log line.
//
// Asserted from the handler's own side rather than through MySQL on
// purpose: what has to hold is that two handlers never run at the same
// time, and a database assertion would pass just as well on a run where the
// race happened not to be lost.
func TestDowntimeQueueIsHandledOneAtATime(t *testing.T) {
	// Well above 1, so a consumer that ignored the rule would be caught
	// rather than accidentally serialized by a low cap.
	const perQueueLimit = 8

	var mu sync.Mutex
	concurrent, maxConcurrent := 0, 0
	handled := make(chan struct{}, 64)

	// Registered under the real queue name: that name is the input to
	// RequiresInOrderProcessing, so a test function name would exercise the
	// wrong branch.
	router := Router{
		QueueDowntimes: func(_ context.Context, _ []byte) error {
			mu.Lock()
			concurrent++
			if concurrent > maxConcurrent {
				maxConcurrent = concurrent
			}
			mu.Unlock()

			// Long enough that genuinely concurrent dispatch overlaps here
			// with room to spare, short enough to keep the test quick.
			time.Sleep(25 * time.Millisecond)

			mu.Lock()
			concurrent--
			mu.Unlock()

			handled <- struct{}{}
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
	defer consumer.Stop()

	// Submitted in one burst with nothing draining them yet, which is what
	// an outage leaves behind at the job server.
	const jobs = 24
	for i := 0; i < jobs; i++ {
		if _, err := cli.DoBg(QueueDowntimes, []byte(`{}`), gearmanClient.JobNormal); err != nil {
			t.Fatalf("submit downtime job %d: %v", i, err)
		}
	}

	for i := 0; i < jobs; i++ {
		select {
		case <-handled:
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d of %d downtime jobs were handled", i, jobs)
		}
	}

	mu.Lock()
	got := maxConcurrent
	mu.Unlock()

	if got != 1 {
		t.Errorf("%d downtime handlers ran at once, want 1 - a downtime's ADD/START/STOP can then "+
			"overtake each other, and START's UPDATE silently does nothing when it wins", got)
	}
}

// TestOtherQueuesKeepTheirConcurrency is the other half: the serialization
// must apply to the downtime queue and nothing else. Without this, capping
// every queue at 1 would satisfy the test above while quietly undoing the
// per-queue budget the consumer exists to provide.
func TestOtherQueuesKeepTheirConcurrency(t *testing.T) {
	const perQueueLimit = 4

	c := NewGearmanConsumer(gearmanAddr, Router{}, perQueueLimit)

	if got := c.jobLimitFor(QueueDowntimes); got != 1 {
		t.Errorf("job limit for %s = %d, want 1", QueueDowntimes, got)
	}
	for _, queueName := range []string{
		QueueHostStatus, QueueServiceStatus, QueueHostChecks, QueueServiceChecks,
		QueueServicePerfdata, QueueStateChanges, QueueLogEntries, QueueNotifications,
		QueueContactNotificationMethod, QueueAcknowledgements, QueueCoreRestart,
	} {
		if got := c.jobLimitFor(queueName); got != perQueueLimit {
			t.Errorf("job limit for %s = %d, want the configured %d", queueName, got, perQueueLimit)
		}
	}
}

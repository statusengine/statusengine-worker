package websocket

import (
	"context"
	"testing"
	"time"
)

// TestDispatchCountsEventsAndFrames pins the one thing batching quietly
// changes about every existing websocket metric: what a "message" is.
//
// A frame carries a whole queue job, so a dispatch loop that simply
// incremented per frame would keep working, keep passing every other test
// in this package, and silently start reporting numbers an order of
// magnitude below the events actually delivered - which is exactly the
// comparison these counters exist for (against db_events_written_total and
// the queue's own counts). The frame count is a separate series, so the
// average batch size stays readable from two plain counters:
//
//	rate(websocket_messages_broadcasted_total[1m])
//	  / rate(websocket_frames_sent_total[1m])
func TestDispatchCountsEventsAndFrames(t *testing.T) {
	const (
		broadcastMetric = "statusengine_websocket_messages_broadcasted_total"
		framesMetric    = "statusengine_websocket_frames_sent_total"

		frames         = 3
		eventsPerFrame = 5
	)

	// Deltas, not absolutes: the registry is process-global.
	broadcastBefore := counterValue(t, broadcastMetric)
	framesBefore := counterValue(t, framesMetric)

	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	// Buffer large enough that nothing is dropped - this test is about
	// what a delivered frame counts as, not about the drop path.
	client := newTestClient(hub, frames)

	for i := 0; i < frames; i++ {
		hub.Publish("statusngin_hoststatus", []byte(`[{},{},{},{},{}]`), eventsPerFrame)
	}

	// Draining the client is how we know dispatch has run for every frame;
	// Publish itself only hands them to the Hub's inbound buffer.
	for i := 0; i < frames; i++ {
		select {
		case <-client.send:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for frame #%d", i)
		}
	}

	// Events on one counter, frames on the other: a dispatch loop that
	// incremented per frame everywhere would fail the first of these, and
	// one that added the event count everywhere would fail the second.
	waitForCounter(t, broadcastMetric, broadcastBefore, frames*eventsPerFrame)
	waitForCounter(t, framesMetric, framesBefore, frames)

}

// waitForCounter polls one unlabeled counter until its delta from before
// reaches want, reporting what it actually saw if it never does.
//
// Polling rather than asserting once: dispatch runs in the Hub's own
// goroutine, so there is no synchronous moment at which the counter is
// known to be final. The Hub's plain uint64 stat fields would answer the
// same question directly, but reading them from the test goroutine is a
// data race - they are documented as belonging to Run - and -race is right
// to reject it. The registry is the supported way to observe this from
// outside, and it is also what a scrape sees.
func waitForCounter(t *testing.T, name string, before, want float64) {
	t.Helper()

	var got float64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got = counterValue(t, name) - before; got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s advanced by %v, want %v", name, got, want)
}

// TestDroppedFrameCostsItsWholeBatch covers the other half: when a frame
// is dropped for a slow client, the client lost every event in it, and the
// counter has to say so. A per-frame increment would report 1 where 5
// events went missing, understating the loss by the batch size - which is
// precisely the number an operator is trying to judge.
func TestDroppedFrameCostsItsWholeBatch(t *testing.T) {
	const (
		droppedMetric  = "statusengine_websocket_messages_dropped_total"
		eventsPerFrame = 5
		frames         = 4
	)

	before := counterValue(t, droppedMetric)

	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	// Capacity 1 and never drained: the first frame is accepted, every
	// one after it is dropped.
	newTestClient(hub, 1)

	for i := 0; i < frames; i++ {
		hub.Publish("statusngin_hoststatus", []byte(`[{},{},{},{},{}]`), eventsPerFrame)
	}

	waitForCounter(t, droppedMetric, before, (frames-1)*eventsPerFrame)
}

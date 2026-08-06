// Command publisher sends synthetic test events for a single queue straight
// to a real Gearman job server, exercising the full production ingestion
// path (Gearman -> GearmanConsumer -> Router -> BulkInserter) from outside
// the process - unlike cmd/simulator, which calls the Router's Handlers
// in-process and bypasses the broker entirely. Point cmd/app at the same
// Gearman server to watch it consume what this tool publishes, or use
// -count 1 to check the 250ms ticker-driven flush (CLAUDE.md rule 3) fires
// for a single event that never reaches the 100-item batch threshold.
//
// Each queue's real wire format is read straight from its JSON dump under
// .claude/specs/ (CLAUDE.md's "Queue Payload Examples") and used as a
// template: for CLAUDE.md's bulk exceptions (statusngin_acknowledgements,
// statusngin_contactnotificationmethod, statusngin_core_restart,
// statusngin_downtimes) the template is cloned once per event and each
// clone is submitted as its own job; every other queue's template messages
// are cloned into batches of up to batchSize and submitted as one bulk
// {"messages": [...]} job per batch, matching what a real Consumer expects
// to decode.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	gearman "github.com/mikespook/gearman-go/client"

	"statusengine-worker/internal/queue"
)

// specsDir is where CLAUDE.md documents the queue payload JSON dumps live,
// relative to the repo root this tool is expected to run from.
const specsDir = ".claude/specs"

// batchSize mirrors CLAUDE.md rule 3's bulk-insert batch threshold: bulk
// queues are published in packets of this size, the same unit the
// pipeline's own BulkInserter flushes in.
const batchSize = 100

// fixtureFiles maps every publishable queue name to the JSON dump under
// specsDir that carries its real wire format.
var fixtureFiles = map[string]string{
	queue.QueueHostStatus:                "statusngin_hoststatus.json",
	queue.QueueServiceStatus:             "statusngin_servicestatus.json",
	queue.QueueHostChecks:                "statusngin_hostchecks.json",
	queue.QueueServiceChecks:             "statusngin_servicechecks.json",
	queue.QueueServicePerfdata:           "statusngin_service_perfdata.json",
	queue.QueueStateChanges:              "statusngin_statechanges.json",
	queue.QueueLogEntries:                "statusngin_logentries.json",
	queue.QueueNotifications:             "statusngin_notifications.json",
	queue.QueueContactNotificationMethod: "statusngin_contactnotificationmethod.json",
	queue.QueueAcknowledgements:          "statusngin_acknowledgements.json",
	queue.QueueDowntimes:                 "statusngin_downtimes.json",
	queue.QueueCoreRestart:               "statusngin_core_restart.json",
}

// nonBulkQueues mirrors CLAUDE.md's "Queue Payload Bulk Exceptions": these
// four queues deliver one raw JSON object per job, never a
// {"messages": [...]} envelope - so unlike every other queue, count events
// become count separate jobs instead of batches of batchSize.
var nonBulkQueues = map[string]bool{
	queue.QueueAcknowledgements:          true,
	queue.QueueContactNotificationMethod: true,
	queue.QueueCoreRestart:               true,
	queue.QueueDowntimes:                 true,
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	queueName := flag.String("queue", queue.QueueHostStatus, "queue name to publish test events to, e.g. statusengine_hoststatus")
	count := flag.Int("count", 1000, "number of events to generate and send")
	server := flag.String("server", "localhost:4730", "Gearman job server address (host:port)")
	flag.Parse()

	if *count < 1 {
		fatal("-count must be >= 1", "count", *count)
	}

	fixtureName, ok := fixtureFiles[*queueName]
	if !ok {
		fatal("unknown -queue", "queue", *queueName, "known_queues", knownQueueNames())
	}

	fixture, err := os.ReadFile(filepath.Join(specsDir, fixtureName))
	if err != nil {
		fatal("read fixture failed", "error", err)
	}

	c, err := gearman.New(gearman.Network, *server)
	if err != nil {
		fatal("connect to gearman failed", "server", *server, "error", err)
	}
	defer c.Close()

	// usecBase folds a random per-process offset into the first generated
	// event's timestamp_usec, so re-running this tool against the same
	// (deliberately non-truncated) dev database doesn't regenerate the
	// exact usec sequence a previous run already sent - which would land on
	// the same hostname+start_time+start_time_usec primary key and get
	// dropped as a duplicate (Error 1062, the exact bug this tool exists to
	// help reproduce and verify the fix for).
	usecBase := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1_000_000)

	slog.Info("publisher starting", "queue", *queueName, "count", *count, "server", *server)

	var sent int
	if nonBulkQueues[*queueName] {
		sent, err = publishNonBulk(c, *queueName, fixture, *count, usecBase)
	} else {
		sent, err = publishBulk(c, *queueName, fixture, *count, usecBase)
	}
	if err != nil {
		fatal("publish failed", "queue", *queueName, "events_sent", sent, "error", err)
	}

	fmt.Printf("Sent %d events to queue %s\n", sent, *queueName)
}

// publishBulk clones fixture's template messages (cycling through however
// many the fixture carries) into batches of up to batchSize events, each
// batch marshaled as one {"messages": [...], "format": ...} job and
// submitted as a single background job - the same shape a real Consumer
// decodes.
func publishBulk(c *gearman.Client, queueName string, fixture []byte, count, usecBase int) (int, error) {
	var envelope struct {
		Messages []json.RawMessage `json:"messages"`
		Format   string            `json:"format"`
	}
	if err := json.Unmarshal(fixture, &envelope); err != nil {
		return 0, fmt.Errorf("parse fixture: %w", err)
	}
	if len(envelope.Messages) == 0 {
		return 0, fmt.Errorf("fixture for %s has no template messages", queueName)
	}

	sent := 0
	batch := make([]map[string]any, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		payload, err := json.Marshal(map[string]any{"messages": batch, "format": envelope.Format})
		if err != nil {
			return fmt.Errorf("marshal batch: %w", err)
		}
		if _, err := c.DoBg(queueName, payload, gearman.JobNormal); err != nil {
			return fmt.Errorf("submit batch job: %w", err)
		}
		sent += len(batch)
		slog.Info("batch sent", "queue", queueName, "batch_events", len(batch), "total_sent", sent)
		batch = batch[:0]
		return nil
	}

	for i := 0; i < count; i++ {
		var msg map[string]any
		if err := json.Unmarshal(envelope.Messages[i%len(envelope.Messages)], &msg); err != nil {
			return sent, fmt.Errorf("parse template message: %w", err)
		}
		bumpEventUsec(msg, usecBase, i)
		batch = append(batch, msg)

		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return sent, err
			}
		}
	}
	if err := flush(); err != nil {
		return sent, err
	}
	return sent, nil
}

// publishNonBulk clones fixture once per event and submits each clone as
// its own background job, for CLAUDE.md's non-bulk queues.
func publishNonBulk(c *gearman.Client, queueName string, fixture []byte, count, usecBase int) (int, error) {
	sent := 0
	for i := 0; i < count; i++ {
		var msg map[string]any
		if err := json.Unmarshal(fixture, &msg); err != nil {
			return sent, fmt.Errorf("parse fixture: %w", err)
		}
		bumpEventUsec(msg, usecBase, i)

		payload, err := json.Marshal(msg)
		if err != nil {
			return sent, fmt.Errorf("marshal event: %w", err)
		}
		if _, err := c.DoBg(queueName, payload, gearman.JobNormal); err != nil {
			return sent, fmt.Errorf("submit job: %w", err)
		}
		sent++
		if sent%100 == 0 || sent == count {
			slog.Info("events sent", "queue", queueName, "total_sent", sent, "count", count)
		}
	}
	return sent, nil
}

// bumpEventUsec overwrites msg's top-level "timestamp_usec" field, if
// present, with a fresh value derived from usecBase+eventIndex - every
// queue's envelope carries at most one such field (statusngin_core_restart
// has none at all, per CLAUDE.md), so unlike cmd/simulator's bumpUsec this
// never needs to recurse.
func bumpEventUsec(msg map[string]any, usecBase, eventIndex int) {
	if _, ok := msg["timestamp_usec"]; ok {
		msg["timestamp_usec"] = float64((usecBase + eventIndex) % 1_000_000)
	}
}

func knownQueueNames() []string {
	names := make([]string, 0, len(fixtureFiles))
	for name := range fixtureFiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

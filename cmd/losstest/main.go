// Command losstest proves - or disproves - that the worker loses no events
// when it is stopped under load, which is the one property a graceful
// shutdown exists to provide (CLAUDE.md rule 6) and the one that cannot be
// checked by reading the code.
//
// It works by publishing events that are individually identifiable. Every
// event gets its own hostname, "lt-<run-id>-<seq>", which is also the first
// column of statusengine_hostchecks' PRIMARY KEY (hostname, start_time,
// start_time_usec). Two consequences follow, and both are the point:
//
//   - A missing sequence number is proof of a lost event, not an artifact of
//     deduplication. Nothing in the pipeline can collapse two of these rows
//     into one.
//   - A redelivered job collides on that PRIMARY KEY. MySQL rejects the
//     whole multi-row INSERT with Error 1062, so the entire batch is
//     dropped, and verify reports it as a contiguous gap. That is not a
//     measurement artifact either - it is exactly what would happen to
//     production data on a redelivery, and worth knowing about.
//
// The intended sequence is three commands with an interruption in the
// middle:
//
//	go run ./cmd/losstest -mode publish -run-id r1 -count 50000
//	# start the worker, let it chew through the backlog, SIGTERM it mid-run,
//	# then start it again and let it drain the rest
//	go run ./cmd/losstest -mode verify -run-id r1 -count 50000
//	go run ./cmd/losstest -mode cleanup -run-id r1
//
// Publishing up front rather than trickling events in is deliberate: it
// leaves a backlog sitting at the job server, which is both the realistic
// restart scenario and the one that exercises the interesting window - jobs
// the broker hands over while the consumer is already shutting down.
//
// Read the exit code, not just the output: verify exits 1 when anything is
// missing, so it can drive a script.
//
// This writes rows into whatever database the DSN names. Point it at a dev
// or staging database, never at production.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	gearman "github.com/mikespook/gearman-go/client"

	"statusengine-worker/internal/queue"
)

const (
	// fixturePath carries the real wire format for the queue used here, the
	// same source cmd/gearman_publisher publishes from. Building the payload
	// by hand instead would test a shape the pipeline never actually
	// receives.
	fixturePath = ".claude/specs/statusngin_hostchecks.json"

	// targetTable is where hostcheck events land, and the table verify
	// counts. statusengine_hostchecks is the right queue for this: it is a
	// plain INSERT rather than an upsert, so a lost row stays visibly
	// lost, and its hostname column is free-form text this tool can use as
	// a marker.
	targetTable = "statusengine_hostchecks"

	// batchSize matches CLAUDE.md rule 3's flush threshold, so one
	// published job maps onto one bulk INSERT.
	batchSize = 100

	// hostnamePrefix marks every row this tool creates. Chosen to be
	// implausible as a real hostname so cleanup's LIKE can never match
	// production data.
	hostnamePrefix = "lt-"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	mode := flag.String("mode", "", `what to do: "publish", "verify" or "cleanup"`)
	runID := flag.String("run-id", "", "identifier for this test run; the same value must be passed to all three modes")
	count := flag.Int("count", 10000, "number of events to publish (publish), or how many to expect (verify)")
	server := flag.String("server", "localhost:4730", "Gearman job server address (host:port)")
	dsn := flag.String("mysql-dsn", "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine-dev", "MySQL data source name")
	fixture := flag.String("fixture", fixturePath, "path to the hostchecks payload fixture")
	flag.Parse()

	if *runID == "" {
		fatal("-run-id is required", "hint", "use a fresh value per run, e.g. -run-id r1")
	}
	if strings.ContainsAny(*runID, "%_'\\") {
		// These would change the meaning of cleanup's LIKE pattern rather
		// than just naming a run.
		fatal("-run-id must not contain %, _, backslash or a quote", "run_id", *runID)
	}
	if *count < 1 {
		fatal("-count must be >= 1", "count", *count)
	}

	switch *mode {
	case "publish":
		runPublish(*server, *fixture, *runID, *count)
	case "verify":
		runVerify(*dsn, *server, *runID, *count)
	case "cleanup":
		runCleanup(*dsn, *runID)
	default:
		fatal("unknown -mode", "mode", *mode, "want", `"publish", "verify" or "cleanup"`)
	}
}

// hostnameFor renders the marker for one event. It is the only place the
// naming scheme is defined; verify parses it back with parseSeq below.
func hostnameFor(runID string, seq int) string {
	return hostnamePrefix + runID + "-" + strconv.Itoa(seq)
}

// hostnameLike is the LIKE pattern matching every row of one run.
func hostnameLike(runID string) string {
	return hostnamePrefix + runID + "-%"
}

// parseSeq recovers the sequence number from a marker hostname, reporting
// false for anything that does not belong to this run - so a stray row can
// never be counted as a delivered event.
func parseSeq(hostname, runID string) (int, bool) {
	prefix := hostnamePrefix + runID + "-"
	rest, ok := strings.CutPrefix(hostname, prefix)
	if !ok {
		return 0, false
	}
	seq, err := strconv.Atoi(rest)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// runPublish sends count events, each carrying its own marker hostname, as
// bulk jobs of batchSize.
func runPublish(server, fixturePath, runID string, count int) {
	template, format, err := loadTemplate(fixturePath)
	if err != nil {
		fatal("read fixture failed", "path", fixturePath, "error", err)
	}

	c, err := gearman.New(gearman.Network, server)
	if err != nil {
		fatal("connect to gearman failed", "server", server, "error", err)
	}
	defer c.Close()

	slog.Info("losstest: publishing",
		"queue", queue.QueueHostChecks, "count", count, "run_id", runID, "server", server)

	sent := 0
	batch := make([]map[string]any, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		payload, err := json.Marshal(map[string]any{"messages": batch, "format": format})
		if err != nil {
			fatal("marshal batch failed", "error", err)
		}
		if _, err := c.DoBg(queue.QueueHostChecks, payload, gearman.JobNormal); err != nil {
			fatal("submit job failed", "events_sent", sent, "error", err)
		}
		sent += len(batch)
		batch = batch[:0]
	}

	for seq := 0; seq < count; seq++ {
		msg, err := cloneWithHostname(template, hostnameFor(runID, seq))
		if err != nil {
			fatal("build event failed", "seq", seq, "error", err)
		}
		batch = append(batch, msg)
		if len(batch) == batchSize {
			flush()
			if sent%10000 == 0 {
				slog.Info("losstest: progress", "sent", sent, "count", count)
			}
		}
	}
	flush()

	fmt.Printf("Published %d events as run %q.\n", sent, runID)
	fmt.Printf("Now start the worker, interrupt it with SIGTERM mid-run, restart it,\n")
	fmt.Printf("and once the backlog is drained run:\n\n")
	fmt.Printf("  go run ./cmd/losstest -mode verify -run-id %s -count %d\n\n", runID, sent)
}

// loadTemplate reads the fixture and returns its first template message
// plus the envelope's format field.
func loadTemplate(path string) (map[string]any, string, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, "", err
	}

	var envelope struct {
		Messages []json.RawMessage `json:"messages"`
		Format   string            `json:"format"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, "", fmt.Errorf("parse fixture: %w", err)
	}
	if len(envelope.Messages) == 0 {
		return nil, "", fmt.Errorf("fixture %s carries no template messages", path)
	}

	var msg map[string]any
	if err := json.Unmarshal(envelope.Messages[0], &msg); err != nil {
		return nil, "", fmt.Errorf("parse template message: %w", err)
	}
	return msg, envelope.Format, nil
}

// cloneWithHostname deep-copies the template (via a JSON round-trip, which
// is cheap enough here and avoids hand-written copying of a nested map) and
// overwrites the hostcheck's host_name with the marker.
func cloneWithHostname(template map[string]any, hostname string) (map[string]any, error) {
	raw, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}
	var clone map[string]any
	if err := json.Unmarshal(raw, &clone); err != nil {
		return nil, err
	}

	hostcheck, ok := clone["hostcheck"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("fixture message has no hostcheck object")
	}
	hostcheck["host_name"] = hostname
	return clone, nil
}

// runVerify reports what actually arrived, and exits non-zero if anything
// is missing.
func runVerify(dsn, server, runID string, count int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db := openDB(ctx, dsn)
	defer db.Close()

	seen, total, err := fetchSeen(ctx, db, runID)
	if err != nil {
		fatal("query failed", "error", err)
	}

	missing := missingRanges(seen, count)
	found := len(seen)
	duplicates := total - found

	fmt.Printf("\nRun %q, expected %d events\n", runID, count)
	fmt.Printf("  rows in %s : %d\n", targetTable, total)
	fmt.Printf("  distinct events  : %d\n", found)
	fmt.Printf("  missing          : %d\n", count-found)
	if duplicates > 0 {
		// Cannot normally happen - the PRIMARY KEY forbids it - so if it
		// does, the schema is not what this tool assumed.
		fmt.Printf("  duplicate rows   : %d\n", duplicates)
	}

	// A backlog still sitting at the broker is not loss: those jobs survive
	// a worker restart by design (CLAUDE.md rule 2). Reported separately so
	// the accounting adds up rather than looking like missing data.
	if queued, err := gearmanQueued(server, queue.QueueHostChecks); err != nil {
		fmt.Printf("  still queued     : unknown (%v)\n", err)
	} else {
		fmt.Printf("  still queued     : %d jobs (~%d events) at %s\n", queued, queued*batchSize, server)
	}

	if len(missing) == 0 {
		fmt.Printf("\nNo events lost.\n")
		return
	}

	fmt.Printf("\nMissing sequence numbers: %s\n", formatRanges(missing))
	fmt.Printf("\nA gap that is an exact multiple of %d, and contiguous, is one or more\n", batchSize)
	fmt.Printf("whole jobs that never made it to MySQL - either dropped during shutdown\n")
	fmt.Printf("or rejected as a batch after a redelivery collided on the PRIMARY KEY.\n")
	fmt.Printf("Check the worker log for \"bulk insert failed\" and Error 1062.\n")
	os.Exit(1)
}

// fetchSeen returns the set of sequence numbers present in the table and
// the raw row count, so the caller can tell missing rows from duplicated
// ones.
func fetchSeen(ctx context.Context, db *sql.DB, runID string) (map[int]struct{}, int, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT hostname FROM "+targetTable+" WHERE hostname LIKE ?", hostnameLike(runID))
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	seen := make(map[int]struct{})
	total := 0
	for rows.Next() {
		var hostname string
		if err := rows.Scan(&hostname); err != nil {
			return nil, 0, err
		}
		total++
		if seq, ok := parseSeq(hostname, runID); ok {
			seen[seq] = struct{}{}
		}
	}
	return seen, total, rows.Err()
}

// missingRanges returns every sequence number below count that never
// arrived, in ascending order.
func missingRanges(seen map[int]struct{}, count int) []int {
	var missing []int
	for seq := 0; seq < count; seq++ {
		if _, ok := seen[seq]; !ok {
			missing = append(missing, seq)
		}
	}
	return missing
}

// formatRanges compresses a sorted list into "0-99, 250, 4000-4099", so a
// gap of ten thousand events is one readable line instead of ten thousand
// numbers. Truncated after maxRanges groups.
func formatRanges(nums []int) string {
	const maxRanges = 20
	if len(nums) == 0 {
		return "none"
	}
	sort.Ints(nums)

	var groups []string
	start, prev := nums[0], nums[0]
	emit := func() {
		if start == prev {
			groups = append(groups, strconv.Itoa(start))
		} else {
			groups = append(groups, fmt.Sprintf("%d-%d", start, prev))
		}
	}
	for _, n := range nums[1:] {
		if n == prev+1 {
			prev = n
			continue
		}
		emit()
		start, prev = n, n
	}
	emit()

	if len(groups) > maxRanges {
		return strings.Join(groups[:maxRanges], ", ") +
			fmt.Sprintf(" ... (%d more ranges)", len(groups)-maxRanges)
	}
	return strings.Join(groups, ", ")
}

// gearmanQueued asks the job server how many jobs are still waiting for
// fnName, using the plain-text admin protocol ("status" returns one
// tab-separated line per function, terminated by a lone dot) - the same
// thing `gearadmin --status` prints.
func gearmanQueued(server, fnName string) (int, error) {
	conn, err := net.DialTimeout("tcp", server, 3*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return 0, err
	}
	if _, err := fmt.Fprint(conn, "status\n"); err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "." {
			break
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 2 || fields[0] != fnName {
			continue
		}
		queued, err := strconv.Atoi(fields[1])
		if err != nil {
			return 0, fmt.Errorf("unexpected status line %q", line)
		}
		return queued, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, nil // function unknown to the server = nothing queued
}

// runCleanup removes this run's rows, in batches for the same reason
// internal/cleanup uses them: one statement over a large range holds locks
// and inflates the binlog for as long as it runs.
func runCleanup(dsn, runID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db := openDB(ctx, dsn)
	defer db.Close()

	const deleteBatch = 5000
	deleted := int64(0)
	for {
		res, err := db.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE hostname LIKE ? LIMIT %d", targetTable, deleteBatch),
			hostnameLike(runID))
		if err != nil {
			fatal("delete failed", "deleted_so_far", deleted, "error", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			fatal("rows affected failed", "error", err)
		}
		deleted += affected
		if affected < deleteBatch {
			break
		}
	}

	fmt.Printf("Deleted %d rows for run %q.\n", deleted, runID)
}

func openDB(ctx context.Context, dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fatal("mysql: open failed", "error", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		fatal("mysql: unreachable", "error", err)
	}
	return db
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

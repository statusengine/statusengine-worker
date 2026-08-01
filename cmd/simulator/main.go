// Command simulator replays the real queue payload fixtures from
// .claude/specs/ through the exact same Router/BulkInserter pipeline
// cmd/app/main.go wires up, at a configurable target rate - so the
// 100-row/250ms throttled bulk-insert behaviour (CLAUDE.md rule 3) can be
// watched happening against a real MySQL connection instead of guessed
// at.
//
// It bypasses Gearman/RabbitMQ entirely: fixture payloads are handed
// straight to the Router's Handlers, which is exactly what a Consumer
// does per message once it has decoded one off the wire - so this is a
// mock in the sense that no broker is involved, not in the sense that the
// decode/persist/broadcast path differs from production.
//
// WARNING: this writes real rows into whatever database -mysql-dsn points
// at, replaying the same fixture host/service names on every iteration.
// Point it at a disposable dev database (the CLAUDE.md default), never
// production.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/queue"
	"statusengine-worker/internal/websocket"
)

// fixtureFiles maps every queue name the Router knows about to the JSON
// dump under .claude/specs/ that carries its real wire format (CLAUDE.md's
// "Queue Payload Examples").
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

type config struct {
	mysqlDSN string
	specsDir string
	rate     int
	duration time.Duration
	workers  int
	queues   string // comma-separated subset of fixtureFiles' keys, or "" for all
	logLevel string
}

func loadConfig() config {
	cfg := config{}

	flag.StringVar(&cfg.mysqlDSN, "mysql-dsn",
		envOrDefault("STATUSENGINE_MYSQL_DSN", "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine-dev?parseTime=true"),
		"MySQL data source name - a disposable dev database; this tool writes real rows")
	flag.StringVar(&cfg.specsDir, "specs-dir", envOrDefault("STATUSENGINE_SPECS_DIR", ".claude/specs"),
		"directory containing the queue payload JSON dumps, run from the repo root by default")
	flag.IntVar(&cfg.rate, "rate", 5000, "target queue messages per second, spread across all replayed queues")
	flag.DurationVar(&cfg.duration, "duration", 15*time.Second, "how long to run before stopping (0 = until interrupted)")
	flag.IntVar(&cfg.workers, "workers", 16, "number of concurrent goroutines calling Router Handlers")
	flag.StringVar(&cfg.queues, "queues", "", "comma-separated queue names to replay (default: all known queues)")
	flag.StringVar(&cfg.logLevel, "log-level", "info", `minimum log level: "debug", "info", "warn" or "error"`)
	flag.Parse()

	return cfg
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func setupLogger(levelStr string) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(levelStr)); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func main() {
	cfg := loadConfig()
	setupLogger(cfg.logLevel)

	queueNames, err := selectQueues(cfg.queues)
	if err != nil {
		fatal("invalid -queues", "error", err)
	}

	payloads, err := loadFixtures(cfg.specsDir, queueNames)
	if err != nil {
		fatal("failed to load fixtures", "error", err)
	}

	slog.Warn("simulator writes real rows into the target database - make sure -mysql-dsn points at a disposable dev database, never production")

	sqlDB, err := sql.Open("mysql", cfg.mysqlDSN)
	if err != nil {
		fatal("mysql: open failed", "error", err)
	}
	defer sqlDB.Close()

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := sqlDB.PingContext(pingCtx); err != nil {
		cancelPing()
		fatal("mysql: unreachable", "error", err)
	}
	cancelPing()

	pipelineCtx, cancelPipeline := context.WithCancel(context.Background())
	defer cancelPipeline()

	var wg sync.WaitGroup

	hub := websocket.NewHub()
	wg.Add(1)
	go func() {
		defer wg.Done()
		hub.Run(pipelineCtx)
	}()

	// Never dialed: this run always uses PerfdataRouteMySQL below, so
	// Enqueue is never called on it - see graphite.Client's doc comment.
	gc := graphite.NewClient("127.0.0.1:2003")

	router, runners := queue.NewRouter(sqlDB, hub, gc, queue.PerfdataRouteMySQL)
	for _, r := range runners {
		wg.Add(1)
		go func(r queue.Runner) {
			defer wg.Done()
			r.Run(pipelineCtx)
		}(r)
	}
	// Let every BulkInserter's Run loop start reading its channel before
	// the first burst - purely cosmetic (Enqueue would just block briefly
	// otherwise), so the very first flush log line isn't skewed by a
	// startup queue.
	time.Sleep(50 * time.Millisecond)

	runCtx := pipelineCtx
	if cfg.duration > 0 {
		var cancelRun context.CancelFunc
		runCtx, cancelRun = context.WithTimeout(pipelineCtx, cfg.duration)
		defer cancelRun()
	} else {
		sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stopSignals()
		runCtx = sigCtx
	}

	slog.Info("simulator starting",
		"queues", strings.Join(queueNames, ","), "target_rate", cfg.rate, "workers", cfg.workers, "duration", cfg.duration)

	sent := runLoad(runCtx, router, queueNames, payloads, cfg.rate, cfg.workers)
	slog.Info("simulator stopped sending", "messages_sent", sent)

	// Flush every BulkInserter's remaining buffer immediately, so the
	// trailing partial batch - which would otherwise sit until
	// FlushInterval next fires - lands (and logs) right away, the same
	// way cmd/app/main.go's graceful shutdown does (CLAUDE.md rule 6).
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
	for _, r := range runners {
		if err := r.Flush(flushCtx); err != nil {
			slog.Error("error flushing runner", "error", err)
		}
	}
	cancelFlush()

	cancelPipeline()
	wg.Wait()

	slog.Info("simulator done")
}

// selectQueues returns every queue name in fixtureFiles, or the subset
// named by a comma-separated -queues value.
func selectQueues(csv string) ([]string, error) {
	if csv == "" {
		names := make([]string, 0, len(fixtureFiles))
		for name := range fixtureFiles {
			names = append(names, name)
		}
		sort.Strings(names)
		return names, nil
	}

	var names []string
	for _, name := range strings.Split(csv, ",") {
		name = strings.TrimSpace(name)
		if _, ok := fixtureFiles[name]; !ok {
			return nil, fmt.Errorf("unknown queue %q", name)
		}
		names = append(names, name)
	}
	return names, nil
}

// loadFixtures reads each queue's fixture file once, up front, so the hot
// send loop only ever does an in-memory map lookup.
func loadFixtures(specsDir string, queueNames []string) (map[string][]byte, error) {
	payloads := make(map[string][]byte, len(queueNames))
	for _, name := range queueNames {
		path := filepath.Join(specsDir, fixtureFiles[name])
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read fixture for %s: %w", name, err)
		}
		payloads[name] = data
	}
	return payloads, nil
}

// timestampFields lists every JSON object key, anywhere in a fixture's
// tree, that withUniqueTimestamps treats as a Unix-seconds timestamp to
// shift. It deliberately excludes "*_usec" companions (e.g.
// "timestamp_usec") - those are a sub-second component of the same
// instant and adding a multi-second offset to the whole-second field
// alone already guarantees uniqueness.
var timestampFields = map[string]bool{
	"timestamp":  true,
	"start_time": true,
	"end_time":   true,
	"entry_time": true,
	"state_time": true,
}

// timestampSpacing is the gap, in seconds, between two calls' offsets
// (see runLoad's seq.Add(1)*timestampSpacing). A single fixture's several
// messages can differ by a few seconds from each other (e.g.
// statusngin_statechanges.json's two entries are 3 seconds apart); some
// destination rows don't carry sub-second (usec) precision into their
// primary key at all, only the whole-second field this tool shifts, so
// consecutive calls need a gap far larger than any such intra-fixture
// delta - otherwise two different calls could still coincidentally land
// on the same shifted second and collide with each other. 100000 (~27h)
// comfortably clears that.
const timestampSpacing = 100_000

// withUniqueTimestamps returns a copy of payload with every field named
// in timestampFields shifted by offsetSeconds, wherever it appears in the
// JSON tree. Fixtures are static files replayed thousands of times per
// run; several destination tables key their primary key on one of these
// fields (e.g. statusengine_hostchecks on hostname+start_time), so
// without this every replay past the first would collide and get dropped
// as a duplicate - which would demonstrate a MySQL error, not the
// buffer's flush behaviour this tool exists to show. Falls back to
// returning payload unmodified if it doesn't parse as JSON, which should
// never happen for our own fixtures.
func withUniqueTimestamps(payload []byte, offsetSeconds int64) []byte {
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return payload
	}
	bumpTimestamps(v, offsetSeconds)

	out, err := json.Marshal(v)
	if err != nil {
		return payload
	}
	return out
}

// bumpTimestamps walks a decoded JSON value (as produced by
// json.Unmarshal into `any`) and adds offset to every number found under
// a key in timestampFields, recursing into nested objects and arrays -
// bulk payloads carry a "messages" array of envelopes, each with its own
// nested "hostcheck"/"servicecheck"/etc. object.
func bumpTimestamps(v any, offset int64) {
	switch t := v.(type) {
	case map[string]any:
		for key, val := range t {
			if timestampFields[key] {
				if n, ok := val.(float64); ok {
					t[key] = n + float64(offset)
					continue
				}
			}
			bumpTimestamps(val, offset)
		}
	case []any:
		for _, item := range t {
			bumpTimestamps(item, offset)
		}
	}
}

// runLoad fans a rate-limited stream of fixture replays across workers
// concurrent goroutines, picking a random queue per send the same way a
// busy production pipeline receives many different queues interleaved.
// It returns the total number of messages successfully handled once
// runCtx is done and every in-flight handler call has returned.
func runLoad(runCtx context.Context, router queue.Router, queueNames []string, payloads map[string][]byte, ratePerSec, workers int) uint64 {
	tokens := rateLimiter(runCtx, ratePerSec)

	var sent atomic.Uint64
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		reportProgress(runCtx, &sent)
	}()

	var seq atomic.Int64

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for range tokens {
				name := queueNames[rng.Intn(len(queueNames))]
				// Every call gets a fresh timestamp offset, widely spaced
				// (see timestampSpacing) - the static fixtures would
				// otherwise replay the exact same hostname+start_time (or
				// service_description+state_time, etc.) on every send,
				// which collides with the primary key on
				// statusengine_hostchecks, statusengine_servicechecks,
				// the *_acknowledgements and *_statehistory tables after
				// the very first insert. See withUniqueTimestamps.
				payload := withUniqueTimestamps(payloads[name], seq.Add(1)*timestampSpacing)
				if err := router[name](runCtx, payload); err != nil {
					slog.Warn("simulator: handler failed", "queue", name, "error", err)
					continue
				}
				sent.Add(1)
			}
		}(time.Now().UnixNano() + int64(w))
	}

	wg.Wait()
	return sent.Load()
}

// rateLimiter releases roughly ratePerSec tokens per second in small
// bursts every 10ms, rather than one at a time, so it can sustain several
// thousand tokens/sec despite the coarser resolution a single
// time.Ticker realistically offers. It closes the returned channel once
// ctx is done, which is what lets runLoad's `for range tokens` workers
// exit cleanly.
func rateLimiter(ctx context.Context, ratePerSec int) <-chan struct{} {
	const ticksPerSecond = 100
	perTick := ratePerSec / ticksPerSecond
	if perTick < 1 {
		perTick = 1
	}

	tokens := make(chan struct{}, ratePerSec)
	go func() {
		defer close(tokens)
		ticker := time.NewTicker(time.Second / ticksPerSecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for i := 0; i < perTick; i++ {
					select {
					case tokens <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return tokens
}

// reportProgress logs achieved throughput every 2 seconds, so a run's
// actual rate can be compared against the -rate target without having to
// wait for the final summary.
func reportProgress(ctx context.Context, sent *atomic.Uint64) {
	const interval = 2 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	var last uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := sent.Load()
			slog.Info("simulator progress",
				"messages_sent", now,
				"current_rate_per_sec", float64(now-last)/interval.Seconds(),
				"elapsed", time.Since(start).Round(time.Second),
			)
			last = now
		}
	}
}

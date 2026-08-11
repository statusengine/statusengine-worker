// Command app wires together the queue consumer, the MySQL bulk-insert
// pipeline and the WebSocket broadcast hub described in CLAUDE.md.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	_ "github.com/go-sql-driver/mysql"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/queue"
	"statusengine-worker/internal/websocket"
)

// config holds every value that can be set via flag, environment variable
// or config file. For every setting below, precedence is: an explicitly
// passed CLI flag wins outright; otherwise an environment variable wins;
// otherwise a value from -config's YAML file wins; otherwise the
// hardcoded default below applies. Defaults match the local dev setup
// documented in .claude/specs/ressources.txt.
type config struct {
	configFile                string // path to an optional YAML config file - see config.example.yaml
	consumerBackend           string // "gearman" or "rabbitmq"
	gearmanAddr               string
	gearmanMaxConcurrentJobs  int // cap on simultaneously running Gearman job handlers
	rabbitMQURL               string
	rabbitMQPrefetch          int // unacknowledged deliveries the broker may push per queue
	mysqlDSN                  string
	mysqlMaxOpenConns         int // upper bound on simultaneously open MySQL connections; also used as the idle limit
	listenAddr                string
	metricsListenAddr         string
	graphiteAddr              string
	graphitePrefix            string // prepended to every Graphite path, e.g. "statusengine.<host>.<service>.<metric>"
	perfdataRoute             string // "mysql", "graphite" or "both"
	nodeName                  string // written into hoststatus/servicestatus rows' node_name column
	apiKeys                   string // comma-separated; empty disables /ws authentication entirely
	enableOpenITCockpitTweaks bool   // selects the core-restart hoststatus/servicestatus cleanup query
	logLevel                  string // "debug", "info", "warn" or "error"
	logFormat                 string // "text" or "json"
}

// fileConfig mirrors config's fields for -config's optional YAML file (see
// config.example.yaml for every key, its default and a description). Every
// key is optional: a zero value (empty string, nil for APIKeys/
// EnableOpenITCockpitTweaks) means "not set in the file", so it never
// overrides an environment variable or hardcoded default - see resolveString/
// resolveBool. EnableOpenITCockpitTweaks is a *bool (rather than bool) for
// exactly this reason: unlike a missing string, Go can't otherwise tell
// "the file didn't mention this key" apart from "the file explicitly set
// it to false".
type fileConfig struct {
	Consumer                  string   `yaml:"consumer"`
	GearmanAddr               string   `yaml:"gearman_addr"`
	GearmanMaxConcurrentJobs  int      `yaml:"gearman_max_concurrent_jobs"`
	RabbitMQURL               string   `yaml:"rabbitmq_url"`
	RabbitMQPrefetch          int      `yaml:"rabbitmq_prefetch"`
	MySQLDSN                  string   `yaml:"mysql_dsn"`
	MySQLMaxOpenConns         int      `yaml:"mysql_max_open_conns"`
	ListenAddr                string   `yaml:"listen_addr"`
	MetricsListenAddr         string   `yaml:"metrics_listen_addr"`
	GraphiteAddr              string   `yaml:"graphite_addr"`
	GraphitePrefix            string   `yaml:"graphite_prefix"`
	PerfdataRoute             string   `yaml:"perfdata_route"`
	NodeName                  string   `yaml:"nodename"`
	APIKeys                   []string `yaml:"api_keys"`
	EnableOpenITCockpitTweaks *bool    `yaml:"enable_openitcockpit_tweaks"`
	LogLevel                  string   `yaml:"log_level"`
	LogFormat                 string   `yaml:"log_format"`
}

// loadFileConfig reads and parses an optional YAML config file. Called
// only when -config/STATUSENGINE_CONFIG names a path, so both a missing
// file and invalid YAML are treated as fatal misconfiguration - silently
// falling back to defaults would leave a typo'd path looking like it
// worked.
func loadFileConfig(path string) fileConfig {
	data, err := os.ReadFile(path)
	if err != nil {
		fatal("config: failed to read -config file", "path", path, "error", err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		fatal("config: failed to parse -config file", "path", path, "error", err)
	}
	return fc
}

// resolveString applies config's flag > env > file > default precedence
// for one string setting. flagVal is the flag's value after Parse() - if
// flagName wasn't passed explicitly (checked via explicit, built from
// flag.Visit), flagVal already equals that flag's hardcoded default, so
// it doubles as the final fallback.
func resolveString(explicit map[string]bool, flagName, flagVal, envKey, fileVal string) string {
	if explicit[flagName] {
		return flagVal
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	if fileVal != "" {
		return fileVal
	}
	return flagVal
}

// resolveInt is resolveString's counterpart for integer settings. A
// fileVal of 0 means "the file didn't set this key" - every integer
// setting here is a positive count, so 0 is not a meaningful value anyway;
// an explicitly configured 0 is rejected in loadConfig rather than
// silently treated as absent.
func resolveInt(explicit map[string]bool, flagName string, flagVal int, envKey string, fileVal int) int {
	if explicit[flagName] {
		return flagVal
	}
	if v, ok := os.LookupEnv(envKey); ok {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	if fileVal != 0 {
		return fileVal
	}
	return flagVal
}

// resolveBool is resolveString's counterpart for the one bool setting;
// fileVal is a pointer so a key the file never mentioned (nil) is
// distinguishable from one explicitly set to false.
func resolveBool(explicit map[string]bool, flagName string, flagVal bool, envKey string, fileVal *bool) bool {
	if explicit[flagName] {
		return flagVal
	}
	if v, ok := os.LookupEnv(envKey); ok {
		if parsed, err := strconv.ParseBool(v); err == nil {
			return parsed
		}
	}
	if fileVal != nil {
		return *fileVal
	}
	return flagVal
}

func loadConfig() config {
	cfg := config{}

	flag.StringVar(&cfg.configFile, "config", envOrDefault("STATUSENGINE_CONFIG", ""),
		"path to an optional YAML config file (see config.example.yaml); flags and environment variables "+
			"still take precedence over anything set here")
	flag.StringVar(&cfg.consumerBackend, "consumer", "gearman",
		`queue backend to use: "gearman" or "rabbitmq"`)
	flag.StringVar(&cfg.gearmanAddr, "gearman-addr", "127.0.0.1:4730",
		"Gearman job server address (host:port)")
	flag.IntVar(&cfg.gearmanMaxConcurrentJobs, "gearman-max-concurrent-jobs", 64,
		"maximum number of Gearman job handlers running at once; the cap that keeps a backlog "+
			"queued at the job server instead of accumulating in this process")
	flag.StringVar(&cfg.rabbitMQURL, "rabbitmq-url", "amqp://statusengine:statusengine@127.0.0.1:5672/",
		"RabbitMQ broker URL")
	flag.IntVar(&cfg.rabbitMQPrefetch, "rabbitmq-prefetch", 100,
		"maximum unacknowledged deliveries the broker may push per queue; applies per queue, so the "+
			"worst-case in-memory backlog is this times the number of queues")
	flag.StringVar(&cfg.mysqlDSN, "mysql-dsn", "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine-dev?parseTime=true",
		"MySQL data source name")
	flag.IntVar(&cfg.mysqlMaxOpenConns, "mysql-max-open-conns", 25,
		"maximum number of open MySQL connections (also used as the idle-connection limit); "+
			"must stay below the server's max_connections")
	flag.StringVar(&cfg.listenAddr, "listen-addr", "127.0.0.1:8080",
		"address the WebSocket HTTP server listens on; loopback-only by default, "+
			"pass an explicit interface (e.g. \":8080\") to expose it on the network")
	flag.StringVar(&cfg.metricsListenAddr, "metrics-listen-addr", ":9105",
		"address the Prometheus /metrics HTTP server listens on")
	flag.StringVar(&cfg.graphiteAddr, "graphite-addr", "127.0.0.1:2003",
		"Graphite Carbon plaintext receiver address (host:port)")
	flag.StringVar(&cfg.graphitePrefix, "graphite-prefix", "statusengine",
		"prefix prepended to every Graphite metric path (prefix.hostname.service_description.label)")
	flag.StringVar(&cfg.perfdataRoute, "perfdata-route", "mysql",
		`where statusngin_service_perfdata metrics are written: "mysql", "graphite" or "both" (CLAUDE.md rule 5)`)
	flag.StringVar(&cfg.nodeName, "nodename", "statusengine",
		"node_name value written into statusengine_hoststatus/statusengine_servicestatus rows")
	flag.StringVar(&cfg.apiKeys, "api-keys", "",
		"comma-separated API keys accepted by the /ws endpoint (Authorization: Bearer <key> or X-Api-Key header; "+
			"api_key query parameter also accepted, for browser clients that can't set headers); empty disables auth")
	flag.BoolVar(&cfg.enableOpenITCockpitTweaks, "enable-openitcockpit-tweaks", false,
		"on a core restart, delete only hoststatus/servicestatus rows for objects openITCockpit no longer "+
			"knows about instead of truncating both tables outright")
	flag.StringVar(&cfg.logLevel, "log-level", "info",
		`minimum log level: "debug", "info", "warn" or "error"`)
	flag.StringVar(&cfg.logFormat, "log-format", "text",
		`structured log output format: "text" or "json"`)
	flag.Parse()

	explicit := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	var fc fileConfig
	if cfg.configFile != "" {
		fc = loadFileConfig(cfg.configFile)
	}

	cfg.consumerBackend = resolveString(explicit, "consumer", cfg.consumerBackend, "STATUSENGINE_CONSUMER", fc.Consumer)
	cfg.gearmanAddr = resolveString(explicit, "gearman-addr", cfg.gearmanAddr, "STATUSENGINE_GEARMAN_ADDR", fc.GearmanAddr)
	cfg.gearmanMaxConcurrentJobs = resolveInt(explicit, "gearman-max-concurrent-jobs", cfg.gearmanMaxConcurrentJobs, "STATUSENGINE_GEARMAN_MAX_CONCURRENT_JOBS", fc.GearmanMaxConcurrentJobs)
	cfg.rabbitMQURL = resolveString(explicit, "rabbitmq-url", cfg.rabbitMQURL, "STATUSENGINE_RABBITMQ_URL", fc.RabbitMQURL)
	cfg.rabbitMQPrefetch = resolveInt(explicit, "rabbitmq-prefetch", cfg.rabbitMQPrefetch, "STATUSENGINE_RABBITMQ_PREFETCH", fc.RabbitMQPrefetch)
	cfg.mysqlDSN = resolveString(explicit, "mysql-dsn", cfg.mysqlDSN, "STATUSENGINE_MYSQL_DSN", fc.MySQLDSN)
	cfg.mysqlMaxOpenConns = resolveInt(explicit, "mysql-max-open-conns", cfg.mysqlMaxOpenConns, "STATUSENGINE_MYSQL_MAX_OPEN_CONNS", fc.MySQLMaxOpenConns)
	cfg.listenAddr = resolveString(explicit, "listen-addr", cfg.listenAddr, "STATUSENGINE_LISTEN_ADDR", fc.ListenAddr)
	cfg.metricsListenAddr = resolveString(explicit, "metrics-listen-addr", cfg.metricsListenAddr, "STATUSENGINE_METRICS_LISTEN_ADDR", fc.MetricsListenAddr)
	cfg.graphiteAddr = resolveString(explicit, "graphite-addr", cfg.graphiteAddr, "STATUSENGINE_GRAPHITE_ADDR", fc.GraphiteAddr)
	cfg.graphitePrefix = resolveString(explicit, "graphite-prefix", cfg.graphitePrefix, "STATUSENGINE_GRAPHITE_PREFIX", fc.GraphitePrefix)
	cfg.perfdataRoute = resolveString(explicit, "perfdata-route", cfg.perfdataRoute, "STATUSENGINE_PERFDATA_ROUTE", fc.PerfdataRoute)
	cfg.nodeName = resolveString(explicit, "nodename", cfg.nodeName, "STATUSENGINE_NODENAME", fc.NodeName)
	cfg.apiKeys = resolveString(explicit, "api-keys", cfg.apiKeys, "STATUSENGINE_API_KEYS", strings.Join(fc.APIKeys, ","))
	cfg.enableOpenITCockpitTweaks = resolveBool(explicit, "enable-openitcockpit-tweaks", cfg.enableOpenITCockpitTweaks, "ENABLE_OPENITCOCKPIT_TWEAKS", fc.EnableOpenITCockpitTweaks)
	cfg.logLevel = resolveString(explicit, "log-level", cfg.logLevel, "STATUSENGINE_LOG_LEVEL", fc.LogLevel)
	cfg.logFormat = resolveString(explicit, "log-format", cfg.logFormat, "STATUSENGINE_LOG_FORMAT", fc.LogFormat)

	if cfg.mysqlMaxOpenConns < 1 {
		fatal("invalid -mysql-max-open-conns", "value", cfg.mysqlMaxOpenConns, "want", "a positive number")
	}
	// Both of these mean "unlimited" at zero in their respective
	// libraries, which is exactly the unbounded behaviour they exist to
	// prevent - so zero is rejected rather than passed through.
	if cfg.gearmanMaxConcurrentJobs < 1 {
		fatal("invalid -gearman-max-concurrent-jobs", "value", cfg.gearmanMaxConcurrentJobs, "want", "a positive number")
	}
	if cfg.rabbitMQPrefetch < 1 {
		fatal("invalid -rabbitmq-prefetch", "value", cfg.rabbitMQPrefetch, "want", "a positive number")
	}

	return cfg
}

// setupLogger configures the process-wide slog default logger from cfg and
// installs it via slog.SetDefault, so every package's package-level
// slog.Info/Warn/Error calls (they never build their own logger) share one
// consistent, structured sink.
func setupLogger(cfg config) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.logLevel)); err != nil {
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.EqualFold(cfg.logFormat, "json") {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// fatal logs msg at error level and exits the process - slog has no
// built-in Fatal, unlike the "log" package's log.Fatalf this replaces.
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

// parseAPIKeys splits raw (the -api-keys flag/STATUSENGINE_API_KEYS value)
// on commas into the set websocket.ServeWS checks incoming /ws requests
// against. An empty raw yields an empty (nil) set; resolveAPIKeys, not
// this function, decides what that means.
func parseAPIKeys(raw string) map[string]struct{} {
	if raw == "" {
		return nil
	}
	keys := make(map[string]struct{})
	for _, k := range strings.Split(raw, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			keys[k] = struct{}{}
		}
	}
	return keys
}

// resolveAPIKeys turns the configured -api-keys value into the key set
// /ws is guarded with, generating a random one when nothing was
// configured. The /ws stream carries every monitoring event the worker
// sees, so it is never served unauthenticated: an unconfigured worker
// gets a per-run key rather than an open endpoint.
//
// This matters more than it looks. A WebSocket handshake is not subject to
// the same-origin policy and triggers no CORS preflight, so an open /ws on
// any address a browser can reach - including a loopback or RFC1918 one -
// can be opened by any web page the operator happens to visit, which then
// receives the whole event stream. Loopback-only binding (see
// -listen-addr's default) narrows who can reach the port; the key is what
// makes reaching it useless.
//
// The generated key changes on every restart, so it is a safety net, not
// the intended production setup: configure -api-keys/STATUSENGINE_API_KEYS
// for a stable one.
func resolveAPIKeys(raw string) map[string]struct{} {
	if keys := parseAPIKeys(raw); len(keys) > 0 {
		return keys
	}

	generated, err := generateAPIKey()
	if err != nil {
		// crypto/rand failing is not something to paper over with a
		// weaker key - without a usable secret, refusing to start beats
		// serving the event stream to anyone who asks.
		fatal("websocket: could not generate an API key", "error", err)
	}

	// Warn rather than Info: this is the one startup line an operator has
	// to actually read. Note that -log-level error suppresses it - that is
	// the operator opting out of warnings, and -api-keys is the answer.
	slog.Warn("websocket: no API key configured, generated a random one for this run",
		"api_key", generated,
		"hint", "set -api-keys/STATUSENGINE_API_KEYS (or api_keys in the config file) for a stable key")

	return map[string]struct{}{generated: {}}
}

// generateAPIKey returns a 256-bit random key, hex-encoded so it survives
// being pasted into a URL, a header or a config file unescaped.
func generateAPIKey() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envBoolOrDefault parses key as a bool (accepting the same formats as
// strconv.ParseBool: "1"/"0", "t"/"f", "true"/"false", ...), falling back to
// def if the variable is unset or not a valid bool.
func envBoolOrDefault(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return parsed
}

// HTTP server timeouts. Without them a client that opens a connection and
// then dribbles (or never finishes) its request headers keeps a goroutine
// and a file descriptor for as long as it likes - the Slowloris shape, and
// what gosec flags as G112 on a bare &http.Server{}.
const (
	// wsReadHeaderTimeout bounds the WebSocket handshake's header phase.
	wsReadHeaderTimeout = 10 * time.Second
	// wsIdleTimeout bounds a kept-alive connection that never upgrades.
	wsIdleTimeout = 120 * time.Second

	metricsReadHeaderTimeout = 5 * time.Second
	metricsReadTimeout       = 10 * time.Second
	metricsWriteTimeout      = 30 * time.Second
	metricsIdleTimeout       = 60 * time.Second
)

// newWebsocketServer builds the /ws server.
//
// It deliberately sets no ReadTimeout or WriteTimeout. Those are whole-
// request deadlines, and a WebSocket connection is a request that is
// meant to last hours: they would apply to the pre-upgrade handshake and
// then be inherited by the hijacked connection, where the read and write
// pumps manage their own deadlines instead (see writeWait/pongWait in
// internal/websocket/client.go). Header and idle timeouts cover the phase
// the server still owns, which is the phase Slowloris attacks.
func newWebsocketServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: wsReadHeaderTimeout,
		IdleTimeout:       wsIdleTimeout,
	}
}

// newMetricsServer builds the /metrics server. Unlike /ws this serves
// ordinary short-lived requests, so it gets the full set of timeouts.
func newMetricsServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
		ReadTimeout:       metricsReadTimeout,
		WriteTimeout:      metricsWriteTimeout,
		IdleTimeout:       metricsIdleTimeout,
	}
}

// shutdownFlushTimeout is the budget for draining every runner's buffer on
// shutdown. Because flushRunners runs them concurrently, this is the wall
// clock the whole flush gets - not a budget that the first runners can
// spend on behalf of the rest.
const shutdownFlushTimeout = 10 * time.Second

// flushRunners drains every runner's buffer, concurrently, and returns how
// many failed. Errors are logged rather than returned: at this point in the
// shutdown there is nothing left to abort, and one wedged table must not
// stop the other fourteen from being written.
//
// Concurrently, because sequentially they shared a single deadline: a slow
// MySQL let the first runners spend the entire budget, after which every
// remaining one was handed an already-expired context, returned ctx.Err()
// immediately and produced an error line for something it never got a
// chance to do. No data was lost (each runner's own finalFlush retries on a
// background context), but the shutdown log said otherwise.
//
// This means up to len(runners) statements in flight at once, which is
// what -mysql-max-open-conns must accommodate; the default of 25 covers the
// current 15 runners comfortably. Setting it below the runner count is
// safe - the pool simply serializes the flushes again.
func flushRunners(ctx context.Context, runners []queue.Runner) int {
	var wg sync.WaitGroup
	var failed atomic.Int64

	for _, r := range runners {
		wg.Add(1)
		go func(r queue.Runner) {
			defer wg.Done()
			if err := r.Flush(ctx); err != nil {
				failed.Add(1)
				slog.Error("error flushing runner on shutdown", "error", err)
			}
		}(r)
	}
	wg.Wait()

	return int(failed.Load())
}

// connMaxLifetime caps how long a pooled MySQL connection is reused.
// database/sql cannot tell a connection silently dropped by a proxy, a
// failover or the server's own wait_timeout from a healthy one until it
// tries to use it, so connections are retired periodically instead. Well
// below MySQL's 8h default wait_timeout, and short enough that a failed-over
// primary is picked up without a restart.
const connMaxLifetime = 5 * time.Minute

// configureDBPool sizes the shared *sql.DB every BulkInserter flushes
// through. Without this, database/sql applies its defaults - unlimited open
// connections but only *two* idle ones - which is the pathological case
// here: the pipeline runs a BulkInserter per queue, all flushing on the same
// 250ms ticker (CLAUDE.md rule 3), so all but two connections are torn down
// and redialed on every single tick. That shows up as MySQL latency in
// DBBatchFlushDurationSeconds while actually being connection setup,
// TLS handshakes and server-side thread churn.
//
// maxIdle is deliberately equal to maxOpen: a pool that closes idle
// connections between ticks recreates exactly the churn this exists to
// avoid, and an idle MySQL connection is cheap.
func configureDBPool(db *sql.DB, maxOpen int) {
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	db.SetConnMaxLifetime(connMaxLifetime)

	slog.Info("mysql: connection pool configured",
		"max_open_conns", maxOpen, "max_idle_conns", maxOpen, "conn_max_lifetime", connMaxLifetime)
}

func main() {
	cfg := loadConfig()
	setupLogger(cfg)
	if cfg.configFile != "" {
		slog.Info("config: loaded settings from file", "path", cfg.configFile)
	}

	perfdataRoute, err := queue.ParsePerfdataRoute(cfg.perfdataRoute)
	if err != nil {
		fatal("invalid -perfdata-route", "error", err)
	}

	// pipelineCtx governs every long-running loop (BulkInserters, the Hub,
	// the consumer's internal ctx.Done() watcher). It is only cancelled
	// once the ordered shutdown sequence below has already stopped the
	// consumer and flushed the buffers - by then, cancelling it is just a
	// backstop, not the primary shutdown mechanism (CLAUDE.md rule 6).
	pipelineCtx, cancelPipeline := context.WithCancel(context.Background())
	defer cancelPipeline()

	var wg sync.WaitGroup

	// 2. WebSocket hub, in its own goroutine.
	hub := websocket.NewHub()
	wg.Add(1)
	go func() {
		defer wg.Done()
		hub.Run(pipelineCtx)
	}()

	apiKeys := resolveAPIKeys(cfg.apiKeys)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWS(hub, w, r, apiKeys)
	})
	httpServer := newWebsocketServer(cfg.listenAddr, mux)
	go func() {
		slog.Info("websocket: listening", "addr", cfg.listenAddr, "path", "/ws", "auth_enabled", len(apiKeys) > 0)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("websocket: http server error", "error", err)
		}
	}()

	// Prometheus /metrics endpoint, on its own port so scraping it never
	// shares a listener (or its request queue) with the WebSocket server.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := newMetricsServer(cfg.metricsListenAddr, metricsMux)
	go func() {
		slog.Info("metrics: listening", "addr", cfg.metricsListenAddr, "path", "/metrics")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics: http server error", "error", err)
		}
	}()

	// 3. MySQL connection and BulkInserters, each in its own goroutine.
	sqlDB, err := sql.Open("mysql", cfg.mysqlDSN)
	if err != nil {
		fatal("mysql: open failed", "error", err)
	}
	configureDBPool(sqlDB, cfg.mysqlMaxOpenConns)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := sqlDB.PingContext(pingCtx); err != nil {
		cancelPing()
		fatal("mysql: unreachable", "error", err)
	}
	cancelPing()

	// The Graphite client's Run loop is started and Flushed alongside every
	// BulkInserter below regardless of perfdataRoute; if perfdataRoute
	// excludes Graphite, NewRouter simply never calls Enqueue on it, so no
	// connection is ever dialed (CLAUDE.md rule 5).
	gc := graphite.NewClient(cfg.graphiteAddr)

	router, runners := queue.NewRouter(sqlDB, hub, gc, perfdataRoute, cfg.graphitePrefix, cfg.nodeName, cfg.enableOpenITCockpitTweaks)
	for _, r := range runners {
		wg.Add(1)
		go func(r queue.Runner) {
			defer wg.Done()
			r.Run(pipelineCtx)
		}(r)
	}

	// 4. The chosen queue consumer.
	var consumer queue.Consumer
	switch strings.ToLower(cfg.consumerBackend) {
	case "gearman":
		consumer = queue.NewGearmanConsumer(cfg.gearmanAddr, router, cfg.gearmanMaxConcurrentJobs)
	case "rabbitmq":
		consumer = queue.NewRabbitMQConsumer(cfg.rabbitMQURL, router, cfg.rabbitMQPrefetch)
	default:
		fatal("unknown -consumer value", "consumer", cfg.consumerBackend, "want", `"gearman" or "rabbitmq"`)
	}

	rawMessages, err := consumer.Start(pipelineCtx)
	if err != nil {
		fatal("consumer failed to start", "backend", cfg.consumerBackend, "error", err)
	}

	// Raw messages are for observability only (the real work already
	// happened inside the Router's Handlers); drain them so the channel's
	// buffer never fills up unnecessarily.
	go func() {
		for range rawMessages {
		}
	}()

	// 5. Graceful shutdown on SIGINT/SIGTERM.
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	<-sigCtx.Done()
	stopSignals()
	slog.Info("shutdown signal received, starting graceful shutdown")

	// 6a. Stop the queue consumer first so no new data comes in.
	if err := consumer.Stop(); err != nil {
		slog.Error("error stopping consumer", "backend", cfg.consumerBackend, "error", err)
	}

	// 6b. Flush every BulkInserter's remaining buffer immediately.
	flushStart := time.Now()
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), shutdownFlushTimeout)
	failed := flushRunners(flushCtx, runners)
	cancelFlush()
	slog.Info("shutdown flush complete",
		"duration", time.Since(flushStart), "runners", len(runners), "failed", failed)

	// 6c. Close the DB connection and the WebSocket hub cleanly.
	cancelPipeline() // stops the (now-empty) BulkInserters' Run loops and the Hub, closing all client connections
	wg.Wait()

	if err := sqlDB.Close(); err != nil {
		slog.Error("mysql: error closing connection", "error", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("websocket: error shutting down http server", "error", err)
	}
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics: error shutting down http server", "error", err)
	}
	cancelShutdown()

	slog.Info("shutdown complete")
}

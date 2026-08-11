// Command app wires together the queue consumer, the MySQL bulk-insert
// pipeline and the WebSocket broadcast hub described in CLAUDE.md.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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
	rabbitMQURL               string
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
	RabbitMQURL               string   `yaml:"rabbitmq_url"`
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
	flag.StringVar(&cfg.rabbitMQURL, "rabbitmq-url", "amqp://statusengine:statusengine@127.0.0.1:5672/",
		"RabbitMQ broker URL")
	flag.StringVar(&cfg.mysqlDSN, "mysql-dsn", "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine-dev?parseTime=true",
		"MySQL data source name")
	flag.IntVar(&cfg.mysqlMaxOpenConns, "mysql-max-open-conns", 25,
		"maximum number of open MySQL connections (also used as the idle-connection limit); "+
			"must stay below the server's max_connections")
	flag.StringVar(&cfg.listenAddr, "listen-addr", ":8080",
		"address the WebSocket HTTP server listens on")
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
	cfg.rabbitMQURL = resolveString(explicit, "rabbitmq-url", cfg.rabbitMQURL, "STATUSENGINE_RABBITMQ_URL", fc.RabbitMQURL)
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
// against. An empty raw yields an empty (nil) set, which ServeWS treats as
// "authentication disabled" - the worker's default.
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

	apiKeys := parseAPIKeys(cfg.apiKeys)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWS(hub, w, r, apiKeys)
	})
	httpServer := &http.Server{Addr: cfg.listenAddr, Handler: mux}
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
	metricsServer := &http.Server{Addr: cfg.metricsListenAddr, Handler: metricsMux}
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
		consumer = queue.NewGearmanConsumer(cfg.gearmanAddr, router)
	case "rabbitmq":
		consumer = queue.NewRabbitMQConsumer(cfg.rabbitMQURL, router)
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
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
	for _, r := range runners {
		if err := r.Flush(flushCtx); err != nil {
			slog.Error("error flushing runner on shutdown", "error", err)
		}
	}
	cancelFlush()
	slog.Info("shutdown flush complete", "duration", time.Since(flushStart), "runners", len(runners))

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

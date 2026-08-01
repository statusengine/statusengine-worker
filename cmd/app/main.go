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
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/queue"
	"statusengine-worker/internal/websocket"
)

// config holds every value that can be set via flag or environment
// variable (flag wins if both are given). Defaults match the local dev
// setup documented in .claude/specs/ressources.txt.
type config struct {
	consumerBackend string // "gearman" or "rabbitmq"
	gearmanAddr     string
	rabbitMQURL     string
	mysqlDSN        string
	listenAddr      string
	graphiteAddr    string
	perfdataRoute   string // "mysql", "graphite" or "both"
	logLevel        string // "debug", "info", "warn" or "error"
	logFormat       string // "text" or "json"
}

func loadConfig() config {
	cfg := config{}

	flag.StringVar(&cfg.consumerBackend, "consumer", envOrDefault("STATUSENGINE_CONSUMER", "gearman"),
		`queue backend to use: "gearman" or "rabbitmq"`)
	flag.StringVar(&cfg.gearmanAddr, "gearman-addr", envOrDefault("STATUSENGINE_GEARMAN_ADDR", "127.0.0.1:4730"),
		"Gearman job server address (host:port)")
	flag.StringVar(&cfg.rabbitMQURL, "rabbitmq-url", envOrDefault("STATUSENGINE_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/"),
		"RabbitMQ broker URL")
	flag.StringVar(&cfg.mysqlDSN, "mysql-dsn", envOrDefault("STATUSENGINE_MYSQL_DSN", "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine-dev?parseTime=true"),
		"MySQL data source name")
	flag.StringVar(&cfg.listenAddr, "listen-addr", envOrDefault("STATUSENGINE_LISTEN_ADDR", ":8080"),
		"address the WebSocket HTTP server listens on")
	flag.StringVar(&cfg.graphiteAddr, "graphite-addr", envOrDefault("STATUSENGINE_GRAPHITE_ADDR", "127.0.0.1:2003"),
		"Graphite Carbon plaintext receiver address (host:port)")
	flag.StringVar(&cfg.perfdataRoute, "perfdata-route", envOrDefault("STATUSENGINE_PERFDATA_ROUTE", "mysql"),
		`where statusngin_service_perfdata metrics are written: "mysql", "graphite" or "both" (CLAUDE.md rule 5)`)
	flag.StringVar(&cfg.logLevel, "log-level", envOrDefault("STATUSENGINE_LOG_LEVEL", "info"),
		`minimum log level: "debug", "info", "warn" or "error"`)
	flag.StringVar(&cfg.logFormat, "log-format", envOrDefault("STATUSENGINE_LOG_FORMAT", "text"),
		`structured log output format: "text" or "json"`)
	flag.Parse()

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

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := loadConfig()
	setupLogger(cfg)

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

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWS(hub, w, r)
	})
	httpServer := &http.Server{Addr: cfg.listenAddr, Handler: mux}
	go func() {
		slog.Info("websocket: listening", "addr", cfg.listenAddr, "path", "/ws")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("websocket: http server error", "error", err)
		}
	}()

	// 3. MySQL connection and BulkInserters, each in its own goroutine.
	sqlDB, err := sql.Open("mysql", cfg.mysqlDSN)
	if err != nil {
		fatal("mysql: open failed", "error", err)
	}
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

	router, runners := queue.NewRouter(sqlDB, hub, gc, perfdataRoute)
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
	cancelShutdown()

	slog.Info("shutdown complete")
}

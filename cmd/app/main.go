// Command app wires together the queue consumer, the MySQL bulk-insert
// pipeline and the WebSocket broadcast hub described in CLAUDE.md.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

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
	flag.Parse()

	return cfg
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := loadConfig()

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
		log.Printf("websocket: listening on %s (path /ws)", cfg.listenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("websocket: HTTP server error: %v", err)
		}
	}()

	// 3. MySQL connection and BulkInserters, each in its own goroutine.
	sqlDB, err := sql.Open("mysql", cfg.mysqlDSN)
	if err != nil {
		log.Fatalf("mysql: open %q: %v", cfg.mysqlDSN, err)
	}
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := sqlDB.PingContext(pingCtx); err != nil {
		cancelPing()
		log.Fatalf("mysql: unreachable: %v", err)
	}
	cancelPing()

	router, runners := queue.NewRouter(sqlDB, hub)
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
		log.Fatalf(`unknown -consumer %q: want "gearman" or "rabbitmq"`, cfg.consumerBackend)
	}

	rawMessages, err := consumer.Start(pipelineCtx)
	if err != nil {
		log.Fatalf("%s: failed to start: %v", cfg.consumerBackend, err)
	}
	log.Printf("%s: consumer started", cfg.consumerBackend)

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
	log.Println("shutdown signal received, starting graceful shutdown")

	// 6a. Stop the queue consumer first so no new data comes in.
	if err := consumer.Stop(); err != nil {
		log.Printf("%s: error stopping consumer: %v", cfg.consumerBackend, err)
	}

	// 6b. Flush every BulkInserter's remaining buffer immediately.
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
	for _, r := range runners {
		if err := r.Flush(flushCtx); err != nil {
			log.Printf("mysql: error flushing bulk inserter: %v", err)
		}
	}
	cancelFlush()

	// 6c. Close the DB connection and the WebSocket hub cleanly.
	cancelPipeline() // stops the (now-empty) BulkInserters' Run loops and the Hub, closing all client connections
	wg.Wait()

	if err := sqlDB.Close(); err != nil {
		log.Printf("mysql: error closing connection: %v", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("websocket: error shutting down HTTP server: %v", err)
	}
	cancelShutdown()

	log.Println("shutdown complete")
}

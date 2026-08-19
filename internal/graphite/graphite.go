// Package graphite implements a non-blocking, buffered client for
// Graphite's plaintext Carbon protocol ("<path> <value> <timestamp>\n" per
// line, over TCP). It ships the time-series metrics extracted from
// perfdata (CLAUDE.md rule 5) and mirrors internal/db's BulkInserter: a
// ticker- and batch-driven buffer decoupled from the rest of the pipeline
// by a channel, so a slow or unreachable Graphite endpoint never
// backpressures ingestion (CLAUDE.md rule 2).
package graphite

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	metricsPkg "statusengine-worker/internal/metrics"
)

const (
	// DefaultMaxBatchSize is the buffer capacity that triggers an immediate
	// flush when nothing else is configured. Override with WithMaxBatchSize.
	DefaultMaxBatchSize = 100

	// MaxConfigurableBatchSize is the hard ceiling WithMaxBatchSize accepts.
	// Unlike its counterpart in internal/db there is no protocol limit
	// behind it: a batch is one Write of newline-delimited plaintext lines,
	// and Carbon reads them line by line, so even the drain overshoot -
	// drainPending can push the buffer to just under 2*n, i.e. ~140 KB of
	// lines here - is nothing a TCP socket minds.
	//
	// The ceiling exists for the other reason only: flushBuffer drops the
	// entire batch on a failed dial or write and leaves re-dialling to the
	// next flush, so everything buffered at that moment is lost at once.
	// That loss grows linearly with this number, and Graphite metrics are
	// the one part of the pipeline with no at-least-once redelivery behind
	// it.
	MaxConfigurableBatchSize = 1000

	// FlushInterval is the ticker period that triggers a flush of whatever
	// is buffered, even if the batch size hasn't been reached.
	FlushInterval = 250 * time.Millisecond

	// StatsLogInterval is how often a running client summarises what it
	// shipped, and is the only thing this package logs at Info during
	// normal operation. Same value as internal/db's, so a log read at Info
	// has one rhythm rather than several.
	StatsLogInterval = 30 * time.Second

	dialTimeout  = 5 * time.Second
	writeTimeout = 5 * time.Second
)

// Option configures a Client at construction time.
type Option func(*clientOptions)

type clientOptions struct {
	maxBatchSize int
}

// WithMaxBatchSize sets how many buffered metrics trigger an immediate
// flush, instead of DefaultMaxBatchSize. Values outside
// [1, MaxConfigurableBatchSize] are clamped rather than rejected - cmd/app
// validates the configured value up front and refuses to start, so an
// operator sees the flag name and the accepted range instead of a silently
// different batch size.
func WithMaxBatchSize(n int) Option {
	return func(o *clientOptions) {
		if n < 1 {
			n = 1
		}
		if n > MaxConfigurableBatchSize {
			n = MaxConfigurableBatchSize
		}
		o.maxBatchSize = n
	}
}

// Metric is one Graphite plaintext-protocol data point.
type Metric struct {
	Path      string
	Value     float64
	Timestamp int64
}

type flushRequest struct {
	ctx   context.Context
	reply chan error
}

// Client batches Metrics received via Enqueue and writes them to a
// Graphite Carbon receiver as newline-delimited plaintext lines over a
// persistent TCP connection. The connection is dialed lazily on first
// flush and automatically re-dialed after a write error (CLAUDE.md rule
// 6), so a Client can be constructed and its Run loop started even when
// perfdata routing doesn't currently send it anything.
type Client struct {
	addr string

	in       chan Metric
	flushReq chan flushRequest

	// maxBatchSize is how many buffered metrics trigger an immediate flush,
	// set once at construction and never changed afterwards. It also sizes
	// in and buffer, so the whole client scales off this one number.
	maxBatchSize int

	buffer []Metric
	conn   net.Conn

	// writeBuf holds the rendered plaintext lines of the batch currently
	// being flushed. Kept on the Client and reset with [:0] rather than
	// built fresh each time, so a flush neither reallocates the whole
	// batch nor copies it a second time on the way to the socket. It
	// settles at the size of the largest batch seen - a few KB at
	// a full batch of lines - and stays there. Owned by Run's goroutine,
	// like buffer and conn.
	writeBuf []byte

	// flushesSinceStats/metricsSinceStats accumulate what the periodic
	// stats line reports, and are reset every time it fires. Plain ints,
	// no atomics: every flushBuffer call happens inside Run's goroutine,
	// which is also the goroutine that logs them.
	flushesSinceStats uint64
	metricsSinceStats uint64

	// processed is the running total of metrics successfully written,
	// reported on every flush log line. Only ever incremented from
	// flushBuffer, which always runs inside Run's single goroutine -
	// atomic only for safe reads from outside that goroutine.
	processed atomic.Uint64
}

// NewClient creates a Client that writes to the Graphite Carbon receiver
// at addr (host:port). Run must be started in its own goroutine before
// enqueued metrics are actually flushed. Pass WithMaxBatchSize to flush at
// something other than DefaultMaxBatchSize metrics.
func NewClient(addr string, opts ...Option) *Client {
	o := clientOptions{maxBatchSize: DefaultMaxBatchSize}
	for _, opt := range opts {
		opt(&o)
	}

	return &Client{
		addr:         addr,
		maxBatchSize: o.maxBatchSize,
		in:           make(chan Metric, o.maxBatchSize),
		flushReq:     make(chan flushRequest),
		// 2*maxBatchSize, not maxBatchSize: drainPending can top up an
		// almost-full buffer with everything sitting in c.in, which holds
		// maxBatchSize itself. Sizing for the real maximum keeps the flush
		// path free of reallocations - the alternative is a one-time grow
		// to exactly this size the first time a drain lands on a full
		// buffer, after which the backing array stays this large anyway.
		buffer: make([]Metric, 0, 2*o.maxBatchSize),
	}
}

// Enqueue hands a metric to the client's buffer, blocking only until
// either it is accepted or ctx is cancelled.
func (c *Client) Enqueue(ctx context.Context, m Metric) error {
	select {
	case c.in <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush requests an immediate write of whatever is currently buffered and
// waits for it to complete, using ctx for both the request and the
// resulting write. This is the method a graceful-shutdown sequence calls
// to drain the buffer before the process exits (CLAUDE.md rule 6).
func (c *Client) Flush(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case c.flushReq <- flushRequest{ctx: ctx, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run owns the buffer and the TCP connection, driving both until ctx is
// cancelled or the input channel is closed, at which point it performs
// one last best-effort flush before closing the connection and returning.
// It must run in exactly one goroutine.
func (c *Client) Run(ctx context.Context) {
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	statsTicker := time.NewTicker(StatsLogInterval)
	defer statsTicker.Stop()

	defer c.closeConn()

	// One last summary on the way out, so metrics shipped since the
	// previous tick are still reported rather than lost because the
	// shutdown happened to land mid-interval.
	defer c.logStats()

	for {
		select {
		case <-ctx.Done():
			// Mirrors db.BulkInserter.Run: ctx.Done() can become ready in
			// the same instant a metric is sitting in c.in (already
			// accepted by Enqueue but not yet read here), so drain it
			// before the final flush to avoid silently dropping it.
			c.drainPending()
			c.finalFlush()
			return

		case m, ok := <-c.in:
			if !ok {
				c.drainPending()
				c.finalFlush()
				return
			}
			c.buffer = append(c.buffer, m)
			if len(c.buffer) >= c.maxBatchSize {
				c.flushBuffer(ctx)
				ticker.Reset(FlushInterval)
			}

		case <-ticker.C:
			c.flushBuffer(ctx)
			ticker.Reset(FlushInterval)

		case <-statsTicker.C:
			c.logStats()

		case req := <-c.flushReq:
			c.drainPending()
			err := c.flushBuffer(req.ctx)
			ticker.Reset(FlushInterval)
			req.reply <- err
		}
	}
}

// drainPending moves any metrics already sitting in the input channel's
// buffer into c.buffer without blocking, so a shutdown racing with an
// in-flight Enqueue never silently loses that metric.
//
// This deliberately appends past maxBatchSize: the point is to lose
// nothing, and the overshoot is bounded by c.in's own capacity, so
// c.buffer peaks just under 2*maxBatchSize - which is what NewClient
// sizes it for.
func (c *Client) drainPending() {
	for {
		select {
		case m := <-c.in:
			c.buffer = append(c.buffer, m)
		default:
			return
		}
	}
}

// finalFlush is used on shutdown, when Run's own ctx is already cancelled
// and therefore unusable for a write - it gives the last batch a bounded
// window of its own instead of dropping it silently.
func (c *Client) finalFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.flushBuffer(ctx)
}

// logStats emits one summary of what this client has shipped since the
// last call, and is the periodic Info-level replacement for logging every
// individual flush.
//
// Silent when nothing was written, so a worker with perfdata routed to
// MySQL only - the default - never mentions Graphite at all rather than
// reporting zeroes forever. Only ever called from Run's goroutine, like
// the counters it reads.
func (c *Client) logStats() {
	if c.flushesSinceStats == 0 {
		return
	}

	slog.Info("graphite: write stats",
		"addr", c.addr,
		"flushes", c.flushesSinceStats,
		"metrics", c.metricsSinceStats,
		"metrics_per_flush", c.metricsSinceStats/c.flushesSinceStats,
		"total_processed", c.processed.Load())

	c.flushesSinceStats = 0
	c.metricsSinceStats = 0
}

// flushBuffer writes the buffered metrics as newline-delimited plaintext
// lines in a single Write call and clears the buffer, reusing its
// underlying array. A failed dial or write is logged and the batch is
// dropped rather than retried indefinitely, since retrying here would
// either block the pipeline or grow the buffer unbounded; the next flush
// will simply try to re-dial.
//
// Every flush is logged exactly once, at most every FlushInterval (or
// a full batch of metrics) - never per metric - so structured logging never
// adds per-message overhead to the hot ingestion path (CLAUDE.md rule 2).
func (c *Client) flushBuffer(ctx context.Context) error {
	if len(c.buffer) == 0 {
		return nil
	}
	metrics := len(c.buffer)

	if err := c.ensureConn(ctx); err != nil {
		slog.Error("graphite: dial failed, metrics dropped", "addr", c.addr, "metrics", metrics, "error", err)
		metricsPkg.PipelineErrorsTotal.WithLabelValues(metricsPkg.ComponentGraphite).Inc()
		c.buffer = c.buffer[:0]
		return err
	}

	// Render straight into the reusable buffer, appending each number in
	// place rather than going through FormatFloat/FormatInt, which would
	// allocate a throwaway string per metric. 'f' with precision -1 is the
	// shortest representation that round-trips, and never exponent
	// notation - Carbon does not accept that.
	buf := c.writeBuf[:0]
	for _, m := range c.buffer {
		buf = append(buf, m.Path...)
		buf = append(buf, ' ')
		buf = strconv.AppendFloat(buf, m.Value, 'f', -1, 64)
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, m.Timestamp, 10)
		buf = append(buf, '\n')
	}
	c.writeBuf = buf

	start := time.Now()
	c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.conn.Write(buf)
	duration := time.Since(start)

	if err != nil {
		slog.Error("graphite: write failed, metrics dropped",
			"addr", c.addr, "metrics", metrics, "duration", duration, "error", err)
		metricsPkg.PipelineErrorsTotal.WithLabelValues(metricsPkg.ComponentGraphite).Inc()
		c.closeConn()
	} else {
		total := c.processed.Add(uint64(metrics))
		c.flushesSinceStats++
		c.metricsSinceStats += uint64(metrics)

		// Debug, not Info: one line per flush, and a flush happens every
		// time the batch fills or the 250ms ticker expires with anything
		// buffered - several a second on a busy perfdata queue, which
		// fills a systemd journal fast. logStats is the Info-level
		// summary; this stays available at -log-level debug.
		slog.Debug("graphite: metrics flushed",
			"addr", c.addr, "metrics", metrics, "duration", duration, "total_processed", total)
	}

	c.buffer = c.buffer[:0]
	return err
}

// ensureConn dials addr if there is no live connection yet.
func (c *Client) ensureConn(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) closeConn() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

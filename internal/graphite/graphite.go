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
	// MaxBatchSize is the buffer capacity that triggers an immediate flush.
	MaxBatchSize = 100
	// FlushInterval is the ticker period that triggers a flush of whatever
	// is buffered, even if MaxBatchSize hasn't been reached.
	FlushInterval = 250 * time.Millisecond

	dialTimeout  = 5 * time.Second
	writeTimeout = 5 * time.Second
)

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

	buffer []Metric
	conn   net.Conn

	// writeBuf holds the rendered plaintext lines of the batch currently
	// being flushed. Kept on the Client and reset with [:0] rather than
	// built fresh each time, so a flush neither reallocates the whole
	// batch nor copies it a second time on the way to the socket. It
	// settles at the size of the largest batch seen - a few KB at
	// MaxBatchSize lines - and stays there. Owned by Run's goroutine,
	// like buffer and conn.
	writeBuf []byte

	// processed is the running total of metrics successfully written,
	// reported on every flush log line. Only ever incremented from
	// flushBuffer, which always runs inside Run's single goroutine -
	// atomic only for safe reads from outside that goroutine.
	processed atomic.Uint64
}

// NewClient creates a Client that writes to the Graphite Carbon receiver
// at addr (host:port). Run must be started in its own goroutine before
// enqueued metrics are actually flushed.
func NewClient(addr string) *Client {
	return &Client{
		addr:     addr,
		in:       make(chan Metric, MaxBatchSize),
		flushReq: make(chan flushRequest),
		// 2*MaxBatchSize, not MaxBatchSize: drainPending can top up an
		// almost-full buffer with everything sitting in c.in, which holds
		// MaxBatchSize itself. Sizing for the real maximum keeps the flush
		// path free of reallocations - the alternative is a one-time grow
		// to exactly this size the first time a drain lands on a full
		// buffer, after which the backing array stays this large anyway.
		buffer: make([]Metric, 0, 2*MaxBatchSize),
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
	defer c.closeConn()

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
			if len(c.buffer) >= MaxBatchSize {
				c.flushBuffer(ctx)
				ticker.Reset(FlushInterval)
			}

		case <-ticker.C:
			c.flushBuffer(ctx)
			ticker.Reset(FlushInterval)

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
// This deliberately appends past MaxBatchSize: the point is to lose
// nothing, and the overshoot is bounded by c.in's own capacity, so
// c.buffer peaks just under 2*MaxBatchSize - which is what NewClient
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

// flushBuffer writes the buffered metrics as newline-delimited plaintext
// lines in a single Write call and clears the buffer, reusing its
// underlying array. A failed dial or write is logged and the batch is
// dropped rather than retried indefinitely, since retrying here would
// either block the pipeline or grow the buffer unbounded; the next flush
// will simply try to re-dial.
//
// Every flush is logged exactly once, at most every FlushInterval (or
// MaxBatchSize metrics) - never per metric - so structured logging never
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
		slog.Info("graphite: metrics flushed",
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

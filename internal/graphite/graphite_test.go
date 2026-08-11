package graphite

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// These tests pin the exact bytes this package puts on the wire. Carbon's
// plaintext protocol is "<path> <value> <timestamp>\n" per line and there
// is no handshake or acknowledgement to catch a malformed line - a wrong
// separator or a reformatted float is simply accepted and stored under the
// wrong value, or silently dropped by the receiver. So the line format
// needs a test of its own, independent of how flushBuffer happens to
// assemble it.

// carbonReceiver is a stand-in Carbon endpoint: it accepts one connection
// and hands whatever is written to it back line by line.
type carbonReceiver struct {
	addr  string
	lines chan string
}

func newCarbonReceiver(t *testing.T) *carbonReceiver {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	r := &carbonReceiver{addr: ln.Addr().String(), lines: make(chan string, 64)}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			r.lines <- scanner.Text()
		}
	}()
	return r
}

// expect reads the next n lines the receiver saw, failing on timeout.
func (r *carbonReceiver) expect(t *testing.T, n int) []string {
	t.Helper()

	got := make([]string, 0, n)
	for i := 0; i < n; i++ {
		select {
		case line := <-r.lines:
			got = append(got, line)
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for line %d of %d; got so far: %v", i+1, n, got)
		}
	}
	return got
}

func TestFlushWritesCarbonPlaintextLines(t *testing.T) {
	recv := newCarbonReceiver(t)
	c := NewClient(recv.addr)

	c.buffer = append(c.buffer,
		Metric{Path: "statusengine.localhost.Ping.rta", Value: 0.069, Timestamp: 1700000000},
		// Trailing zeros must not come back: FormatFloat with precision -1
		// renders the shortest representation that round-trips.
		Metric{Path: "statusengine.localhost.Ping.pl", Value: 0, Timestamp: 1700000001},
		Metric{Path: "statusengine.localhost.Load.load1", Value: -1.5, Timestamp: 1700000002},
		// A large value must not flip into exponent notation, which Carbon
		// does not accept.
		Metric{Path: "statusengine.localhost.Disk.used", Value: 123456789012, Timestamp: 1700000003},
	)

	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("flushBuffer: %v", err)
	}

	want := []string{
		"statusengine.localhost.Ping.rta 0.069 1700000000",
		"statusengine.localhost.Ping.pl 0 1700000001",
		"statusengine.localhost.Load.load1 -1.5 1700000002",
		"statusengine.localhost.Disk.used 123456789012 1700000003",
	}
	got := recv.expect(t, len(want))
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}

	if len(c.buffer) != 0 {
		t.Errorf("buffer holds %d metrics after a flush, want 0", len(c.buffer))
	}
	if got := c.processed.Load(); got != uint64(len(want)) {
		t.Errorf("processed = %d, want %d", got, len(want))
	}
}

// TestConsecutiveFlushesDoNotLeakBetweenBatches guards the write path
// against carrying anything over from one batch to the next - the failure
// mode a buffer reused across flushes would introduce, where a shorter
// second batch leaves the tail of the first one behind.
func TestConsecutiveFlushesDoNotLeakBetweenBatches(t *testing.T) {
	recv := newCarbonReceiver(t)
	c := NewClient(recv.addr)

	// A deliberately long first batch, then a single short line.
	for i := 0; i < 20; i++ {
		c.buffer = append(c.buffer, Metric{
			Path:      "statusengine.a-fairly-long-hostname.example.org.Service.metric",
			Value:     float64(i) + 0.5,
			Timestamp: 1700000000 + int64(i),
		})
	}
	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("first flushBuffer: %v", err)
	}
	recv.expect(t, 20)

	c.buffer = append(c.buffer, Metric{Path: "s.h.S.m", Value: 1, Timestamp: 1700000100})
	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("second flushBuffer: %v", err)
	}

	got := recv.expect(t, 1)
	if want := "s.h.S.m 1 1700000100"; got[0] != want {
		t.Fatalf("second flush wrote %q, want %q", got[0], want)
	}

	// Nothing further may arrive: a leaked tail would show up here.
	select {
	case extra := <-recv.lines:
		t.Fatalf("unexpected extra line after the second flush: %q", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestFlushOnEmptyBufferIsANoop(t *testing.T) {
	c := NewClient("127.0.0.1:1") // nothing listens; must not even be dialed
	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("flushing an empty buffer should do nothing, got: %v", err)
	}
	if c.conn != nil {
		t.Error("an empty flush dialed a connection")
	}
}

func TestFlushDropsBatchWhenDialFails(t *testing.T) {
	c := NewClient("127.0.0.1:1") // nothing listens here
	c.buffer = append(c.buffer, Metric{Path: "a.b.c", Value: 1, Timestamp: 1})

	err := c.flushBuffer(context.Background())
	if err == nil {
		t.Fatal("expected a dial error")
	}
	if !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "refused") {
		t.Logf("dial error was %v (accepted, platform-dependent wording)", err)
	}
	// The batch must be dropped rather than retried forever - see the
	// flushBuffer doc comment.
	if len(c.buffer) != 0 {
		t.Errorf("buffer holds %d metrics after a failed dial, want 0", len(c.buffer))
	}
}

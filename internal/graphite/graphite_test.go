package graphite

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync/atomic"
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

// multiConnReceiver is a Carbon stand-in that accepts any number of
// connections, counting them. The first blackholeFirst connections are held
// open and never read - the kernel still ACKs whatever is written to them,
// which is exactly what makes them look healthy to the client. Every later
// connection is read line by line into lines, so expect works as on
// carbonReceiver.
type multiConnReceiver struct {
	carbonReceiver
	accepted atomic.Int32
}

func newMultiConnReceiver(t *testing.T, blackholeFirst int) *multiConnReceiver {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	r := &multiConnReceiver{carbonReceiver: carbonReceiver{addr: ln.Addr().String(), lines: make(chan string, 64)}}
	var held []net.Conn
	done := make(chan struct{})
	t.Cleanup(func() {
		ln.Close()
		<-done
		for _, c := range held {
			c.Close()
		}
	})

	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			if int(r.accepted.Add(1)) <= blackholeFirst {
				held = append(held, conn)
				continue
			}
			go func() {
				defer conn.Close()
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					r.lines <- scanner.Text()
				}
			}()
		}
	}()
	return r
}

// TestConnIsReplacedAfterMaxLifetime is the incident in
// .claude/specs/problem.txt: after a reboot the worker's one connection led
// into a peer that accepted the bytes and never delivered them, every write
// succeeded, and nothing reached Carbon until the worker was restarted by
// hand. With a max lifetime the next flush after it expires must dial a
// fresh connection and get through.
func TestConnIsReplacedAfterMaxLifetime(t *testing.T) {
	recv := newMultiConnReceiver(t, 1)
	c := NewClient(recv.addr, WithConnMaxLifetime(time.Hour))

	c.buffer = append(c.buffer, Metric{Path: "lost.in.the.void", Value: 1, Timestamp: 1})
	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("a write into a black hole must succeed - that is the whole problem - got: %v", err)
	}
	select {
	case line := <-recv.lines:
		t.Fatalf("black hole delivered %q", line)
	case <-time.After(200 * time.Millisecond):
	}

	// Age the connection past its lifetime instead of sleeping through it.
	c.connSince = time.Now().Add(-time.Hour)

	c.buffer = append(c.buffer, Metric{Path: "arrives", Value: 2, Timestamp: 2})
	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("flushBuffer: %v", err)
	}
	if got := recv.expect(t, 1)[0]; got != "arrives 2 2" {
		t.Fatalf("got %q, want %q", got, "arrives 2 2")
	}
	if n := recv.accepted.Load(); n != 2 {
		t.Errorf("accepted %d connections, want 2", n)
	}
}

// TestConnIsReusedWithinMaxLifetime pins the other half: the lifetime is a
// bound, not a dial per flush. A flush every 250ms on a healthy connection
// must keep using it.
func TestConnIsReusedWithinMaxLifetime(t *testing.T) {
	recv := newMultiConnReceiver(t, 0)
	c := NewClient(recv.addr, WithConnMaxLifetime(time.Hour))

	for i := int64(1); i <= 3; i++ {
		c.buffer = append(c.buffer, Metric{Path: "m", Value: float64(i), Timestamp: i})
		if err := c.flushBuffer(context.Background()); err != nil {
			t.Fatalf("flushBuffer %d: %v", i, err)
		}
	}
	recv.expect(t, 3)
	if n := recv.accepted.Load(); n != 1 {
		t.Errorf("accepted %d connections for three flushes within the lifetime, want 1", n)
	}
}

// TestZeroMaxLifetimeKeepsConnection: 0 switches the limit off, which is
// the behaviour from before it existed - a connection is replaced only
// after a failed write.
func TestZeroMaxLifetimeKeepsConnection(t *testing.T) {
	recv := newMultiConnReceiver(t, 0)
	c := NewClient(recv.addr, WithConnMaxLifetime(0))

	c.buffer = append(c.buffer, Metric{Path: "m", Value: 1, Timestamp: 1})
	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("flushBuffer: %v", err)
	}
	c.connSince = time.Now().Add(-365 * 24 * time.Hour)
	c.buffer = append(c.buffer, Metric{Path: "m", Value: 2, Timestamp: 2})
	if err := c.flushBuffer(context.Background()); err != nil {
		t.Fatalf("flushBuffer: %v", err)
	}

	recv.expect(t, 2)
	if n := recv.accepted.Load(); n != 1 {
		t.Errorf("accepted %d connections with the lifetime disabled, want 1", n)
	}
}

func TestConnMaxLifetimeOption(t *testing.T) {
	if got := NewClient("x").connMaxLifetime; got != DefaultConnMaxLifetime {
		t.Errorf("default = %s, want %s", got, DefaultConnMaxLifetime)
	}
	if got := NewClient("x", WithConnMaxLifetime(-time.Second)).connMaxLifetime; got != 0 {
		t.Errorf("negative lifetime = %s, want 0 (disabled)", got)
	}
}

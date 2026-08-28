package queue

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	gearmanClient "github.com/mikespook/gearman-go/client"
	"github.com/prometheus/client_golang/prometheus"
)

// tcpBreaker is a minimal TCP proxy that a test can sit in front of the
// job server in order to sever the connection on demand.
//
// It exists because there is no other way to reach the connection from a
// test. The RabbitMQ reconnect test simply calls Close on the consumer's
// own *amqp.Connection; gearman-go keeps its socket in an unexported field
// of an unexported agent, with nothing exported that leads to it. The only
// alternatives would be restarting the real gearmand - which a test must
// not do to a shared dev service - or trusting that the code path is right
// because it looks right, which is what left this bug in place.
type tcpBreaker struct {
	ln      net.Listener
	backend string

	mu     sync.Mutex
	conns  []net.Conn
	closed bool
}

// newTCPBreaker starts a proxy forwarding to backend and returns it. Every
// connection it accepts, and every one it opens to the backend, is
// remembered so that breakAll can drop them all at once.
func newTCPBreaker(t *testing.T, backend string) *tcpBreaker {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	b := &tcpBreaker{ln: ln, backend: backend}
	go b.acceptLoop()
	t.Cleanup(b.close)

	return b
}

func (b *tcpBreaker) addr() string { return b.ln.Addr().String() }

func (b *tcpBreaker) acceptLoop() {
	for {
		client, err := b.ln.Accept()
		if err != nil {
			return // listener closed
		}

		server, err := net.Dial("tcp", b.backend)
		if err != nil {
			client.Close()
			continue
		}

		if !b.track(client, server) {
			client.Close()
			server.Close()
			return
		}

		go func() { io.Copy(server, client); server.Close() }()
		go func() { io.Copy(client, server); client.Close() }()
	}
}

// track remembers one connection pair, reporting false if the breaker has
// already been closed - in which case the caller must drop the pair itself
// rather than leaving it running past the end of the test.
func (b *tcpBreaker) track(conns ...net.Conn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return false
	}
	b.conns = append(b.conns, conns...)
	return true
}

// waitForConnection blocks until the proxy has accepted and paired at
// least one connection.
//
// Necessary because a consumer's Start can return before this proxy has
// even accepted: the dial into the listener's backlog succeeds
// immediately, and the CAN_DO that follows goes into the socket buffer, so
// Ready reports success while acceptLoop is still dialling the backend.
// breakAll called at that moment finds an empty list, breaks nothing, and
// the test then waits out its timeout on a connection that was never
// severed - which is exactly how this failed roughly one run in six before
// the wait existed.
func (b *tcpBreaker) waitForConnection(t *testing.T) {
	t.Helper()

	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		b.mu.Lock()
		n := len(b.conns)
		b.mu.Unlock()

		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the proxy never accepted a connection from the consumer")
		}
	}
}

// breakAll drops every connection currently proxied, which is what the
// consumer sees as its job server going away. New connections are still
// accepted afterwards, so a reconnect can succeed.
func (b *tcpBreaker) breakAll() {
	b.mu.Lock()
	conns := b.conns
	b.conns = nil
	b.mu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}

// stopAccepting closes the listener while leaving the breaker usable, so a
// test can make every reconnect attempt fail rather than merely be slow.
func (b *tcpBreaker) stopAccepting() { b.ln.Close() }

func (b *tcpBreaker) close() {
	b.ln.Close()

	b.mu.Lock()
	b.closed = true
	conns := b.conns
	b.conns = nil
	b.mu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}

// TestGearmanConsumerReconnectsAfterConnectionDrop is the regression test
// for a worker that stopped consuming for good when it lost the job server.
//
// The library does not recover on its own and does not pretend to: on EOF
// agent.work hands a *WorkerDisconnectError to the ErrorHandler and
// returns, so nothing sends on worker.in any more and Work() - which is a
// plain range over that channel - parks there forever. The consumer stayed
// up, kept logging its stats line, and processed nothing: twelve WARN lines
// and then silence until someone restarted it by hand.
//
// So this asserts the only thing that distinguishes a fix from a
// well-argued comment: a job published *after* the drop still reaches its
// Handler.
func TestGearmanConsumerReconnectsAfterConnectionDrop(t *testing.T) {
	breaker := newTCPBreaker(t, gearmanAddr)

	// Unique per run: gearmand is shared here, and a leftover job from an
	// earlier run must not be able to satisfy the assertion below.
	fnName := fmt.Sprintf("queue_pkg_test_reconnect_%d", time.Now().UnixNano())

	// Buffered for the same reason as in TestGearmanConsumerEndToEnd, and
	// with one extra consideration here: a job whose acknowledgement was
	// lost in the drop is redelivered after the reconnect, so the handler
	// can legitimately run more often than jobs were published.
	received := make(chan []byte, 64)
	router := Router{
		fnName: func(_ context.Context, payload []byte) error {
			received <- payload
			return nil
		},
	}

	consumer := NewGearmanConsumer(breaker.addr(), router, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := consumer.Start(ctx); err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}
	defer consumer.Stop()

	cli, err := gearmanClient.New(gearmanClient.Network, gearmanAddr)
	if err != nil {
		t.Fatalf("gearman client: %v", err)
	}
	defer cli.Close()

	// Establish that it works at all before breaking it, so a failure
	// below can only mean the reconnect.
	if _, err := cli.DoBg(fnName, []byte(`{"before":"drop"}`), gearmanClient.JobNormal); err != nil {
		t.Fatalf("submit job before the drop: %v", err)
	}
	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran before the connection was dropped")
	}

	breaker.waitForConnection(t)
	breaker.breakAll()

	// Wait for the supervisor to have finished, not merely started: it
	// counts the reconnect, closes the dead generation and only then
	// rebuilds, so publishing on the strength of the counter alone would
	// race the new CAN_DO. Poll for the connection instead, which is what
	// the queue_connected gauge exists to expose.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if consumer.reconnects.Load() > 0 && gatheredQueueConnected(t, fnName) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer did not reconnect within 30s of the connection being dropped "+
				"(reconnects=%d)", consumer.reconnects.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}

	if _, err := cli.DoBg(fnName, []byte(`{"after":"drop"}`), gearmanClient.JobNormal); err != nil {
		t.Fatalf("submit job after the drop: %v", err)
	}
	select {
	case <-received:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never ran after the reconnect - the consumer stopped consuming for good")
	}
}

// TestGearmanConsumerStopsWhileReconnecting pins the property the whole
// construction hangs on: the retry loop must never outlast a shutdown.
//
// It retries forever by design (the job server coming back is exactly the
// case worth waiting for), so the only thing keeping a Stop during an
// outage from running past systemd's TimeoutStopSec - which would SIGKILL
// the worker mid-flush and lose the buffered rows - is that closing
// c.stopping ends both the backoff wait and the loop itself.
func TestGearmanConsumerStopsWhileReconnecting(t *testing.T) {
	breaker := newTCPBreaker(t, gearmanAddr)

	fnName := fmt.Sprintf("queue_pkg_test_stop_reconnecting_%d", time.Now().UnixNano())
	router := Router{fnName: func(context.Context, []byte) error { return nil }}

	consumer := NewGearmanConsumer(breaker.addr(), router, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := consumer.Start(ctx); err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}

	// Drop the connection and make sure nothing can be dialled again, so
	// the supervisor is genuinely stuck in its retry loop rather than
	// recovering before Stop is called.
	breaker.waitForConnection(t)
	breaker.stopAccepting()
	breaker.breakAll()

	for deadline := time.Now().Add(10 * time.Second); consumer.reconnects.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("consumer never noticed the connection had dropped")
		}
		time.Sleep(50 * time.Millisecond)
	}

	stopped := make(chan error, 1)
	start := time.Now()
	go func() { stopped <- consumer.Stop() }()

	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
		t.Logf("Stop returned after %s while a reconnect was pending", time.Since(start).Round(time.Millisecond))
	case <-time.After(15 * time.Second):
		t.Fatal("Stop did not return while a reconnect was pending - a job-server outage would " +
			"hold the shutdown past TimeoutStopSec and cost the buffered rows")
	}
}

// gatheredQueueConnected reads statusengine_queue_connected for one queue
// through the gatherer, reporting -1 when the series does not exist at all
// (which is its state before any consumer has connected). Read this way
// rather than from the consumer, because there is no field to read - the
// gauge is the observable.
func gatheredQueueConnected(t *testing.T, queueName string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	for _, family := range families {
		if family.GetName() != "statusengine_queue_connected" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "queue_name" && pair.GetValue() == queueName {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	return -1
}

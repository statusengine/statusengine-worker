package command

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	gearmanworker "github.com/mikespook/gearman-go/worker"
	amqp "github.com/rabbitmq/amqp091-go"
)

// The dev services documented in .claude/specs/ressources.txt.
var (
	gearmanAddr  = envOrDefaultTest("STATUSENGINE_TEST_GEARMAN_ADDR", "127.0.0.1:4730")
	rabbitmqURL  = envOrDefaultTest("STATUSENGINE_TEST_RABBITMQ_URL", "amqp://statusengine:statusengine@127.0.0.1:5672/")
	requireEnvVa = "STATUSENGINE_TEST_REQUIRE_SERVICES"
)

func envOrDefaultTest(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// skipOrFailService is a deliberate twin of the helper in
// internal/queue/testservices_test.go, and must keep behaving identically:
// an unreachable dev service is a skip locally and a failure when
// STATUSENGINE_TEST_REQUIRE_SERVICES=1, which CI sets. A test that quietly
// skipped in CI would leave the only proof that a command actually reaches
// the broker unrun, while the job still printed ok.
func skipOrFailService(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, err := strconv.ParseBool(os.Getenv(requireEnvVa)); err == nil && required {
		t.Fatalf("%s=1 but this service is unreachable: "+format,
			append([]any{requireEnvVa}, args...)...)
	}
	t.Skipf(format, args...)
}

const testPayload = `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;publisher-test-host"}`

// A scratch queue rather than statusngin_cmd: consuming the real one would
// take a command away from a NEB module that happens to be running on this
// machine, which is both flaky and rude.
const testQueue = "statusngin_cmd_publisher_test"

// startTestConsumer registers a worker for the scratch function and returns
// a channel of what it receives.
//
// Every test that publishes must start one, and not only so it can assert
// that the job arrived: a Gearman job with no registered worker stays queued
// at the job server indefinitely. Publishing without consuming leaves the
// dev gearmand accumulating jobs for a function nothing will ever register
// again - the same leak the RabbitMQ fairness test once had, in the shape
// Gearman offers it.
func startTestConsumer(t *testing.T) <-chan []byte {
	t.Helper()
	received := make(chan []byte, 64)

	w := gearmanworker.New(gearmanworker.Unlimited)
	if err := w.AddServer(gearmanworker.Network, gearmanAddr); err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}
	w.ErrorHandler = func(e error) { t.Logf("gearman worker: %v", e) }
	if err := w.AddFunc(testQueue, func(job gearmanworker.Job) ([]byte, error) {
		select {
		case received <- job.Data():
		default:
		}
		return nil, nil
	}, gearmanworker.Unlimited); err != nil {
		t.Fatalf("register test function: %v", err)
	}
	if err := w.Ready(); err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}
	go w.Work()
	t.Cleanup(func() { w.Close() })
	return received
}

func TestGearmanPublisherReachesTheQueue(t *testing.T) {
	pub := NewGearmanPublisher(gearmanAddr)
	pub.queue = testQueue
	defer pub.Close()

	received := startTestConsumer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, []byte(testPayload)); err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}

	select {
	case got := <-received:
		if string(got) != testPayload {
			t.Errorf("payload = %s, want %s", got, testPayload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the published job never arrived at the worker")
	}
}

// The publisher is used from one HTTP handler that may serve concurrent
// requests, and it reuses a single connection behind a mutex. -race is what
// makes this test worth having.
func TestGearmanPublisherIsSafeForConcurrentUse(t *testing.T) {
	pub := NewGearmanPublisher(gearmanAddr)
	pub.queue = testQueue
	defer pub.Close()

	// Started before publishing, so these jobs are consumed rather than left
	// queued at the dev job server - see startTestConsumer.
	received := startTestConsumer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, []byte(testPayload)); err != nil {
		skipOrFailService(t, "no reachable dev Gearman job server at %s: %v", gearmanAddr, err)
	}

	const concurrent = 8
	errs := make(chan error, concurrent)
	for i := 0; i < concurrent; i++ {
		go func() { errs <- pub.Publish(ctx, []byte(testPayload)) }()
	}
	for i := 0; i < concurrent; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent publish: %v", err)
		}
	}

	// Drain what was published, so the test leaves the job server as it
	// found it. Everything must arrive: a job that does not is one left
	// queued behind.
	deadline := time.After(15 * time.Second)
	for i := 0; i < concurrent+1; i++ {
		select {
		case <-received:
		case <-deadline:
			t.Fatalf("only %d of %d published jobs were consumed - the rest are still queued at the job server",
				i, concurrent+1)
		}
	}
}

func TestRabbitMQPublisherReachesTheQueue(t *testing.T) {
	pub := NewRabbitMQPublisher(rabbitmqURL)
	pub.queue = testQueue
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, []byte(testPayload)); err != nil {
		skipOrFailService(t, "no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}

	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		skipOrFailService(t, "no reachable dev RabbitMQ broker at %s: %v", rabbitmqURL, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	// Teardown runs LIFO and after every defer in this function, so the
	// queue is removed while the connection is still open - the ordering
	// mistake that leaked queues in internal/queue once already.
	t.Cleanup(func() { conn.Close() })
	t.Cleanup(func() {
		if _, err := ch.QueueDelete(testQueue, false, false, false); err != nil {
			t.Errorf("cleanup: delete %s: %v - test queue was left on the broker", testQueue, err)
		}
		ch.Close()
	})

	deliveries, err := ch.Consume(testQueue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	select {
	case d := <-deliveries:
		if string(d.Body) != testPayload {
			t.Errorf("payload = %s, want %s", d.Body, testPayload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the published message never arrived on the queue")
	}
}

// An unreachable broker must surface as an error the handler can turn into
// a 503, and it must do so quickly - the caller is a waiting HTTP client,
// not a background flush.
func TestPublishersFailFastWhenUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for name, pub := range map[string]Publisher{
		"gearman":  NewGearmanPublisher("127.0.0.1:1"), // port 1: nothing listens
		"rabbitmq": NewRabbitMQPublisher("amqp://guest:guest@127.0.0.1:1/"),
	} {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			if err := pub.Publish(ctx, []byte(testPayload)); err == nil {
				t.Fatal("Publish() = nil, want an error against a dead broker")
			}
			if elapsed := time.Since(start); elapsed > 20*time.Second {
				t.Errorf("took %v to give up; an HTTP caller is waiting on this", elapsed)
			}
			if err := pub.Close(); err != nil {
				t.Errorf("Close() after a failed publish: %v", err)
			}
		})
	}
}

// Close must be safe without a successful publish and safe twice, because
// cmd/app calls it on every shutdown path including ones where the broker
// was never reachable.
func TestPublisherCloseIsSafeWithoutUse(t *testing.T) {
	for name, pub := range map[string]Publisher{
		"gearman":  NewGearmanPublisher(gearmanAddr),
		"rabbitmq": NewRabbitMQPublisher(rabbitmqURL),
	} {
		t.Run(name, func(t *testing.T) {
			if err := pub.Close(); err != nil {
				t.Errorf("first Close(): %v", err)
			}
			if err := pub.Close(); err != nil {
				t.Errorf("second Close(): %v", err)
			}
		})
	}
}

func TestNewPublisherRejectsAnUnknownBackend(t *testing.T) {
	if _, err := NewPublisher("kafka", gearmanAddr, rabbitmqURL); err == nil {
		t.Fatal("NewPublisher(\"kafka\") = nil error, want one")
	}
	for _, backend := range []string{"gearman", "rabbitmq"} {
		if _, err := NewPublisher(backend, gearmanAddr, rabbitmqURL); err != nil {
			t.Errorf("NewPublisher(%q) = %v", backend, err)
		}
	}
}

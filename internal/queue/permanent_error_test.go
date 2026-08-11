package queue

import (
	"context"
	"errors"
	"testing"

	"statusengine-worker/internal/websocket"
)

// errEnqueueFailed stands in for a transient persistence failure (MySQL
// briefly unreachable, buffer full) as opposed to a malformed payload.
var errEnqueueFailed = errors.New("mysql is having a moment")

type failingEnqueuer[P any] struct{}

func (failingEnqueuer[P]) Enqueue(context.Context, P) error { return errEnqueueFailed }

// TestDecodeFailuresArePermanent is the contract the RabbitMQ consumer's
// nack decision rests on: a payload that cannot be decoded must report
// itself as permanent, so it is dropped rather than requeued forever. If
// this ever regresses, a single malformed message silently turns into an
// infinite redelivery loop that starves its whole queue.
func TestDecodeFailuresArePermanent(t *testing.T) {
	hub := websocket.NewHub()
	ctx := context.Background()
	garbage := []byte(`{ this is not the payload you are looking for`)

	handlers := map[string]Handler{
		"NewHandler":          NewHandler(hub, QueueHostChecks, &fakeEnqueuer[hostCheckEvent]{}, decodeHostCheck),
		"NewBroadcastHandler": NewBroadcastHandler(hub, QueueHostChecks, decodeHostCheck),
		"NewPerfdataHandler": NewPerfdataHandler(hub, QueueServicePerfdata, PerfdataRouteMySQL,
			&fakeEnqueuer[perfdataMetric]{}, nil, "statusengine"),
	}

	for name, handler := range handlers {
		t.Run(name, func(t *testing.T) {
			err := handler(ctx, garbage)
			if err == nil {
				t.Fatal("expected a decode error for a malformed payload")
			}
			if !errors.Is(err, ErrPermanent) {
				t.Fatalf("decode error is not classified as permanent: %v", err)
			}
		})
	}
}

// TestTransientFailuresAreNotPermanent is the other half of that contract:
// a persistence failure must stay requeueable, or a MySQL hiccup would
// quietly discard every message that arrived during it.
func TestTransientFailuresAreNotPermanent(t *testing.T) {
	hub := websocket.NewHub()
	handler := NewHandler(hub, QueueHostChecks, failingEnqueuer[hostCheckEvent]{}, decodeHostCheck)

	err := handler(context.Background(), readFixture(t, "statusngin_hostchecks.json"))
	if err == nil {
		t.Fatal("expected the enqueue failure to surface")
	}
	if errors.Is(err, ErrPermanent) {
		t.Fatalf("a transient enqueue failure must not be classified as permanent: %v", err)
	}
	if !errors.Is(err, errEnqueueFailed) {
		t.Fatalf("the underlying enqueue error was not wrapped: %v", err)
	}
}

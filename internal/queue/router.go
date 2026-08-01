package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"statusengine-worker/internal/websocket"
)

// Handler decodes one raw queue payload and dispatches every item it
// contains to persistence and/or WebSocket subscribers. A Consumer calls
// the Handler registered for a queue name for every message it receives on
// that queue.
type Handler func(ctx context.Context, payload []byte) error

// Router maps a queue name (e.g. "statusngin_hoststatus") to the Handler
// responsible for decoding and dispatching its messages. Both the Gearman
// and RabbitMQ consumers are driven by the same Router, so the decode and
// dispatch logic for a given queue is written exactly once regardless of
// which broker delivers it.
type Router map[string]Handler

// Runner is the subset of *db.BulkInserter[T] the queue package needs to
// manage its lifecycle without depending on T: starting its Run loop and
// flushing it on graceful shutdown (CLAUDE.md rule 6). Every
// *db.BulkInserter[T], for any T, satisfies this interface.
type Runner interface {
	Run(ctx context.Context)
	Flush(ctx context.Context) error
}

// enqueuer is the subset of *db.BulkInserter[P] a Handler needs to persist
// a decoded item. Declared locally (rather than referencing *db.BulkInserter
// directly) so Handlers stay testable without a real database.
type enqueuer[P any] interface {
	Enqueue(ctx context.Context, item P) error
}

// NewHandler builds a Handler for a queue whose decoded items must reach
// both MySQL and WebSocket subscribers: decode turns the raw payload into
// its individual items; each item is enqueued on ins for bulk-insertion and
// published on hub under topic (CLAUDE.md rule 2 - the actual DB write and
// WebSocket dispatch happen asynchronously in ins's and hub's own
// goroutines, decoupled from this call by their respective channels).
func NewHandler[P any](hub *websocket.Hub, topic string, ins enqueuer[P], decode func([]byte) ([]P, error)) Handler {
	return func(ctx context.Context, payload []byte) error {
		items, err := decode(payload)
		if err != nil {
			return fmt.Errorf("queue: decode %s: %w", topic, err)
		}

		for _, item := range items {
			publish(hub, topic, item)

			if err := ins.Enqueue(ctx, item); err != nil {
				return fmt.Errorf("queue: enqueue %s event: %w", topic, err)
			}
		}
		return nil
	}
}

// NewBroadcastHandler builds a Handler for a queue with no MySQL
// destination (e.g. no matching table, or a not-yet-implemented routing
// target such as Graphite): every decoded item is only published to hub
// under topic.
func NewBroadcastHandler[P any](hub *websocket.Hub, topic string, decode func([]byte) ([]P, error)) Handler {
	return func(_ context.Context, payload []byte) error {
		items, err := decode(payload)
		if err != nil {
			return fmt.Errorf("queue: decode %s: %w", topic, err)
		}

		for _, item := range items {
			publish(hub, topic, item)
		}
		return nil
	}
}

// publish JSON-encodes item and publishes it to hub under topic, logging
// (rather than failing the whole dispatch) on encode errors - a single
// unencodable event must never take down the surrounding batch.
func publish[P any](hub *websocket.Hub, topic string, item P) {
	raw, err := json.Marshal(item)
	if err != nil {
		slog.Error("queue: failed to encode event for websocket", "topic", topic, "error", err)
		return
	}
	hub.Publish(topic, raw)
}

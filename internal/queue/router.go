package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"statusengine-worker/internal/websocket"
)

// ErrPermanent marks a Handler failure that will fail again identically no
// matter how often the same payload is redelivered - a malformed or
// structurally unexpected message, as opposed to a transient failure like
// MySQL being briefly unreachable. Consumers use errors.Is(err,
// ErrPermanent) to decide whether redelivering is worth anything: without
// that distinction a single undecodable message is nacked-and-requeued
// forever, spinning a CPU core, flooding the log and starving every healthy
// message behind it in the same queue.
var ErrPermanent = errors.New("permanent handler failure")

// decodeError wraps a decode failure so it satisfies errors.Is(err,
// ErrPermanent) while still carrying the topic and the original error for
// the log line. Every Handler that decodes a payload reports its decode
// failures through this one helper.
func decodeError(topic string, err error) error {
	return fmt.Errorf("queue: decode %s: %w: %w", topic, ErrPermanent, err)
}

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
			return decodeError(topic, err)
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
			return decodeError(topic, err)
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
//
// Encoding is skipped entirely when no client is connected. This sits on
// the hottest path in the worker - it runs once per decoded event, for
// every queue - and a production worker normally has nobody attached to
// /ws at all, so without this check every ingested event pays for a full
// reflection-driven marshal (and the garbage that comes with it) to
// produce bytes that are then immediately discarded by Publish.
func publish[P any](hub *websocket.Hub, topic string, item P) {
	if !hub.HasClients() {
		return
	}

	raw, err := json.Marshal(item)
	if err != nil {
		slog.Error("queue: failed to encode event for websocket", "topic", topic, "error", err)
		return
	}
	hub.Publish(topic, raw)
}

// Package websocket implements the pub/sub broadcast Hub that fans out
// events to subscribed clients without ever blocking the ingestion/DB
// pipeline (see CLAUDE.md rule 4).
package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"
)

// broadcastBufferSize is the depth of the Hub's inbound event buffer.
// Publish never blocks: once this buffer is full, further events are
// dropped rather than backpressuring the ingestion pipeline.
const broadcastBufferSize = 1024

// statsLogInterval is how often Run logs a summary of throughput and
// dropped-message counts. Per-event logging would put a log call on the
// hot dispatch path for every single message; aggregating into one line
// per interval keeps structured logging visible without slowing down
// broadcasting (CLAUDE.md rule 4).
const statsLogInterval = 30 * time.Second

// Event is a single message published by the ingestion pipeline, tagged
// with the topic (queue name, e.g. "statusngin_hoststatus") clients
// subscribe to.
type Event struct {
	Topic   string
	Payload []byte // already JSON-encoded event payload
}

// outboundMessage is the wire format a client receives: the topic the
// message was published under alongside its raw payload.
type outboundMessage struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// subscriptionUpdate carries a client's requested subscribe/unsubscribe
// changes into the Hub's single-goroutine state owner.
type subscriptionUpdate struct {
	client *Client
	sub    subscriptionMessage
}

// Hub maintains the set of registered clients and fans out published
// Events to the clients subscribed to each event's topic.
//
// All Hub state (clients map, per-client topic sets) is only ever touched
// from within Run's goroutine, so no locking is required - registration,
// unsubscription and dispatch are simply serialized through channels.
type Hub struct {
	broadcast          chan Event
	register           chan *Client
	unregister         chan *Client
	updateSubscription chan subscriptionUpdate

	clients map[*Client]struct{}

	// publishDropped counts events dropped by Publish because the
	// broadcast buffer was full. Publish is called from arbitrary
	// ingestion goroutines (not Run's), so this one counter is atomic;
	// every other stat below is only ever touched from within Run and
	// needs no synchronization.
	publishDropped atomic.Uint64

	// received/dispatched/dropped track broadcast throughput for the
	// periodic stats log. dropped counts per-client sends skipped because
	// that client's own send buffer was full (CLAUDE.md rule 4's
	// non-blocking dispatch), distinct from publishDropped above.
	received   uint64
	dispatched uint64
	dropped    uint64
}

// NewHub creates a Hub. Run must be started in its own goroutine before the
// Hub does anything useful.
func NewHub() *Hub {
	return &Hub{
		broadcast:          make(chan Event, broadcastBufferSize),
		register:           make(chan *Client),
		unregister:         make(chan *Client),
		updateSubscription: make(chan subscriptionUpdate),
		clients:            make(map[*Client]struct{}),
	}
}

// Publish enqueues an event for broadcasting to subscribed clients. It
// never blocks the caller: if the Hub's inbound buffer is full, the event
// is dropped so the ingestion/DB pipeline is never slowed down by
// WebSocket broadcasting. Drops are counted, not logged individually -
// see statsLogInterval - since Publish sits on the hot ingestion path.
func (h *Hub) Publish(topic string, payload []byte) {
	select {
	case h.broadcast <- Event{Topic: topic, Payload: payload}:
	default:
		h.publishDropped.Add(1)
	}
}

// Run processes client (de)registrations, subscription updates and
// broadcasts until ctx is cancelled, at which point it closes every
// connected client. It must run in exactly one goroutine.
func (h *Hub) Run(ctx context.Context) {
	statsTicker := time.NewTicker(statsLogInterval)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.closeAll()
			return

		case client := <-h.register:
			h.clients[client] = struct{}{}

		case client := <-h.unregister:
			h.removeClient(client)

		case update := <-h.updateSubscription:
			h.applySubscriptionUpdate(update)

		case event := <-h.broadcast:
			h.received++
			h.dispatch(event)

		case <-statsTicker.C:
			h.logStats()
		}
	}
}

// logStats emits one structured summary line of Hub throughput - message
// counts rather than per-message logging, so observability never adds
// overhead to the hot dispatch path (CLAUDE.md rule 4).
func (h *Hub) logStats() {
	slog.Info("websocket: hub stats",
		"clients", len(h.clients),
		"received", h.received,
		"dispatched", h.dispatched,
		"dropped", h.dropped,
		"publish_dropped", h.publishDropped.Load(),
	)
}

// dispatch fans an event out to every currently subscribed client, never
// blocking on a slow client's send buffer.
func (h *Hub) dispatch(event Event) {
	msg, err := json.Marshal(outboundMessage{Topic: event.Topic, Payload: event.Payload})
	if err != nil {
		slog.Error("websocket: failed to encode event", "topic", event.Topic, "error", err)
		return
	}

	for client := range h.clients {
		if !client.wantsTopic(event.Topic) {
			continue
		}

		select {
		case client.send <- msg:
			h.dispatched++
		default:
			// Client's buffer is full - drop the message for this client
			// instead of blocking the dispatch loop (and, transitively,
			// the ingestion pipeline behind Publish's buffered channel).
			// Counted, not logged per-drop, for the same reason as Publish
			// above.
			h.dropped++
		}
	}
}

func (h *Hub) removeClient(client *Client) {
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.send)
	}
}

func (h *Hub) applySubscriptionUpdate(update subscriptionUpdate) {
	if _, ok := h.clients[update.client]; !ok {
		return
	}
	for _, topic := range update.sub.Subscribe {
		update.client.topics[topic] = struct{}{}
	}
	for _, topic := range update.sub.Unsubscribe {
		delete(update.client.topics, topic)
	}
}

func (h *Hub) closeAll() {
	for client := range h.clients {
		delete(h.clients, client)
		close(client.send)
	}
}

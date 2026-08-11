// Package websocket implements the pub/sub broadcast Hub that fans out
// events to subscribed clients without ever blocking the ingestion/DB
// pipeline (see CLAUDE.md rule 4).
package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"statusengine-worker/internal/metrics"
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
// message was published under alongside its raw payload. It is the
// authoritative definition of that format (and what the tests decode
// into), but the hot path does not marshal through it - see encode.
type outboundMessage struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// topicKey and payloadKey are outboundMessage's field encodings, spelled
// out so encode can assemble a frame without reflection. They must stay in
// step with the struct tags above.
const (
	topicKey   = `{"topic":`
	payloadKey = `,"payload":`
)

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
//
// A Hub is single-use. Once Run returns, the Hub is permanently stopped:
// every client has been closed and the lifecycle channels below have no
// reader, so anything still trying to register, unregister or resubscribe
// gives up via done instead of blocking forever on a channel nobody will
// ever read. A restart or config reload therefore builds a *new* Hub and
// swaps it in (e.g. behind an atomic.Pointer the /ws handler reads),
// rather than restarting this one - which is why done is created once and
// never replaced.
type Hub struct {
	broadcast          chan Event
	register           chan *Client
	unregister         chan *Client
	updateSubscription chan subscriptionUpdate

	// done is closed when Run returns, whatever the reason. It is the
	// escape hatch for every send on the three lifecycle channels above:
	// they are unbuffered, so without it a client arriving during or
	// after shutdown parks a goroutine (and its connection's file
	// descriptor) for the rest of the process's life.
	done     chan struct{}
	doneOnce sync.Once

	clients map[*Client]struct{}

	// topicPrefix caches the encoded {"topic":"...","payload": prefix of
	// each topic's wire frame, keyed by topic. Bounded by the number of
	// distinct queue names the pipeline publishes (a compile-time constant
	// set, see queue.NewRouter), so it never grows without limit. Owned by
	// Run's goroutine, like clients above.
	topicPrefix map[string][]byte

	// publishDropped counts events dropped by Publish because the
	// broadcast buffer was full. Publish is called from arbitrary
	// ingestion goroutines (not Run's), so this one counter is atomic;
	// every other stat below is only ever touched from within Run and
	// needs no synchronization.
	publishDropped atomic.Uint64

	// clientCount mirrors len(clients) for HasClients to read from
	// ingestion goroutines without touching the map itself.
	clientCount atomic.Int64

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
		done:               make(chan struct{}),
		clients:            make(map[*Client]struct{}),
		topicPrefix:        make(map[string][]byte),
	}
}

// HasClients reports whether any client is currently connected. It exists
// so callers can skip the cost of encoding an event nobody can receive -
// the normal state of a production worker, where the ingestion pipeline
// runs flat out and a dashboard attaches only occasionally.
//
// The answer is inherently a snapshot: a client connecting immediately
// after a false may miss the event that was skipped. That is the same
// guarantee the Hub already gives (events published before a client
// registers are never replayed), and it is why this must never be used to
// gate anything but best-effort broadcasting.
func (h *Hub) HasClients() bool {
	return h.clientCount.Load() > 0
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
// connected client. It must run in exactly one goroutine, and only once
// per Hub - see the type comment.
func (h *Hub) Run(ctx context.Context) {
	// Closed on every return path, not just the ctx.Done() one, so a Hub
	// that stops for any reason still releases whoever is waiting to
	// register or unregister. sync.Once rather than a bare close: a
	// second Run call is a programming error, but turning it into a
	// "close of closed channel" panic during shutdown - the one place a
	// new panic is least welcome - helps nobody.
	defer h.doneOnce.Do(func() { close(h.done) })

	statsTicker := time.NewTicker(statsLogInterval)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.closeAll()
			return

		case client := <-h.register:
			h.clients[client] = struct{}{}
			h.clientCount.Add(1)
			metrics.WebsocketClientsActive.Inc()

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
// blocking on a slow client's send buffer. The wire message is built lazily
// on the first client that actually wants the topic, so an event nobody
// subscribed to costs nothing beyond the map walk.
func (h *Hub) dispatch(event Event) {
	var msg []byte

	for client := range h.clients {
		if !client.wantsTopic(event.Topic) {
			continue
		}

		if msg == nil {
			if msg = h.encode(event); msg == nil {
				return
			}
		}

		select {
		case client.send <- msg:
			h.dispatched++
			metrics.WebsocketMessagesBroadcastedTotal.Inc()
		default:
			// Client's buffer is full - drop the message for this client
			// instead of blocking the dispatch loop (and, transitively,
			// the ingestion pipeline behind Publish's buffered channel).
			// Counted, not logged per-drop, for the same reason as Publish
			// above; the per-client total is reported on disconnect.
			h.dropped++
			client.dropped++
			metrics.WebsocketMessagesDroppedTotal.Inc()
		}
	}
}

// encode builds one client-bound frame, {"topic":...,"payload":...},
// returning nil if the topic itself cannot be encoded.
//
// The payload is appended verbatim rather than re-marshalled through
// outboundMessage: it is already the output of a json.Marshal (see
// queue.publish), so re-encoding it as a json.RawMessage would mean a
// second reflection-driven pass over every single event on the hot
// dispatch path, purely to reproduce bytes that are already correct. Only
// the topic needs real encoding, and since topics are a small fixed set of
// queue names, its encoded prefix is cached after first use. Both the cache
// and the counters it feeds are exclusive to Run's goroutine (see the Hub
// type comment), so neither needs a lock.
func (h *Hub) encode(event Event) []byte {
	prefix, ok := h.topicPrefix[event.Topic]
	if !ok {
		encodedTopic, err := json.Marshal(event.Topic)
		if err != nil {
			slog.Error("websocket: failed to encode event topic", "topic", event.Topic, "error", err)
			metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentWebSocket).Inc()
			return nil
		}
		prefix = make([]byte, 0, len(encodedTopic)+len(topicKey)+len(payloadKey))
		prefix = append(prefix, topicKey...)
		prefix = append(prefix, encodedTopic...)
		prefix = append(prefix, payloadKey...)
		h.topicPrefix[event.Topic] = prefix
	}

	msg := make([]byte, 0, len(prefix)+len(event.Payload)+1)
	msg = append(msg, prefix...)
	msg = append(msg, event.Payload...)
	return append(msg, '}')
}

func (h *Hub) removeClient(client *Client) {
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.send)
		h.clientCount.Add(-1)
		metrics.WebsocketClientsActive.Dec()
		h.logClientGone(client)
	}
}

// logClientGone reports a disconnecting client's drop count - the
// per-client attribution deliberately kept out of the Prometheus metric's
// labels (see metrics.WebsocketMessagesDroppedTotal). Silent for the
// overwhelmingly common case of a client that kept up.
func (h *Hub) logClientGone(client *Client) {
	if client.dropped > 0 {
		slog.Warn("websocket: client disconnected after dropped messages",
			"client_id", client.id, "dropped", client.dropped)
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
		h.clientCount.Add(-1)
		metrics.WebsocketClientsActive.Dec()
		h.logClientGone(client)
	}
}

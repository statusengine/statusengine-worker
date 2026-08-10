package websocket

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"statusengine-worker/internal/metrics"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 8192
	sendBufferSize = 256
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// TODO: restrict to known origins once frontend deployment is fixed.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Client represents a single websocket connection registered with a Hub.
type Client struct {
	hub  *Hub
	conn *websocket.Conn

	// id uniquely identifies this client for the lifetime of the process -
	// used only as the "client_id" label on
	// metrics.WebsocketMessagesDroppedTotal, so a slow client can be told
	// apart from the rest without exposing anything about the connection
	// itself (e.g. remote address).
	id string

	// send is this client's outbound message buffer. The Hub writes to it
	// non-blockingly; writePump drains it to the socket.
	send chan []byte

	// topics is the set of event topics (queue names) this client wants to
	// receive. An empty set means "subscribe to everything".
	topics map[string]struct{}
}

// nextClientID hands out a unique id per Client, process-wide.
var nextClientID atomic.Uint64

// subscriptionMessage is the client->server control message used to
// (re)configure topic subscriptions after the connection is established,
// e.g. {"subscribe":["statusngin_hoststatus"]}.
type subscriptionMessage struct {
	Subscribe   []string `json:"subscribe,omitempty"`
	Unsubscribe []string `json:"unsubscribe,omitempty"`
}

// ServeWS upgrades r to a websocket connection, registers the resulting
// Client with hub and starts its read/write pumps. Initial topics can be
// supplied via the "topics" query parameter as a comma-separated list
// (e.g. "?topics=statusngin_hoststatus,statusngin_servicestatus"); clients
// can subscribe/unsubscribe further at any time by sending a
// subscriptionMessage frame.
//
// If validKeys is non-empty, r must carry one of them (see extractAPIKey)
// or the request is rejected with 401 before the handshake ever upgrades -
// an empty/nil validKeys leaves the endpoint open, matching the worker's
// default (see cfg.apiKeys in cmd/app/main.go).
func ServeWS(hub *Hub, w http.ResponseWriter, r *http.Request, validKeys map[string]struct{}) {
	if !authorized(r, validKeys) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentWebSocket).Inc()
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket: upgrade failed", "error", err)
		metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentWebSocket).Inc()
		return
	}

	client := &Client{
		hub:    hub,
		conn:   conn,
		id:     strconv.FormatUint(nextClientID.Add(1), 10),
		send:   make(chan []byte, sendBufferSize),
		topics: parseTopics(r.URL.Query().Get("topics")),
	}

	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func parseTopics(raw string) map[string]struct{} {
	topics := make(map[string]struct{})
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			topics[t] = struct{}{}
		}
	}
	return topics
}

// wantsTopic reports whether the client should receive an event published
// under topic. An empty subscription set means "receive everything". Only
// called from the Hub's Run goroutine, which is also the sole mutator of
// topics, so no synchronization is needed.
func (c *Client) wantsTopic(topic string) bool {
	if len(c.topics) == 0 {
		return true
	}
	_, ok := c.topics[topic]
	return ok
}

// readPump pumps subscription control frames from the websocket connection
// into the Hub, and unregisters the client on any read error (including a
// client-initiated close).
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Warn("websocket: unexpected close error", "error", err)
			}
			return
		}

		var sub subscriptionMessage
		if err := json.Unmarshal(message, &sub); err != nil {
			continue // ignore malformed control frames, keep the connection alive
		}
		c.hub.updateSubscription <- subscriptionUpdate{client: c, sub: sub}
	}
}

// writePump pumps messages from the client's send buffer to the websocket
// connection, and periodically pings the client to detect dead
// connections. It returns - closing the connection - when the Hub closes
// send (on unregister or shutdown) or a write fails.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel: shut the connection down cleanly.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

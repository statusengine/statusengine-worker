package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"statusengine-worker/internal/metrics"
)

// Path is the URL path the command endpoint is served on.
const Path = "/commands"

// response is what the endpoint returns, on success and on failure alike, so
// a client can parse one shape rather than branching on the status code.
type response struct {
	// Accepted counts the commands handed to the broker. It is not a count
	// of commands Naemon executed - see the 202 comment on ServeHTTP.
	Accepted int    `json:"accepted,omitempty"`
	Error    string `json:"error,omitempty"`
}

// Handler serves POST /commands: it authenticates the caller, validates the
// envelope and publishes it onto the command queue.
type Handler struct {
	publisher Publisher
	apiKeys   map[string]struct{}
}

// NewHandler builds the endpoint. apiKeys must be non-empty - cmd/app does
// not register the endpoint at all when no key is configured, so an empty
// set here would be a programming error rather than a configuration one;
// authorized denies everything in that case regardless.
func NewHandler(publisher Publisher, apiKeys map[string]struct{}) *Handler {
	return &Handler{publisher: publisher, apiKeys: apiKeys}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	if !authorized(r, h.apiKeys) {
		metrics.CommandsRejectedTotal.WithLabelValues(string(ReasonAuth)).Inc()
		// Logged at Warn, and deliberately not aggregated the way the hot
		// path's stats are (CLAUDE.md rule 2): this is a rejected attempt to
		// control the monitoring core, which is worth a line each time. The
		// key itself is never logged - a mistyped key is one character from
		// a real one.
		slog.Warn("command: rejected unauthorized request", "remote_addr", r.RemoteAddr)
		writeError(w, http.StatusUnauthorized, "missing or invalid API key")
		return
	}

	// MaxBytesReader makes an oversized body fail at the read rather than
	// after it has been buffered, so a caller cannot make the process
	// allocate more than this by claiming a large Content-Length.
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)

	var env Envelope
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&env); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			metrics.CommandsRejectedTotal.WithLabelValues(string(ReasonTooLarge)).Inc()
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", MaxBodyBytes))
			return
		}
		metrics.CommandsRejectedTotal.WithLabelValues(string(ReasonMalformed)).Inc()
		writeError(w, http.StatusBadRequest, "request body is not valid JSON: "+err.Error())
		return
	}

	names, err := Validate(&env)
	if err != nil {
		reason, ok := ReasonOf(err)
		if !ok {
			reason = ReasonMalformed
		}
		metrics.CommandsRejectedTotal.WithLabelValues(string(reason)).Inc()

		status := http.StatusBadRequest
		switch reason {
		case ReasonDenied:
			// Someone holding a valid key tried to shut the core down or
			// restart it. That is the one rejection worth alerting on, so
			// it is logged at Warn as well as counted.
			status = http.StatusForbidden
			slog.Warn("command: rejected a denied external command",
				"remote_addr", r.RemoteAddr, "error", err.Error())
		case ReasonTooLarge:
			status = http.StatusRequestEntityTooLarge
		}
		writeError(w, status, err.Error())
		return
	}

	// Re-marshaled from the parsed envelope rather than forwarded verbatim,
	// so what reaches the broker is exactly what was validated. Forwarding
	// the raw body would leave room for the two to disagree - a duplicate
	// JSON key, say, where encoding/json takes the last and another parser
	// takes the first.
	payload, err := json.Marshal(&env)
	if err != nil {
		metrics.CommandsRejectedTotal.WithLabelValues(string(ReasonMalformed)).Inc()
		writeError(w, http.StatusBadRequest, "could not re-encode the command: "+err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), publishTimeout)
	defer cancel()

	if err := h.publisher.Publish(ctx, payload); err != nil {
		metrics.CommandPublishErrorsTotal.Inc()
		metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentCommand).Inc()
		slog.Error("command: publishing failed", "commands", len(names), "error", err)
		// 503, not 500: the request was fine and the same request will work
		// once the broker is back, which is exactly what this status tells a
		// client that retries.
		writeError(w, http.StatusServiceUnavailable, "could not reach the message broker, retry later")
		return
	}

	for _, name := range names {
		metrics.CommandsReceivedTotal.WithLabelValues(name).Inc()
	}
	metrics.CommandsPublishedTotal.Inc()
	slog.Debug("command: published", "commands", len(names))

	// 202 Accepted, never 200 OK, and the difference is not pedantry.
	// Publishing puts the message on a queue; whether Naemon then acts on it
	// is something this process cannot observe and will never learn - the
	// broker module logs nothing for a command it does not recognise, and
	// there is no reply path. A 200 would read as "done"; 202 says
	// "handed on", which is the whole truth available here.
	writeJSON(w, http.StatusAccepted, response{Accepted: len(names)})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, response{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, body response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Debug("command: writing response failed", "error", err)
	}
}

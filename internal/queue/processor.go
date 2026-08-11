package queue

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/websocket"
)

// PerfdataRoute selects where decoded statusngin_service_perfdata metrics
// are written (CLAUDE.md rule 5: Conditional Perfdata Routing).
type PerfdataRoute int

const (
	PerfdataRouteMySQL PerfdataRoute = iota
	PerfdataRouteGraphite
	PerfdataRouteBoth
)

// ParsePerfdataRoute parses the -perfdata-route flag/env value.
func ParsePerfdataRoute(s string) (PerfdataRoute, error) {
	switch strings.ToLower(s) {
	case "mysql":
		return PerfdataRouteMySQL, nil
	case "graphite":
		return PerfdataRouteGraphite, nil
	case "both":
		return PerfdataRouteBoth, nil
	default:
		return 0, fmt.Errorf("queue: unknown perfdata route %q: want \"mysql\", \"graphite\" or \"both\"", s)
	}
}

// perfdataMetric is one perf_data point parsed out of a servicecheck,
// tagged with its originating host/service and the message's timestamp -
// the shape both the statusengine_perfdata BulkInserter and the Graphite
// client consume.
type perfdataMetric struct {
	HostName           string
	ServiceDescription string
	Label              string
	Timestamp          int64
	Value              float64
	Unit               string
}

// perfdataRow renders a perfdataMetric as a statusengine_perfdata row
// (hostname, service_description, label, timestamp, timestamp_unix,
// value, unit); timestamp and timestamp_unix both carry the same Unix
// second, mirroring how the legacy worker populates this table.
func perfdataRow(m perfdataMetric, dst []any) []any {
	return append(dst, m.HostName, m.ServiceDescription, m.Label, m.Timestamp*1000, m.Timestamp, m.Value, m.Unit)
}

// graphiteMetricPath renders a perfdataMetric's dotted Graphite path as
// prefix.hostname.service_description.label (e.g.
// "statusengine.localhost.Ping.RTA"), sanitizing every segment - including
// prefix itself, mirroring the legacy worker's
// replaceIllegalCharacters($this->prefix) - so a literal ".", " ", quote or
// locale-specific character (e.g. an umlaut) in any of them can't inject a
// bogus path segment or otherwise confuse Graphite. Label may carry a
// leading/trailing "'" (parsePerfData deliberately keeps a quoted metric's
// quote characters, matching the legacy worker's statusengine_perfdata.label
// - see perfdata.go), which is exactly the kind of character this sanitizes.
func graphiteMetricPath(prefix string, m perfdataMetric) string {
	return strings.Join([]string{
		sanitizeGraphitePathSegment(prefix),
		sanitizeGraphitePathSegment(m.HostName),
		sanitizeGraphitePathSegment(m.ServiceDescription),
		sanitizeGraphitePathSegment(m.Label),
	}, ".")
}

// graphiteIllegalCharacters matches every character outside the legacy
// worker's whitelist (see Config::getGraphiteIllegalCharacters's default
// "/[^a-zA-Z^0-9\-\.]/"): a-z, A-Z, 0-9, '-', '.', and - as a quirk of that
// PHP regex, where '^' only negates the character class as its very first
// character - a literal '^' too. Anything else, including umlauts and
// other non-ASCII characters, is replaced with '_'.
var graphiteIllegalCharacters = regexp.MustCompile(`[^a-zA-Z^0-9\-.]`)

func sanitizeGraphitePathSegment(s string) string {
	return graphiteIllegalCharacters.ReplaceAllString(s, "_")
}

// graphiteEnqueuer is the subset of *graphite.Client a Handler needs to
// ship a metric - declared locally (rather than referencing
// *graphite.Client directly) so NewPerfdataHandler stays testable without
// a real Graphite connection, mirroring the enqueuer interface in
// router.go.
type graphiteEnqueuer interface {
	Enqueue(ctx context.Context, m graphite.Metric) error
}

// NewPerfdataHandler builds the Handler for the statusngin_service_perfdata
// queue. Every decoded servicecheck is published to hub as-is (unchanged
// from before); its perf_data string is then parsed into individual
// metrics, each of which is routed to mysqlIns, gc, or both, depending on
// route (CLAUDE.md rule 5). Routing is decided once, outside the hot
// per-message loop, rather than branching on route for every metric.
// graphitePrefix is sanitized once here too, rather than on every metric.
func NewPerfdataHandler(hub *websocket.Hub, topic string, route PerfdataRoute, mysqlIns enqueuer[perfdataMetric], gc graphiteEnqueuer, graphitePrefix string) Handler {
	toMySQL := route == PerfdataRouteMySQL || route == PerfdataRouteBoth
	toGraphite := route == PerfdataRouteGraphite || route == PerfdataRouteBoth
	sanitizedPrefix := sanitizeGraphitePathSegment(graphitePrefix)

	return func(ctx context.Context, payload []byte) error {
		events, err := decodePerfdata(payload)
		if err != nil {
			return decodeError(topic, err)
		}

		for _, ev := range events {
			publish(hub, topic, ev.ServiceCheckPayload)

			for _, p := range parsePerfData(ev.PerfData) {
				metric := perfdataMetric{
					HostName:           ev.HostName,
					ServiceDescription: ev.ServiceDescription,
					Label:              p.Label,
					Timestamp:          ev.Timestamp,
					Value:              p.Value,
					Unit:               p.Unit,
				}

				if toMySQL {
					if err := mysqlIns.Enqueue(ctx, metric); err != nil {
						return fmt.Errorf("queue: enqueue %s metric: %w", topic, err)
					}
				}
				if toGraphite {
					if err := gc.Enqueue(ctx, graphite.Metric{
						Path:      graphiteMetricPath(sanitizedPrefix, metric),
						Value:     metric.Value,
						Timestamp: metric.Timestamp,
					}); err != nil {
						return fmt.Errorf("queue: enqueue %s metric to graphite: %w", topic, err)
					}
				}
			}
		}
		return nil
	}
}

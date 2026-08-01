package queue

import (
	"context"
	"fmt"
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
func perfdataRow(m perfdataMetric) []any {
	return []any{m.HostName, m.ServiceDescription, m.Label, m.Timestamp, m.Timestamp, m.Value, m.Unit}
}

// graphiteMetricPath renders a perfdataMetric's dotted Graphite path,
// sanitizing hostname/service/label so a literal "." or " " in any of them
// can't inject a bogus path segment.
func graphiteMetricPath(m perfdataMetric) string {
	return strings.Join([]string{
		"statusengine",
		sanitizeGraphitePathSegment(m.HostName),
		sanitizeGraphitePathSegment(m.ServiceDescription),
		sanitizeGraphitePathSegment(m.Label),
	}, ".")
}

var graphitePathReplacer = strings.NewReplacer(".", "_", " ", "_")

func sanitizeGraphitePathSegment(s string) string {
	return graphitePathReplacer.Replace(s)
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
func NewPerfdataHandler(hub *websocket.Hub, topic string, route PerfdataRoute, mysqlIns enqueuer[perfdataMetric], gc graphiteEnqueuer) Handler {
	toMySQL := route == PerfdataRouteMySQL || route == PerfdataRouteBoth
	toGraphite := route == PerfdataRouteGraphite || route == PerfdataRouteBoth

	return func(ctx context.Context, payload []byte) error {
		events, err := decodePerfdata(payload)
		if err != nil {
			return fmt.Errorf("queue: decode %s: %w", topic, err)
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
						Path:      graphiteMetricPath(metric),
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

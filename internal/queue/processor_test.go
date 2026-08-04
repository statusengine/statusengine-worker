package queue

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/types"
	"statusengine-worker/internal/websocket"
)

// fakeGraphiteEnqueuer stands in for a *graphite.Client in tests that don't
// need a real Graphite connection: it just records what was enqueued.
type fakeGraphiteEnqueuer struct {
	mu      sync.Mutex
	metrics []graphite.Metric
	err     error
}

func (f *fakeGraphiteEnqueuer) Enqueue(_ context.Context, m graphite.Metric) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.metrics = append(f.metrics, m)
	return nil
}

func (f *fakeGraphiteEnqueuer) snapshot() []graphite.Metric {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]graphite.Metric, len(f.metrics))
	copy(out, f.metrics)
	return out
}

func TestParsePerfdataRoute(t *testing.T) {
	tests := []struct {
		in      string
		want    PerfdataRoute
		wantErr bool
	}{
		{"mysql", PerfdataRouteMySQL, false},
		{"MySQL", PerfdataRouteMySQL, false},
		{"graphite", PerfdataRouteGraphite, false},
		{"both", PerfdataRouteBoth, false},
		{"carbon", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		got, err := ParsePerfdataRoute(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParsePerfdataRoute(%q) = %v, want error", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParsePerfdataRoute(%q) unexpected error: %v", tt.in, err)
		}
		if got != tt.want {
			t.Errorf("ParsePerfdataRoute(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// The statusngin_service_perfdata.json fixture carries 3 servicecheck
// messages with perf_data "users=0;20;50;0", "swap=0MB;0;0;0;0" and
// "rta=0.084000ms;100.000000;500.000000;0.000000 pl=0%;20;60;0" - 4
// individual metrics in total.
const wantFixtureMetricCount = 4

func TestNewPerfdataHandlerRouteMySQLOnly(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	conn := dialTopic(t, hub, QueueServicePerfdata)

	mysqlIns := &fakeEnqueuer[perfdataMetric]{}
	gc := &fakeGraphiteEnqueuer{}
	handler := NewPerfdataHandler(hub, QueueServicePerfdata, PerfdataRouteMySQL, mysqlIns, gc)

	if err := handler(ctx, readFixture(t, "statusngin_service_perfdata.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got := len(mysqlIns.snapshot()); got != wantFixtureMetricCount {
		t.Fatalf("mysqlIns got %d metrics, want %d", got, wantFixtureMetricCount)
	}
	if got := len(gc.snapshot()); got != 0 {
		t.Fatalf("graphite got %d metrics, want 0 for mysql-only route", got)
	}

	topic, payload := readTopicMessage(t, conn)
	if topic != QueueServicePerfdata {
		t.Fatalf("topic = %q, want %q", topic, QueueServicePerfdata)
	}
	var got types.ServiceCheckPayload
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.HostName != "localhost" {
		t.Fatalf("unexpected broadcast payload: %+v", got)
	}
}

func TestNewPerfdataHandlerRouteGraphiteOnly(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	mysqlIns := &fakeEnqueuer[perfdataMetric]{}
	gc := &fakeGraphiteEnqueuer{}
	handler := NewPerfdataHandler(hub, QueueServicePerfdata, PerfdataRouteGraphite, mysqlIns, gc)

	if err := handler(ctx, readFixture(t, "statusngin_service_perfdata.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got := len(mysqlIns.snapshot()); got != 0 {
		t.Fatalf("mysqlIns got %d metrics, want 0 for graphite-only route", got)
	}
	metrics := gc.snapshot()
	if len(metrics) != wantFixtureMetricCount {
		t.Fatalf("graphite got %d metrics, want %d", len(metrics), wantFixtureMetricCount)
	}
	if metrics[0].Path != "statusengine.localhost.Current_Users.users" {
		t.Fatalf("unexpected graphite path: %q", metrics[0].Path)
	}
}

func TestNewPerfdataHandlerRouteBoth(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	mysqlIns := &fakeEnqueuer[perfdataMetric]{}
	gc := &fakeGraphiteEnqueuer{}
	handler := NewPerfdataHandler(hub, QueueServicePerfdata, PerfdataRouteBoth, mysqlIns, gc)

	if err := handler(ctx, readFixture(t, "statusngin_service_perfdata.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got := len(mysqlIns.snapshot()); got != wantFixtureMetricCount {
		t.Fatalf("mysqlIns got %d metrics, want %d", got, wantFixtureMetricCount)
	}
	if got := len(gc.snapshot()); got != wantFixtureMetricCount {
		t.Fatalf("graphite got %d metrics, want %d", got, wantFixtureMetricCount)
	}
}

func TestGraphiteMetricPathSanitizesDotsAndSpaces(t *testing.T) {
	m := perfdataMetric{
		HostName:           "host.example.com",
		ServiceDescription: "Disk Usage /var",
		Label:              "used bytes",
	}
	want := "statusengine.host_example_com.Disk_Usage_/var.used_bytes"
	if got := graphiteMetricPath(m); got != want {
		t.Fatalf("graphiteMetricPath() = %q, want %q", got, want)
	}
}

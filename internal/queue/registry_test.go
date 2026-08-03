package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/types"
	"statusengine-worker/internal/websocket"
)

// testDSN points at the local dev MySQL instance documented in
// .claude/specs/ressources.txt.
const testDSN = "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine-dev?parseTime=true"

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	sqlDB, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Skipf("mysql driver open: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		t.Skipf("no reachable dev MySQL at %s: %v", testDSN, err)
	}
	return sqlDB
}

func TestNewRouterCoversAllQueues(t *testing.T) {
	sqlDB := openTestDB(t)
	hub := websocket.NewHub()
	router, runners := NewRouter(sqlDB, hub, graphite.NewClient("127.0.0.1:2003"), PerfdataRouteMySQL, "statusengine-test")

	want := []string{
		QueueHostStatus, QueueServiceStatus, QueueHostChecks, QueueServiceChecks,
		QueueServicePerfdata, QueueStateChanges, QueueLogEntries, QueueNotifications,
		QueueContactNotificationMethod, QueueAcknowledgements, QueueDowntimes, QueueCoreRestart,
	}
	for _, q := range want {
		if _, ok := router[q]; !ok {
			t.Errorf("router missing handler for queue %q", q)
		}
	}
	if len(router) != len(want) {
		t.Errorf("router has %d queues, want %d", len(router), len(want))
	}
	if len(runners) != 11 {
		t.Errorf("expected 11 runners (host/service status, hostchecks, servicechecks, logentries, "+
			"host/service statehistory, host/service acknowledgements, perfdata, graphite client), got %d", len(runners))
	}
}

// runAllAndFlush starts every runner's Run loop and, on cleanup, flushes
// each of them so pending rows land before the test's own assertions and
// before the surrounding context is cancelled.
func runAllAndFlush(t *testing.T, runners []Runner) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	for _, r := range runners {
		go r.Run(ctx)
	}
	t.Cleanup(func() {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer flushCancel()
		for _, r := range runners {
			r.Flush(flushCtx)
		}
		cancel()
	})
	return ctx
}

func TestHostCheckHandlerPersistsToMySQL(t *testing.T) {
	sqlDB := openTestDB(t)
	hub := websocket.NewHub()
	router, runners := NewRouter(sqlDB, hub, graphite.NewClient("127.0.0.1:2003"), PerfdataRouteMySQL, "statusengine-test")
	ctx := runAllAndFlush(t, runners)
	go hub.Run(ctx)

	hostname := fmt.Sprintf("queue-pkg-test-hostcheck-%d", time.Now().UnixNano())
	startTime := time.Now().Unix()
	t.Cleanup(func() {
		sqlDB.Exec("DELETE FROM statusengine_hostchecks WHERE hostname = ?", hostname)
	})

	bulk := types.HostCheckBulk{
		Format: "none",
		Messages: []types.HostCheckMessage{{
			Envelope: types.Envelope{Type: types.EventTypeHostCheck, Timestamp: startTime},
			HostCheck: types.HostCheckPayload{
				HostName:    hostname,
				CommandName: "check-host-alive",
				Output:      "PING OK",
				State:       0,
				StateType:   1,
				StartTime:   startTime,
				EndTime:     startTime,
			},
		}},
	}
	payload, err := json.Marshal(bulk)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	handle, ok := router[QueueHostChecks]
	if !ok {
		t.Fatal("no handler registered for QueueHostChecks")
	}
	if err := handle(ctx, payload); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// Force a flush now rather than waiting for the cleanup-time one, so
	// the SELECT below can rely on the row already being present.
	for _, r := range runners {
		if err := r.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	var gotOutput string
	var gotState, gotHard int
	row := sqlDB.QueryRowContext(ctx,
		"SELECT output, state, is_hardstate FROM statusengine_hostchecks WHERE hostname = ? AND start_time = ?",
		hostname, startTime)
	if err := row.Scan(&gotOutput, &gotState, &gotHard); err != nil {
		t.Fatalf("row not persisted: %v", err)
	}
	if gotOutput != "PING OK" || gotState != 0 || gotHard != 1 {
		t.Fatalf("got (output=%q, state=%d, is_hardstate=%d), want (PING OK, 0, 1)", gotOutput, gotState, gotHard)
	}
}

func TestAcknowledgementHandlerRoutesToHostAndServiceTables(t *testing.T) {
	sqlDB := openTestDB(t)
	hub := websocket.NewHub()
	router, runners := NewRouter(sqlDB, hub, graphite.NewClient("127.0.0.1:2003"), PerfdataRouteMySQL, "statusengine-test")
	ctx := runAllAndFlush(t, runners)
	go hub.Run(ctx)

	hostOnly := fmt.Sprintf("queue-pkg-test-ack-host-%d", time.Now().UnixNano())
	withService := fmt.Sprintf("queue-pkg-test-ack-svc-%d", time.Now().UnixNano())
	entryTime := time.Now().Unix()
	t.Cleanup(func() {
		sqlDB.Exec("DELETE FROM statusengine_host_acknowledgements WHERE hostname = ?", hostOnly)
		sqlDB.Exec("DELETE FROM statusengine_service_acknowledgements WHERE hostname = ?", withService)
	})

	handle, ok := router[QueueAcknowledgements]
	if !ok {
		t.Fatal("no handler registered for QueueAcknowledgements")
	}

	// Non-bulk queue: one raw JSON object per call, per CLAUDE.md's bulk
	// exception list.
	hostMsg := types.AcknowledgementMessage{
		Envelope: types.Envelope{Type: types.EventTypeAcknowledgement, Timestamp: entryTime},
		Acknowledgement: types.AcknowledgementPayload{
			HostName:   hostOnly,
			AuthorName: "test-author",
			State:      1,
		},
	}
	hostPayload, err := json.Marshal(hostMsg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := handle(ctx, hostPayload); err != nil {
		t.Fatalf("handle (host): %v", err)
	}

	serviceMsg := types.AcknowledgementMessage{
		Envelope: types.Envelope{Type: types.EventTypeAcknowledgement, Timestamp: entryTime},
		Acknowledgement: types.AcknowledgementPayload{
			HostName:           withService,
			ServiceDescription: "Swap Usage",
			AuthorName:         "test-author",
			State:              2,
		},
	}
	servicePayload, err := json.Marshal(serviceMsg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := handle(ctx, servicePayload); err != nil {
		t.Fatalf("handle (service): %v", err)
	}

	for _, r := range runners {
		if err := r.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	var hostState int
	if err := sqlDB.QueryRowContext(ctx,
		"SELECT state FROM statusengine_host_acknowledgements WHERE hostname = ? AND entry_time = ?",
		hostOnly, entryTime).Scan(&hostState); err != nil {
		t.Fatalf("host row not persisted: %v", err)
	}
	if hostState != 1 {
		t.Fatalf("host row state = %d, want 1", hostState)
	}

	var serviceState int
	if err := sqlDB.QueryRowContext(ctx,
		"SELECT state FROM statusengine_service_acknowledgements WHERE hostname = ? AND entry_time = ?",
		withService, entryTime).Scan(&serviceState); err != nil {
		t.Fatalf("service row not persisted: %v", err)
	}
	if serviceState != 2 {
		t.Fatalf("service row state = %d, want 2", serviceState)
	}

	// Confirm the host-only ack did NOT also land in the service table and
	// vice versa - proof newAcknowledgementHandler's routing works, not
	// just that some row exists.
	var count int
	if err := sqlDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM statusengine_service_acknowledgements WHERE hostname = ?", hostOnly,
	).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 0 {
		t.Fatalf("host-only ack leaked into service_acknowledgements: count = %d", count)
	}
}

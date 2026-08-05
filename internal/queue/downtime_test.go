package queue

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"statusengine-worker/internal/db"
	"statusengine-worker/internal/types"
	"statusengine-worker/internal/websocket"
)

const testDowntimeNodeName = "statusengine-test"

func marshalDowntimeMessage(t *testing.T, msg types.DowntimeMessage) []byte {
	t.Helper()
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal downtime message: %v", err)
	}
	return raw
}

// toDBRow converts a DowntimeRowData (internal/queue) into a db.DowntimeRow
// (internal/db) - the exact same conversion execDowntimeAction does in
// registry.go, duplicated here (rather than exported and reused) because
// internal/db can't import internal/queue's type and this test wants to
// derive its expected queries from the real internal/db builders instead of
// re-deriving raw SQL text by hand.
func toDBRow(data DowntimeRowData) db.DowntimeRow {
	return db.DowntimeRow{
		IsHostDowntime:     data.IsHostDowntime,
		HostName:           data.HostName,
		ServiceDescription: data.ServiceDescription,
		NodeName:           data.NodeName,
		InternalDowntimeID: data.InternalDowntimeID,
		EntryTime:          data.EntryTime,
		EntryTimeUsec:      data.EntryTimeUsec,
		AuthorName:         data.AuthorName,
		CommentData:        data.CommentData,
		TriggeredByID:      data.TriggeredByID,
		IsFixed:            data.IsFixed,
		Duration:           data.Duration,
		ScheduledStartTime: data.ScheduledStartTime,
		ScheduledEndTime:   data.ScheduledEndTime,
		WasStarted:         data.WasStarted,
		ActualStartTime:    data.ActualStartTime,
		ActualEndTime:      data.ActualEndTime,
		WasCancelled:       data.WasCancelled,
	}
}

func toDriverArgs(args []any) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a
	}
	return out
}

// buildDowntimeQuery mirrors execDowntimeAction's own (Table, Action)
// dispatch switch in registry.go, so a test can compute exactly the query
// and args the handler is expected to send for a given action - without
// duplicating internal/db's SQL text by hand (that's
// internal/db/downtime_test.go's job).
func buildDowntimeQuery(action DowntimeAction) (string, []any) {
	row := toDBRow(action.Data)
	switch {
	case action.Table == ScheduledDowntimesTable && action.Action == DowntimeActionUpsert:
		return db.UpsertScheduledDowntimeQuery(row)
	case action.Table == ScheduledDowntimesTable && action.Action == DowntimeActionDelete:
		return db.DeleteScheduledDowntimeQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionUpsert:
		return db.UpsertDowntimeHistoryQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionUpdateStarted:
		return db.UpdateDowntimeHistoryStartedQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionUpdateStopped:
		return db.UpdateDowntimeHistoryStoppedQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionDelete:
		return db.DeleteDowntimeHistoryQuery(row)
	default:
		panic("buildDowntimeQuery: unhandled action")
	}
}

// expectDowntimeAction queues one sqlmock expectation for action. Query and
// args come from the same internal/db builder execDowntimeAction itself
// calls, so this test proves newDowntimeHandler wires
// DetermineDowntimeActions' decisions to the right builder with the right
// data and in the right order - not that the SQL text itself is correct
// (internal/db/downtime_test.go already covers that in isolation).
func expectDowntimeAction(mock sqlmock.Sqlmock, action DowntimeAction) {
	query, args := buildDowntimeQuery(action)
	mock.ExpectExec("^" + regexp.QuoteMeta(query) + "$").
		WithArgs(toDriverArgs(args)...).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func setupDowntimeHandler(t *testing.T) (Handler, sqlmock.Sqlmock, *websocket.Hub, context.Context) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { mockDB.Close() })

	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go hub.Run(ctx)

	handler := newDowntimeHandler(hub, QueueDowntimes, mockDB, testDowntimeNodeName)
	return handler, mock, hub, ctx
}

var hostDowntimePayload = types.DowntimePayload{
	HostName:     "web01",
	AuthorName:   "Daniel Z",
	CommentData:  "In maintenance",
	DowntimeType: types.DowntimeTypeHost,
	EntryTime:    1000,
	StartTime:    2000,
	EndTime:      2500,
	TriggeredBy:  0,
	DowntimeID:   42,
	Fixed:        1,
	Duration:     500,
}

var serviceDowntimePayload = types.DowntimePayload{
	HostName:           "web01",
	ServiceDescription: "Swap Usage",
	AuthorName:         "Daniel Z",
	CommentData:        "In maintenance",
	DowntimeType:       types.DowntimeTypeService,
	EntryTime:          1000,
	StartTime:          2000,
	EndTime:            2500,
	TriggeredBy:        0,
	DowntimeID:         42,
	Fixed:              1,
	Duration:           500,
}

func TestDowntimeHandlerAdd(t *testing.T) {
	handler, mock, hub, ctx := setupDowntimeHandler(t)
	conn := dialTopic(t, hub, QueueDowntimes)

	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeAdd, Timestamp: 1000, TimestampUsec: 250},
		Downtime: hostDowntimePayload,
	}

	actions := DetermineDowntimeActions(msg, testDowntimeNodeName)
	if len(actions) != 2 {
		t.Fatalf("test setup: want exactly 2 actions (downtimehistory then scheduleddowntimes), got %d: %+v", len(actions), actions)
	}
	for _, action := range actions {
		expectDowntimeAction(mock, action)
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}

	topic, payload := readTopicMessage(t, conn)
	if topic != QueueDowntimes {
		t.Fatalf("ws topic = %q, want %q", topic, QueueDowntimes)
	}
	var got types.DowntimeMessage
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("unmarshal broadcast payload: %v", err)
	}
	if got.Type != types.EventTypeDowntimeAdd || got.Downtime.DowntimeID != 42 {
		t.Fatalf("unexpected broadcast payload: %+v", got)
	}
}

func TestDowntimeHandlerStart(t *testing.T) {
	handler, mock, _, ctx := setupDowntimeHandler(t)

	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeStart, Timestamp: 2005},
		Downtime: serviceDowntimePayload,
	}

	actions := DetermineDowntimeActions(msg, testDowntimeNodeName)
	if len(actions) != 2 {
		t.Fatalf("test setup: want exactly 2 actions, got %d: %+v", len(actions), actions)
	}
	for _, action := range actions {
		expectDowntimeAction(mock, action)
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestDowntimeHandlerStopCancelled(t *testing.T) {
	handler, mock, _, ctx := setupDowntimeHandler(t)

	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeStop, Attr: types.DowntimeAttrStopCancelled, Timestamp: 2200},
		Downtime: hostDowntimePayload,
	}

	actions := DetermineDowntimeActions(msg, testDowntimeNodeName)
	if len(actions) != 2 {
		t.Fatalf("test setup: want exactly 2 actions, got %d: %+v", len(actions), actions)
	}
	for _, action := range actions {
		expectDowntimeAction(mock, action)
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestDowntimeHandlerDeleteAfterAlreadyStarted(t *testing.T) {
	handler, mock, _, ctx := setupDowntimeHandler(t)

	// start_time (2000) < timestamp (3000): already started (and, in the
	// normal flow, already STOPped) before this DELETE arrives - so
	// downtimehistory must NOT be touched, only scheduleddowntimes deleted.
	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeDelete, Timestamp: 3000},
		Downtime: serviceDowntimePayload,
	}

	actions := DetermineDowntimeActions(msg, testDowntimeNodeName)
	if len(actions) != 1 {
		t.Fatalf("test setup: want exactly 1 action, got %d: %+v", len(actions), actions)
	}
	for _, action := range actions {
		expectDowntimeAction(mock, action)
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met (downtimehistory must stay untouched): %v", err)
	}
}

func TestDowntimeHandlerDeleteNeverStarted(t *testing.T) {
	handler, mock, _, ctx := setupDowntimeHandler(t)

	// start_time (2000) > timestamp (500): the downtime was deleted before
	// its scheduled start was ever reached - both tables must be purged.
	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeDelete, Timestamp: 500},
		Downtime: hostDowntimePayload,
	}

	actions := DetermineDowntimeActions(msg, testDowntimeNodeName)
	if len(actions) != 2 {
		t.Fatalf("test setup: want exactly 2 actions (scheduleddowntimes then downtimehistory), got %d: %+v", len(actions), actions)
	}
	for _, action := range actions {
		expectDowntimeAction(mock, action)
	}

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestDowntimeHandlerLoadIsNoopButStillBroadcasts(t *testing.T) {
	handler, mock, hub, ctx := setupDowntimeHandler(t)
	conn := dialTopic(t, hub, QueueDowntimes)

	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeLoad, Timestamp: 1500},
		Downtime: hostDowntimePayload,
	}

	// Deliberately no mock.ExpectExec calls at all - LOAD must not touch
	// either table (downtime_ablauf.txt section 5).
	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected zero DB calls for LOAD: %v", err)
	}

	topic, _ := readTopicMessage(t, conn)
	if topic != QueueDowntimes {
		t.Fatalf("LOAD should still broadcast: topic = %q, want %q", topic, QueueDowntimes)
	}
}

func TestDowntimeHandlerPropagatesExecError(t *testing.T) {
	handler, mock, _, ctx := setupDowntimeHandler(t)

	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeAdd, Timestamp: 1000, TimestampUsec: 250},
		Downtime: hostDowntimePayload,
	}

	actions := DetermineDowntimeActions(msg, testDowntimeNodeName)
	if len(actions) != 2 {
		t.Fatalf("test setup: want exactly 2 actions, got %d: %+v", len(actions), actions)
	}
	// Only the first (downtimehistory) query is expected: the handler must
	// stop and return the error instead of proceeding to scheduleddowntimes.
	query, args := buildDowntimeQuery(actions[0])
	mock.ExpectExec("^" + regexp.QuoteMeta(query) + "$").
		WithArgs(toDriverArgs(args)...).
		WillReturnError(context.DeadlineExceeded)

	if err := handler(ctx, marshalDowntimeMessage(t, msg)); err == nil {
		t.Fatal("expected handler to propagate the ExecContext error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

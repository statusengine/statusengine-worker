package queue

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"statusengine-worker/internal/websocket"
)

// fakeTableCleaner stands in for a *db.BulkInserter[T] in tests that only
// need to prove newCoreRestartHandler paused-and-ran fn, without a real
// buffer/flush loop behind it.
type fakeTableCleaner struct {
	calls int
}

func (f *fakeTableCleaner) WithPaused(ctx context.Context, fn func(ctx context.Context) error) error {
	f.calls++
	return fn(ctx)
}

func TestCoreRestartHandlerIgnoresNonMatchingObjectType(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	hostStatus := &fakeTableCleaner{}
	serviceStatus := &fakeTableCleaner{}
	handler := newCoreRestartHandler(hub, QueueCoreRestart, mockDB, hostStatus, serviceStatus, false)

	if err := handler(ctx, []byte(`{"object_type": 999}`)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if hostStatus.calls != 0 || serviceStatus.calls != 0 {
		t.Fatalf("expected no cleanup for non-matching object_type, got hostStatus=%d serviceStatus=%d",
			hostStatus.calls, serviceStatus.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected query executed: %v", err)
	}
}

func TestCoreRestartHandlerTruncatesByDefault(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^TRUNCATE TABLE statusengine_hoststatus;$`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`^TRUNCATE TABLE statusengine_servicestatus;$`).WillReturnResult(sqlmock.NewResult(0, 0))

	hostStatus := &fakeTableCleaner{}
	serviceStatus := &fakeTableCleaner{}
	handler := newCoreRestartHandler(hub, QueueCoreRestart, mockDB, hostStatus, serviceStatus, false)

	if err := handler(ctx, []byte(`{"object_type": 102}`)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if hostStatus.calls != 1 || serviceStatus.calls != 1 {
		t.Fatalf("expected one paused cleanup each, got hostStatus=%d serviceStatus=%d", hostStatus.calls, serviceStatus.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestCoreRestartHandlerUsesOpenITCockpitDeleteQueriesWhenEnabled(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`^DELETE FROM statusengine_hoststatus WHERE NOT EXISTS \(SELECT hosts\.uuid FROM hosts WHERE statusengine_hoststatus\.hostname = hosts\.uuid AND hosts\.disabled=0\)$`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`^DELETE FROM statusengine_servicestatus WHERE NOT EXISTS \(SELECT services\.uuid FROM services WHERE statusengine_servicestatus\.service_description = services\.uuid AND services\.disabled=0\)$`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	hostStatus := &fakeTableCleaner{}
	serviceStatus := &fakeTableCleaner{}
	handler := newCoreRestartHandler(hub, QueueCoreRestart, mockDB, hostStatus, serviceStatus, true)

	if err := handler(ctx, []byte(`{"object_type": 102}`)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

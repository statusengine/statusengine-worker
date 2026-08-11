package queue

import (
	"context"
	"testing"

	"statusengine-worker/internal/websocket"
)

func TestNotificationHandlerDiscardsNonEndTypeAndNoContacts(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[notificationLogEvent]{}
	serviceIns := &fakeEnqueuer[notificationLogEvent]{}
	handler := newNotificationHandler(hub, QueueNotifications, hostIns, serviceIns)

	// The real fixture carries two type-601 messages, both with
	// contacts_notified 1: the first has a service_description ("Swap
	// Usage"), the second's is null - so one row each.
	if err := handler(ctx, readFixture(t, "statusngin_notifications.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(hostIns.snapshot()); got != 1 {
		t.Fatalf("hostIns got %d items, want 1", got)
	}
	if got := len(serviceIns.snapshot()); got != 1 {
		t.Fatalf("serviceIns got %d items, want 1", got)
	}

	// type != 601 must be discarded entirely.
	wrongType := []byte(`{"messages":[{"type":602,"timestamp":1,"timestamp_usec":2,
		"notification_data":{"host_name":"localhost","contacts_notified":1}}],"format":"none"}`)
	if err := handler(ctx, wrongType); err != nil {
		t.Fatalf("handler (wrong type): %v", err)
	}
	if got := len(hostIns.snapshot()); got != 1 {
		t.Fatalf("hostIns got %d items after non-601 event, want still 1", got)
	}

	// contacts_notified <= 0 must be discarded entirely, even with type 601.
	noContacts := []byte(`{"messages":[{"type":601,"timestamp":1,"timestamp_usec":2,
		"notification_data":{"host_name":"localhost","contacts_notified":0}}],"format":"none"}`)
	if err := handler(ctx, noContacts); err != nil {
		t.Fatalf("handler (no contacts): %v", err)
	}
	if got := len(hostIns.snapshot()); got != 1 {
		t.Fatalf("hostIns got %d items after contacts_notified<=0 event, want still 1", got)
	}
}

func TestNotificationHandlerRoutesHostVsService(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[notificationLogEvent]{}
	serviceIns := &fakeEnqueuer[notificationLogEvent]{}
	handler := newNotificationHandler(hub, QueueNotifications, hostIns, serviceIns)

	hostEvent := []byte(`{"messages":[{"type":601,"timestamp":1,"timestamp_usec":222,
		"notification_data":{"host_name":"localhost","service_description":"","contacts_notified":1,"start_time":111}}],"format":"none"}`)
	if err := handler(ctx, hostEvent); err != nil {
		t.Fatalf("handler (host): %v", err)
	}

	got := hostIns.snapshot()
	if len(got) != 1 {
		t.Fatalf("hostIns got %d items, want 1", len(got))
	}
	if got[0].StartTime != 111 || got[0].TimestampUsec != 222 {
		t.Fatalf("start_time/start_time_usec = %d/%d, want 111/222", got[0].StartTime, got[0].TimestampUsec)
	}
	if len(serviceIns.snapshot()) != 0 {
		t.Fatalf("serviceIns got %d items, want 0", len(serviceIns.snapshot()))
	}
}

func TestHostNotificationLogRowAndServiceNotificationLogRowColumns(t *testing.T) {
	items, err := decodeNotificationLog(readFixture(t, "statusngin_notifications.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := items[0]

	hostRow := hostNotificationLogRow(ev, nil)
	if len(hostRow) != 11 {
		t.Fatalf("hostNotificationLogRow has %d values, want 11", len(hostRow))
	}
	if hostRow[0] != ev.HostName || hostRow[1] != ev.StartTime || hostRow[2] != ev.TimestampUsec {
		t.Fatalf("hostNotificationLogRow[0:3] = %v, want [%v %v %v]", hostRow[0:3], ev.HostName, ev.StartTime, ev.TimestampUsec)
	}

	serviceRow := serviceNotificationLogRow(ev, nil)
	if len(serviceRow) != 12 {
		t.Fatalf("serviceNotificationLogRow has %d values, want 12", len(serviceRow))
	}
	if serviceRow[0] != ev.HostName || serviceRow[1] != ev.ServiceDescription || serviceRow[2] != ev.StartTime || serviceRow[3] != ev.TimestampUsec {
		t.Fatalf("serviceNotificationLogRow[0:4] = %v, want [%v %v %v %v]",
			serviceRow[0:4], ev.HostName, ev.ServiceDescription, ev.StartTime, ev.TimestampUsec)
	}
}

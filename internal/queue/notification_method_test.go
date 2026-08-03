package queue

import (
	"context"
	"testing"

	"statusengine-worker/internal/websocket"
)

func TestContactNotificationMethodHandlerDiscardsNonEndType(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[notificationMethodEvent]{}
	serviceIns := &fakeEnqueuer[notificationMethodEvent]{}
	handler := newContactNotificationMethodHandler(hub, QueueContactNotificationMethod, hostIns, serviceIns)

	// The real fixture carries type 605 (NEBTYPE_CONTACTNOTIFICATIONMETHOD_END)
	// and a service_description, so it must land in serviceIns.
	if err := handler(ctx, readFixture(t, "statusngin_contactnotificationmethod.json")); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(hostIns.snapshot()); got != 0 {
		t.Fatalf("hostIns got %d items, want 0", got)
	}
	if got := len(serviceIns.snapshot()); got != 1 {
		t.Fatalf("serviceIns got %d items, want 1", got)
	}

	// Any other type value must be discarded immediately - neither inserter
	// sees a new item.
	other := []byte(`{
		"type": 604,
		"timestamp": 1785517089,
		"timestamp_usec": 927284,
		"contactnotificationmethod": {
			"host_name": "localhost",
			"service_description": "Swap Usage",
			"contact_name": "someone"
		}
	}`)
	if err := handler(ctx, other); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(hostIns.snapshot()); got != 0 {
		t.Fatalf("hostIns got %d items after non-605 event, want 0", got)
	}
	if got := len(serviceIns.snapshot()); got != 1 {
		t.Fatalf("serviceIns got %d items after non-605 event, want still 1", got)
	}
}

func TestContactNotificationMethodHandlerRoutesHostVsService(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[notificationMethodEvent]{}
	serviceIns := &fakeEnqueuer[notificationMethodEvent]{}
	handler := newContactNotificationMethodHandler(hub, QueueContactNotificationMethod, hostIns, serviceIns)

	hostEvent := []byte(`{
		"type": 605,
		"timestamp": 111,
		"timestamp_usec": 222,
		"contactnotificationmethod": {
			"host_name": "localhost",
			"service_description": "",
			"contact_name": "someone"
		}
	}`)
	if err := handler(ctx, hostEvent); err != nil {
		t.Fatalf("handler: %v", err)
	}

	got := hostIns.snapshot()
	if len(got) != 1 {
		t.Fatalf("hostIns got %d items, want 1", len(got))
	}
	if got[0].Timestamp != 111 || got[0].TimestampUsec != 222 {
		t.Fatalf("start_time/start_time_usec = %d/%d, want 111/222", got[0].Timestamp, got[0].TimestampUsec)
	}
	if len(serviceIns.snapshot()) != 0 {
		t.Fatalf("serviceIns got %d items, want 0", len(serviceIns.snapshot()))
	}
}

func TestHostNotificationRowAndServiceNotificationRowColumns(t *testing.T) {
	items, err := decodeContactNotificationMethod(readFixture(t, "statusngin_contactnotificationmethod.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := items[0]

	hostRow := hostNotificationRow(ev)
	if len(hostRow) != 12 {
		t.Fatalf("hostNotificationRow has %d values, want 12", len(hostRow))
	}
	if hostRow[0] != ev.HostName || hostRow[1] != ev.Timestamp || hostRow[2] != ev.TimestampUsec {
		t.Fatalf("hostNotificationRow[0:3] = %v, want [%v %v %v]", hostRow[0:3], ev.HostName, ev.Timestamp, ev.TimestampUsec)
	}

	serviceRow := serviceNotificationRow(ev)
	if len(serviceRow) != 13 {
		t.Fatalf("serviceNotificationRow has %d values, want 13", len(serviceRow))
	}
	if serviceRow[0] != ev.ServiceDescription || serviceRow[1] != ev.Timestamp || serviceRow[2] != ev.TimestampUsec {
		t.Fatalf("serviceNotificationRow[0:3] = %v, want [%v %v %v]", serviceRow[0:3], ev.ServiceDescription, ev.Timestamp, ev.TimestampUsec)
	}
	if serviceRow[3] != ev.HostName {
		t.Fatalf("serviceNotificationRow[3] (hostname) = %v, want %v", serviceRow[3], ev.HostName)
	}
}

package queue

import (
	"context"
	"encoding/json"
	"testing"

	"statusengine-worker/internal/types"
	"statusengine-worker/internal/websocket"
)

// TestStateChangeHandlerRoutesOnStateChangeType proves newStateChangeHandler
// routes purely on statechange_type (0 = host, 1 = service), not on whether
// service_description happens to be set.
func TestStateChangeHandlerRoutesOnStateChangeType(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[stateChangeEvent]{}
	serviceIns := &fakeEnqueuer[stateChangeEvent]{}
	handler := newStateChangeHandler(hub, QueueStateChanges, hostIns, serviceIns)

	// statechange_type 0, even with a service_description present, must
	// still land in hostIns.
	hostTyped := marshalStateChangeMessage(types.StateChangePayload{
		HostName:           "localhost",
		ServiceDescription: "Flapping",
		StateChangeType:    0,
	})
	if err := handler(ctx, hostTyped); err != nil {
		t.Fatalf("handle (type 0): %v", err)
	}
	if got := len(hostIns.snapshot()); got != 1 {
		t.Fatalf("hostIns got %d items, want 1", got)
	}
	if got := len(serviceIns.snapshot()); got != 0 {
		t.Fatalf("serviceIns got %d items, want 0", got)
	}

	// statechange_type 1, even with no service_description at all, must
	// still land in serviceIns.
	serviceTyped := marshalStateChangeMessage(types.StateChangePayload{
		HostName:        "localhost",
		StateChangeType: 1,
	})
	if err := handler(ctx, serviceTyped); err != nil {
		t.Fatalf("handle (type 1): %v", err)
	}
	if got := len(hostIns.snapshot()); got != 1 {
		t.Fatalf("hostIns got %d items, want still 1", got)
	}
	if got := len(serviceIns.snapshot()); got != 1 {
		t.Fatalf("serviceIns got %d items, want 1", got)
	}
}

func marshalStateChangeMessage(p types.StateChangePayload) []byte {
	bulk := types.StateChangeBulk{
		Messages: []types.StateChangeMessage{
			{
				Envelope:    types.Envelope{Timestamp: 111, TimestampUsec: 222},
				StateChange: p,
			},
		},
	}
	raw, err := json.Marshal(bulk)
	if err != nil {
		panic(err)
	}
	return raw
}

func TestHostStateHistoryRowAndServiceStateHistoryRowColumns(t *testing.T) {
	items, err := decodeStateChange(readFixture(t, "statusngin_statechanges.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := items[0]

	hostRow := hostStateHistoryRow(ev, nil)
	if len(hostRow) != 12 {
		t.Fatalf("hostStateHistoryRow has %d values, want 12", len(hostRow))
	}
	if hostRow[0] != ev.HostName || hostRow[1] != ev.Timestamp || hostRow[2] != ev.TimestampUsec || hostRow[3] != ev.StateChangeType {
		t.Fatalf("hostStateHistoryRow[0:4] = %v, want [%v %v %v %v]",
			hostRow[0:4], ev.HostName, ev.Timestamp, ev.TimestampUsec, ev.StateChangeType)
	}

	serviceRow := serviceStateHistoryRow(ev, nil)
	if len(serviceRow) != 13 {
		t.Fatalf("serviceStateHistoryRow has %d values, want 13", len(serviceRow))
	}
	if serviceRow[0] != ev.ServiceDescription || serviceRow[1] != ev.Timestamp || serviceRow[2] != ev.TimestampUsec {
		t.Fatalf("serviceStateHistoryRow[0:3] = %v, want [%v %v %v]", serviceRow[0:3], ev.ServiceDescription, ev.Timestamp, ev.TimestampUsec)
	}
	if serviceRow[3] != ev.HostName || serviceRow[4] != ev.StateChangeType {
		t.Fatalf("serviceStateHistoryRow[3:5] = %v, want [%v %v]", serviceRow[3:5], ev.HostName, ev.StateChangeType)
	}
}

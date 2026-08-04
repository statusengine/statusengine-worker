package queue

import (
	"context"
	"encoding/json"
	"testing"

	"statusengine-worker/internal/types"
	"statusengine-worker/internal/websocket"
)

// TestAcknowledgementHandlerRoutesOnAcknowledgementType proves
// newAcknowledgementHandler routes purely on acknowledgement_type (0 = host,
// 1 = service), not on whether service_description happens to be set - the
// legacy broker_acknowledgement_data quirk documented in registry.go.
func TestAcknowledgementHandlerRoutesOnAcknowledgementType(t *testing.T) {
	hub := websocket.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hostIns := &fakeEnqueuer[acknowledgementEvent]{}
	serviceIns := &fakeEnqueuer[acknowledgementEvent]{}
	handler := newAcknowledgementHandler(hub, QueueAcknowledgements, hostIns, serviceIns)

	// acknowledgement_type 0, even with a service_description present, must
	// still land in hostIns.
	hostTyped := marshalAckMessage(types.AcknowledgementPayload{
		HostName:            "localhost",
		ServiceDescription:  "Swap Usage",
		AcknowledgementType: 0,
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

	// acknowledgement_type 1, even with no service_description at all, must
	// still land in serviceIns.
	serviceTyped := marshalAckMessage(types.AcknowledgementPayload{
		HostName:            "localhost",
		AcknowledgementType: 1,
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

func marshalAckMessage(p types.AcknowledgementPayload) []byte {
	msg := types.AcknowledgementMessage{
		Envelope:        types.Envelope{Type: types.EventTypeAcknowledgement, Timestamp: 111, TimestampUsec: 222},
		Acknowledgement: p,
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return raw
}

package queue

import (
	"encoding/json"

	"statusengine-worker/internal/types"
)

// stateChangeEvent augments types.StateChangePayload with the timestamp of
// the message that carried it: the statechange object itself carries no
// time field of its own (see .claude/specs/statusngin_statechanges.json),
// only the message Envelope does.
type stateChangeEvent struct {
	Timestamp int64 `json:"timestamp"`
	types.StateChangePayload
}

// acknowledgementEvent augments types.AcknowledgementPayload with the
// entry time of the message that carried it: the acknowledgement object
// itself carries no entry_time, but statusengine_host_acknowledgements and
// statusengine_service_acknowledgements both key on one.
type acknowledgementEvent struct {
	EntryTime     int64 `json:"entry_time"`
	EntryTimeUsec int   `json:"entry_time_usec"`
	types.AcknowledgementPayload
}

func decodeHostStatus(payload []byte) ([]types.HostStatusPayload, error) {
	var bulk types.HostStatusBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]types.HostStatusPayload, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = m.HostStatus
	}
	return items, nil
}

func decodeServiceStatus(payload []byte) ([]types.ServiceStatusPayload, error) {
	var bulk types.ServiceStatusBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]types.ServiceStatusPayload, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = m.ServiceStatus
	}
	return items, nil
}

func decodeHostCheck(payload []byte) ([]types.HostCheckPayload, error) {
	var bulk types.HostCheckBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]types.HostCheckPayload, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = m.HostCheck
	}
	return items, nil
}

// decodeServiceCheck is shared by the statusngin_servicechecks and
// statusngin_service_perfdata queues - both deliver the same "servicecheck"
// wire format (see types.ServiceCheckPayload's doc comment).
func decodeServiceCheck(payload []byte) ([]types.ServiceCheckPayload, error) {
	var bulk types.ServiceCheckBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]types.ServiceCheckPayload, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = m.ServiceCheck
	}
	return items, nil
}

// perfdataEvent augments types.ServiceCheckPayload with the timestamp of
// the message that carried it: statusengine_perfdata needs a message
// timestamp per metric point, distinct from the check's own start_time
// (CLAUDE.md rule 5).
type perfdataEvent struct {
	Timestamp int64 `json:"timestamp"`
	types.ServiceCheckPayload
}

func decodePerfdata(payload []byte) ([]perfdataEvent, error) {
	var bulk types.ServiceCheckBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]perfdataEvent, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = perfdataEvent{Timestamp: m.Timestamp, ServiceCheckPayload: m.ServiceCheck}
	}
	return items, nil
}

func decodeStateChange(payload []byte) ([]stateChangeEvent, error) {
	var bulk types.StateChangeBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]stateChangeEvent, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = stateChangeEvent{Timestamp: m.Timestamp, StateChangePayload: m.StateChange}
	}
	return items, nil
}

func decodeLogEntry(payload []byte) ([]types.LogEntryPayload, error) {
	var bulk types.LogEntryBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]types.LogEntryPayload, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = m.LogEntry
	}
	return items, nil
}

func decodeNotification(payload []byte) ([]types.NotificationPayload, error) {
	var bulk types.NotificationBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]types.NotificationPayload, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = m.NotificationData
	}
	return items, nil
}

// The remaining queues are the CLAUDE.md bulk exceptions: each message is a
// single JSON object, not a {"messages": [...]} envelope, so their decode
// functions always return a slice of at most one item.

func decodeContactNotificationMethod(payload []byte) ([]types.ContactNotificationMethodPayload, error) {
	var msg types.ContactNotificationMethodMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}
	return []types.ContactNotificationMethodPayload{msg.ContactNotificationMethod}, nil
}

func decodeAcknowledgement(payload []byte) ([]acknowledgementEvent, error) {
	var msg types.AcknowledgementMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}
	return []acknowledgementEvent{{
		EntryTime:              msg.Timestamp,
		EntryTimeUsec:          msg.TimestampUsec,
		AcknowledgementPayload: msg.Acknowledgement,
	}}, nil
}

func decodeDowntime(payload []byte) ([]types.DowntimePayload, error) {
	var msg types.DowntimeMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}
	return []types.DowntimePayload{msg.Downtime}, nil
}

// decodeCoreRestart has no envelope at all - just {"object_type": 102}.
func decodeCoreRestart(payload []byte) ([]types.CoreRestartMessage, error) {
	var msg types.CoreRestartMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}
	return []types.CoreRestartMessage{msg}, nil
}

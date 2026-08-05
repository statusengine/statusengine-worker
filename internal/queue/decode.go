package queue

import (
	"encoding/json"

	"statusengine-worker/internal/types"
)

// stateChangeEvent augments types.StateChangePayload with the timestamp of
// the message that carried it: the statechange object itself carries no
// time field of its own (see .claude/specs/statusngin_statechanges.json),
// only the message Envelope does. Timestamp/TimestampUsec become
// statusengine_host_statehistory's and statusengine_service_statehistory's
// state_time/state_time_usec columns.
type stateChangeEvent struct {
	Timestamp     int64 `json:"timestamp"`
	TimestampUsec int   `json:"timestamp_usec"`
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

// hostStatusEvent augments types.HostStatusPayload with the timestamp of the
// message that carried it: statusengine_hoststatus's status_update_time
// column has no equivalent field on the hoststatus object itself, only the
// message Envelope does.
type hostStatusEvent struct {
	Timestamp int64 `json:"timestamp"`
	types.HostStatusPayload
}

// serviceStatusEvent is the servicestatus equivalent of hostStatusEvent, for
// statusengine_servicestatus's status_update_time column.
type serviceStatusEvent struct {
	Timestamp int64 `json:"timestamp"`
	types.ServiceStatusPayload
}

func decodeHostStatus(payload []byte) ([]hostStatusEvent, error) {
	var bulk types.HostStatusBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]hostStatusEvent, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = hostStatusEvent{Timestamp: m.Timestamp, HostStatusPayload: m.HostStatus}
	}
	return items, nil
}

func decodeServiceStatus(payload []byte) ([]serviceStatusEvent, error) {
	var bulk types.ServiceStatusBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]serviceStatusEvent, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = serviceStatusEvent{Timestamp: m.Timestamp, ServiceStatusPayload: m.ServiceStatus}
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
		items[i] = stateChangeEvent{Timestamp: m.Timestamp, TimestampUsec: m.TimestampUsec, StateChangePayload: m.StateChange}
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

// notificationLogEvent augments types.NotificationPayload with the envelope
// fields the handler needs: Type gates the early-exit filter (only
// NEBTYPE_NOTIFICATION_END, 601, is persisted), and TimestampUsec becomes
// statusengine_host_notifications_log's and
// statusengine_service_notifications_log's start_time_usec column. Unlike
// notificationMethodEvent/stateChangeEvent, the start_time column itself
// comes straight from the payload's own StartTime field, not the envelope's
// Timestamp - the notification_data object already carries its own
// start_time.
type notificationLogEvent struct {
	Type          int `json:"type"`
	TimestampUsec int `json:"timestamp_usec"`
	types.NotificationPayload
}

func decodeNotificationLog(payload []byte) ([]notificationLogEvent, error) {
	var bulk types.NotificationBulk
	if err := json.Unmarshal(payload, &bulk); err != nil {
		return nil, err
	}
	items := make([]notificationLogEvent, len(bulk.Messages))
	for i, m := range bulk.Messages {
		items[i] = notificationLogEvent{Type: m.Type, TimestampUsec: m.TimestampUsec, NotificationPayload: m.NotificationData}
	}
	return items, nil
}

// The remaining queues are the CLAUDE.md bulk exceptions: each message is a
// single JSON object, not a {"messages": [...]} envelope, so their decode
// functions always return a slice of at most one item.

// notificationMethodEvent augments types.ContactNotificationMethodPayload
// with the envelope fields the handler needs: Type gates the early-exit
// filter (only NEBTYPE_CONTACTNOTIFICATIONMETHOD_END, 605, is persisted),
// and Timestamp/TimestampUsec become statusengine_host_notifications' and
// statusengine_service_notifications' start_time/start_time_usec columns -
// the payload's own start_time field describes the parent notification's
// timeframe, not this specific delivery, mirroring how host/service
// acknowledgements key on the envelope's entry_time rather than a payload
// field.
type notificationMethodEvent struct {
	Type          int   `json:"type"`
	Timestamp     int64 `json:"timestamp"`
	TimestampUsec int   `json:"timestamp_usec"`
	types.ContactNotificationMethodPayload
}

func decodeContactNotificationMethod(payload []byte) ([]notificationMethodEvent, error) {
	var msg types.ContactNotificationMethodMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}
	return []notificationMethodEvent{{
		Type:                             msg.Type,
		Timestamp:                        msg.Timestamp,
		TimestampUsec:                    msg.TimestampUsec,
		ContactNotificationMethodPayload: msg.ContactNotificationMethod,
	}}, nil
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

// decodeDowntimeMessage returns the full types.DowntimeMessage - Envelope
// (Type/Attr/Timestamp) included - rather than just its Downtime payload:
// unlike every other queue, DetermineDowntimeActions needs Type and Attr to
// decide what to do, not just the payload fields (see
// .claude/specs/downtime_ablauf.txt section 1).
func decodeDowntimeMessage(payload []byte) (types.DowntimeMessage, error) {
	var msg types.DowntimeMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return types.DowntimeMessage{}, err
	}
	return msg, nil
}

// decodeCoreRestart has no envelope at all - just {"object_type": 102}.
func decodeCoreRestart(payload []byte) ([]types.CoreRestartMessage, error) {
	var msg types.CoreRestartMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}
	return []types.CoreRestartMessage{msg}, nil
}

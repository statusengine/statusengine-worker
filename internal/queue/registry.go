package queue

import (
	"context"
	"database/sql"
	"fmt"

	"statusengine-worker/internal/db"
	"statusengine-worker/internal/types"
	"statusengine-worker/internal/websocket"
)

// Queue names as delivered by the Gearman/RabbitMQ brokers. These double as
// the WebSocket topic clients subscribe to (CLAUDE.md rule 4: "Connected
// clients can subscribe to specific event types, e.g. only
// statusngin_hoststatus").
const (
	QueueHostStatus                = "statusngin_hoststatus"
	QueueServiceStatus             = "statusngin_servicestatus"
	QueueHostChecks                = "statusngin_hostchecks"
	QueueServiceChecks             = "statusngin_servicechecks"
	QueueServicePerfdata           = "statusngin_service_perfdata"
	QueueStateChanges              = "statusngin_statechanges"
	QueueLogEntries                = "statusngin_logentries"
	QueueNotifications             = "statusngin_notifications"
	QueueContactNotificationMethod = "statusngin_contactnotificationmethod"
	QueueAcknowledgements          = "statusngin_acknowledgements"
	QueueDowntimes                 = "statusngin_downtimes"
	QueueCoreRestart               = "statusngin_core_restart"
)

// isHardState maps the standard Nagios/Icinga/Naemon state_type convention
// (0 = SOFT, 1 = HARD) to the tinyint(1) is_hardstate column.
func isHardState(stateType int) int {
	if stateType == 1 {
		return 1
	}
	return 0
}

func hostCheckRow(p types.HostCheckPayload) []any {
	return []any{
		p.HostName, p.StartTime, p.State, isHardState(p.StateType), p.EndTime,
		p.Output, p.LongOutput, p.Timeout, p.EarlyTimeout, p.Latency,
		p.ExecutionTime, p.PerfData, p.CommandLine, p.CurrentAttempt, p.MaxAttempts,
	}
}

func serviceCheckRow(p types.ServiceCheckPayload) []any {
	return []any{
		p.ServiceDescription, p.StartTime, p.HostName, p.State, isHardState(p.StateType), p.EndTime,
		p.Output, p.LongOutput, p.Timeout, p.EarlyTimeout, p.Latency,
		p.ExecutionTime, p.PerfData, p.CommandLine, p.CurrentAttempt, p.MaxAttempts,
	}
}

func logEntryRow(p types.LogEntryPayload) []any {
	return []any{p.EntryTime, p.DataType, p.Data}
}

func hostStateHistoryRow(ev stateChangeEvent) []any {
	return []any{
		ev.HostName, ev.Timestamp, ev.StateChangeType, ev.State, isHardState(ev.StateType),
		ev.CurrentAttempt, ev.MaxAttempts, ev.LastState, ev.LastHardState, ev.Output, ev.LongOutput,
	}
}

func serviceStateHistoryRow(ev stateChangeEvent) []any {
	return []any{
		ev.ServiceDescription, ev.Timestamp, ev.HostName, ev.StateChangeType, ev.State, isHardState(ev.StateType),
		ev.CurrentAttempt, ev.MaxAttempts, ev.LastState, ev.LastHardState, ev.Output, ev.LongOutput,
	}
}

func hostAcknowledgementRow(ev acknowledgementEvent) []any {
	return []any{
		ev.HostName, ev.EntryTime, ev.State, ev.AuthorName, ev.CommentData,
		ev.AcknowledgementType, ev.IsSticky, ev.PersistentComment, ev.NotifyContacts,
	}
}

func serviceAcknowledgementRow(ev acknowledgementEvent) []any {
	return []any{
		ev.ServiceDescription, ev.EntryTime, ev.HostName, ev.State, ev.AuthorName, ev.CommentData,
		ev.AcknowledgementType, ev.IsSticky, ev.PersistentComment, ev.NotifyContacts,
	}
}

// newStateChangeHandler and newAcknowledgementHandler route each decoded
// item to one of two BulkInserters depending on whether it describes a
// host or a service, mirroring the schema's separate host/service tables.
// Both items still publish to the same WebSocket topic (the queue name);
// only MySQL persistence is split.

func newStateChangeHandler(hub *websocket.Hub, topic string, hostIns, serviceIns enqueuer[stateChangeEvent]) Handler {
	return func(ctx context.Context, payload []byte) error {
		events, err := decodeStateChange(payload)
		if err != nil {
			return fmt.Errorf("queue: decode %s: %w", topic, err)
		}

		for _, ev := range events {
			publish(hub, topic, ev)

			ins := hostIns
			if ev.ServiceDescription != "" {
				ins = serviceIns
			}
			if err := ins.Enqueue(ctx, ev); err != nil {
				return fmt.Errorf("queue: enqueue %s event: %w", topic, err)
			}
		}
		return nil
	}
}

func newAcknowledgementHandler(hub *websocket.Hub, topic string, hostIns, serviceIns enqueuer[acknowledgementEvent]) Handler {
	return func(ctx context.Context, payload []byte) error {
		events, err := decodeAcknowledgement(payload)
		if err != nil {
			return fmt.Errorf("queue: decode %s: %w", topic, err)
		}

		for _, ev := range events {
			publish(hub, topic, ev)

			ins := hostIns
			if ev.ServiceDescription != "" {
				ins = serviceIns
			}
			if err := ins.Enqueue(ctx, ev); err != nil {
				return fmt.Errorf("queue: enqueue %s event: %w", topic, err)
			}
		}
		return nil
	}
}

// NewRouter wires every known queue to a Handler that decodes its payload
// and dispatches each item onward. Queues whose payload maps cleanly onto a
// destination table (CLAUDE.md rule 3) get a dedicated *db.BulkInserter;
// their host/service variants are split across the schema's separate
// tables. Queues without an unambiguous destination table - hoststatus and
// servicestatus need legacy-specific fields (e.g. is_passive_check,
// node_name) that aren't derivable from the sample payloads alone, see the
// legacy reference in CLAUDE.md; service_perfdata's Graphite routing (rule
// 5) isn't implemented yet; contactnotificationmethod and core_restart have
// no matching table at all - are broadcast to WebSocket subscribers only.
//
// The returned []Runner lets the caller start every BulkInserter's Run loop
// and Flush them on graceful shutdown without needing to know each one's
// concrete item type.
func NewRouter(sqlDB *sql.DB, hub *websocket.Hub) (Router, []Runner) {
	hostChecks := db.NewBulkInserter(sqlDB, "statusengine_hostchecks",
		[]string{"hostname", "start_time", "state", "is_hardstate", "end_time", "output", "long_output",
			"timeout", "early_timeout", "latency", "execution_time", "perfdata", "command",
			"current_check_attempt", "max_check_attempts"},
		hostCheckRow)

	serviceChecks := db.NewBulkInserter(sqlDB, "statusengine_servicechecks",
		[]string{"service_description", "start_time", "hostname", "state", "is_hardstate", "end_time", "output",
			"long_output", "timeout", "early_timeout", "latency", "execution_time", "perfdata", "command",
			"current_check_attempt", "max_check_attempts"},
		serviceCheckRow)

	logEntries := db.NewBulkInserter(sqlDB, "statusengine_logentries",
		[]string{"entry_time", "logentry_type", "logentry_data"},
		logEntryRow)

	hostStateHistory := db.NewBulkInserter(sqlDB, "statusengine_host_statehistory",
		[]string{"hostname", "state_time", "state_change", "state", "is_hardstate", "current_check_attempt",
			"max_check_attempts", "last_state", "last_hard_state", "output", "long_output"},
		hostStateHistoryRow)

	serviceStateHistory := db.NewBulkInserter(sqlDB, "statusengine_service_statehistory",
		[]string{"service_description", "state_time", "hostname", "state_change", "state", "is_hardstate",
			"current_check_attempt", "max_check_attempts", "last_state", "last_hard_state", "output", "long_output"},
		serviceStateHistoryRow)

	hostAcks := db.NewBulkInserter(sqlDB, "statusengine_host_acknowledgements",
		[]string{"hostname", "entry_time", "state", "author_name", "comment_data",
			"acknowledgement_type", "is_sticky", "persistent_comment", "notify_contacts"},
		hostAcknowledgementRow)

	serviceAcks := db.NewBulkInserter(sqlDB, "statusengine_service_acknowledgements",
		[]string{"service_description", "entry_time", "hostname", "state", "author_name", "comment_data",
			"acknowledgement_type", "is_sticky", "persistent_comment", "notify_contacts"},
		serviceAcknowledgementRow)

	router := Router{
		QueueHostChecks:       NewHandler(hub, QueueHostChecks, hostChecks, decodeHostCheck),
		QueueServiceChecks:    NewHandler(hub, QueueServiceChecks, serviceChecks, decodeServiceCheck),
		QueueLogEntries:       NewHandler(hub, QueueLogEntries, logEntries, decodeLogEntry),
		QueueStateChanges:     newStateChangeHandler(hub, QueueStateChanges, hostStateHistory, serviceStateHistory),
		QueueAcknowledgements: newAcknowledgementHandler(hub, QueueAcknowledgements, hostAcks, serviceAcks),

		QueueHostStatus:                NewBroadcastHandler(hub, QueueHostStatus, decodeHostStatus),
		QueueServiceStatus:             NewBroadcastHandler(hub, QueueServiceStatus, decodeServiceStatus),
		QueueServicePerfdata:           NewBroadcastHandler(hub, QueueServicePerfdata, decodeServiceCheck),
		QueueNotifications:             NewBroadcastHandler(hub, QueueNotifications, decodeNotification),
		QueueContactNotificationMethod: NewBroadcastHandler(hub, QueueContactNotificationMethod, decodeContactNotificationMethod),
		QueueDowntimes:                 NewBroadcastHandler(hub, QueueDowntimes, decodeDowntime),
		QueueCoreRestart:               NewBroadcastHandler(hub, QueueCoreRestart, decodeCoreRestart),
	}

	runners := []Runner{
		hostChecks, serviceChecks, logEntries,
		hostStateHistory, serviceStateHistory,
		hostAcks, serviceAcks,
	}

	return router, runners
}

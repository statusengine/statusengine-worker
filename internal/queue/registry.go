package queue

import (
	"context"
	"database/sql"
	"fmt"

	"statusengine-worker/internal/db"
	"statusengine-worker/internal/graphite"
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

// isPassiveCheck maps the standard Nagios/Icinga/Naemon check_type
// convention (0 = ACTIVE, 1 = PASSIVE) to the tinyint(1) is_passive_check
// column: check_type == 0 means the check was active, so is_passive_check
// is 1 only when check_type is anything else.
func isPassiveCheck(checkType int) int {
	if checkType == 0 {
		return 1
	}
	return 0
}

// hostStatusColumns and hostStatusUpdateColumns are shared by NewRouter's
// hostStatus BulkInserter and newHostStatusRow: hostStatusUpdateColumns is
// every hostStatusColumns entry except the table's primary key ("hostname"),
// refreshed via ON DUPLICATE KEY UPDATE on every repeat status update for
// the same host.
var hostStatusColumns = []string{
	"hostname", "status_update_time", "output", "long_output", "perfdata", "current_state",
	"current_check_attempt", "max_check_attempts", "last_check", "next_check", "is_passive_check",
	"last_state_change", "last_hard_state_change", "last_hard_state", "is_hardstate",
	"last_notification", "next_notification", "notifications_enabled", "problem_has_been_acknowledged",
	"acknowledgement_type", "passive_checks_enabled", "active_checks_enabled", "event_handler_enabled",
	"flap_detection_enabled", "is_flapping", "latency", "execution_time", "scheduled_downtime_depth",
	"process_performance_data", "obsess_over_host", "normal_check_interval", "retry_check_interval",
	"check_timeperiod", "node_name", "last_time_up", "last_time_down", "last_time_unreachable",
	"current_notification_number", "percent_state_change", "event_handler", "check_command",
}

var hostStatusUpdateColumns = hostStatusColumns[1:]

// serviceStatusColumns and serviceStatusUpdateColumns mirror hostStatusColumns
// for statusengine_servicestatus, whose primary key is the composite
// (hostname, service_description) - so both are excluded from the update list.
var serviceStatusColumns = []string{
	"hostname", "service_description", "status_update_time", "output", "long_output", "perfdata",
	"current_state", "current_check_attempt", "max_check_attempts", "last_check", "next_check",
	"is_passive_check", "last_state_change", "last_hard_state_change", "last_hard_state", "is_hardstate",
	"last_notification", "next_notification", "notifications_enabled", "problem_has_been_acknowledged",
	"acknowledgement_type", "passive_checks_enabled", "active_checks_enabled", "event_handler_enabled",
	"flap_detection_enabled", "is_flapping", "latency", "execution_time", "scheduled_downtime_depth",
	"process_performance_data", "obsess_over_service", "normal_check_interval", "retry_check_interval",
	"check_timeperiod", "node_name", "last_time_ok", "last_time_warning", "last_time_critical",
	"last_time_unknown", "current_notification_number", "percent_state_change", "event_handler", "check_command",
}

var serviceStatusUpdateColumns = serviceStatusColumns[2:]

// newHostStatusRow returns a RowFunc for statusengine_hoststatus, closing
// over nodeName since it comes from process configuration rather than the
// event itself (CLAUDE.md: worker-wide "nodename" option, default
// "statusengine").
func newHostStatusRow(nodeName string) db.RowFunc[hostStatusEvent] {
	return func(ev hostStatusEvent) []any {
		p := ev.HostStatusPayload
		return []any{
			p.Name, ev.Timestamp, p.PluginOutput, p.LongPluginOutput, p.PerfData, p.CurrentState,
			p.CurrentAttempt, p.MaxAttempts, p.LastCheck, p.NextCheck, isPassiveCheck(p.CheckType),
			p.LastStateChange, p.LastHardStateChange, p.LastHardState, isHardState(p.StateType),
			p.LastNotification, p.NextNotification, p.NotificationsEnabled, p.ProblemHasBeenAcknowledged,
			p.AcknowledgementType, p.AcceptPassiveChecks, p.ChecksEnabled, p.EventHandlerEnabled,
			p.FlapDetectionEnabled, p.IsFlapping, p.Latency, p.ExecutionTime, p.ScheduledDowntimeDepth,
			p.ProcessPerformanceData, p.Obsess, int(p.CheckInterval), int(p.RetryInterval),
			p.CheckPeriod, nodeName, p.LastTimeUp, p.LastTimeDown, p.LastTimeUnreachable,
			p.CurrentNotificationNumber, p.PercentStateChange, p.EventHandler, p.CheckCommand,
		}
	}
}

// newServiceStatusRow is the statusengine_servicestatus equivalent of
// newHostStatusRow.
func newServiceStatusRow(nodeName string) db.RowFunc[serviceStatusEvent] {
	return func(ev serviceStatusEvent) []any {
		p := ev.ServiceStatusPayload
		return []any{
			p.HostName, p.Description, ev.Timestamp, p.PluginOutput, p.LongPluginOutput, p.PerfData,
			p.CurrentState, p.CurrentAttempt, p.MaxAttempts, p.LastCheck, p.NextCheck,
			isPassiveCheck(p.CheckType), p.LastStateChange, p.LastHardStateChange, p.LastHardState,
			isHardState(p.StateType), p.LastNotification, p.NextNotification, p.NotificationsEnabled,
			p.ProblemHasBeenAcknowledged, p.AcknowledgementType, p.AcceptPassiveChecks, p.ChecksEnabled,
			p.EventHandlerEnabled, p.FlapDetectionEnabled, p.IsFlapping, p.Latency, p.ExecutionTime,
			p.ScheduledDowntimeDepth, p.ProcessPerformanceData, p.Obsess, int(p.CheckInterval),
			int(p.RetryInterval), p.CheckPeriod, nodeName, p.LastTimeOk, p.LastTimeWarning,
			p.LastTimeCritical, p.LastTimeUnknown, p.CurrentNotificationNumber, p.PercentStateChange,
			p.EventHandler, p.CheckCommand,
		}
	}
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
		ev.HostName, ev.Timestamp, ev.TimestampUsec,
		// NOTE: Unlike standard NDOUtils where 'state_change' indicates a state transition occurrence,
		// Statusengine repurposes this field to differentiate between Host (0) and Service (1) state history.
		// In standard NDO, this field is hardcoded to TRUE.
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/dbhandlers.c#L2940
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/ndomod.c#L3435
		ev.StateChangeType,
		ev.State, isHardState(ev.StateType),
		ev.CurrentAttempt, ev.MaxAttempts, ev.LastState, ev.LastHardState, ev.Output, ev.LongOutput,
	}
}

func serviceStateHistoryRow(ev stateChangeEvent) []any {
	return []any{
		ev.ServiceDescription, ev.Timestamp, ev.TimestampUsec, ev.HostName,
		// NOTE: Unlike standard NDOUtils where 'state_change' indicates a state transition occurrence,
		// Statusengine repurposes this field to differentiate between Host (0) and Service (1) state history.
		// In standard NDO, this field is hardcoded to TRUE.
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/dbhandlers.c#L2940
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/ndomod.c#L3435
		ev.StateChangeType,
		ev.State, isHardState(ev.StateType),
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

// notificationTypeContactNotificationMethodEnd is the Nagios/Icinga/Naemon
// NEBTYPE_CONTACTNOTIFICATIONMETHOD_END event type: the only
// contactnotificationmethod event that represents a completed notification
// method delivery, and therefore the only one persisted to
// statusengine_host_notifications/statusengine_service_notifications. Every
// other type value on this queue is discarded immediately.
const notificationTypeContactNotificationMethodEnd = 605

func hostNotificationRow(ev notificationMethodEvent) []any {
	return []any{
		ev.HostName, ev.Timestamp, ev.TimestampUsec, ev.ContactName, ev.CommandName, ev.CommandArgs,
		ev.State, ev.EndTime, ev.ReasonType, ev.Output, ev.AckAuthor, ev.AckData,
	}
}

func serviceNotificationRow(ev notificationMethodEvent) []any {
	return []any{
		ev.ServiceDescription, ev.Timestamp, ev.TimestampUsec, ev.HostName, ev.ContactName, ev.CommandName,
		ev.CommandArgs, ev.State, ev.EndTime, ev.ReasonType, ev.Output, ev.AckAuthor, ev.AckData,
	}
}

// newContactNotificationMethodHandler filters out every event whose type
// isn't notificationTypeContactNotificationMethodEnd, then routes the rest
// to hostIns or serviceIns depending on whether service_description is set
// - mirroring newStateChangeHandler/newAcknowledgementHandler's host-vs-
// service split.
func newContactNotificationMethodHandler(hub *websocket.Hub, topic string, hostIns, serviceIns enqueuer[notificationMethodEvent]) Handler {
	return func(ctx context.Context, payload []byte) error {
		events, err := decodeContactNotificationMethod(payload)
		if err != nil {
			return fmt.Errorf("queue: decode %s: %w", topic, err)
		}

		for _, ev := range events {
			if ev.Type != notificationTypeContactNotificationMethodEnd {
				continue
			}

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

// newStateChangeHandler and newAcknowledgementHandler route each decoded
// item to one of two BulkInserters depending on whether it describes a
// host or a service, mirroring the schema's separate host/service tables.
// newStateChangeHandler routes on StateChangeType (0 = host, 1 = service);
// newAcknowledgementHandler routes on AcknowledgementType instead - see the
// comment inside it for why. Both items still publish to the same WebSocket
// topic (the queue name); only MySQL persistence is split.

func newStateChangeHandler(hub *websocket.Hub, topic string, hostIns, serviceIns enqueuer[stateChangeEvent]) Handler {
	return func(ctx context.Context, payload []byte) error {
		events, err := decodeStateChange(payload)
		if err != nil {
			return fmt.Errorf("queue: decode %s: %w", topic, err)
		}

		for _, ev := range events {
			publish(hub, topic, ev)

			// statechange_type distinguishes host (0) from service (1) state
			// history the same way it's persisted into 'state_change' below -
			// see hostStateHistoryRow/serviceStateHistoryRow for why.
			ins := hostIns
			if ev.StateChangeType == 1 {
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

			// In the broker_acknowledgement_data callback, acknowledgement type is used to determine if it is a host or service acknowledgement
			// This is a differente behavior than the broker_host_status and broker_service_status callbacks have -.-
			// 0 = HOST_ACKNOWLEDGEMENT
			// 1 = SERVICE_ACKNOWLEDGEMENT
			ins := hostIns
			if ev.AcknowledgementType == 1 {
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
// tables. hoststatus and servicestatus are upserted (CLAUDE.md rule 3: still
// batched at 100 rows/250ms, just as "INSERT ... ON DUPLICATE KEY UPDATE ..."
// instead of a plain INSERT) since repeated status snapshots for the same
// host/service collide on the tables' primary keys; is_passive_check is
// derived from check_type (0 = active) and node_name comes from the
// worker-wide nodeName config value, since neither is present in the sample
// payloads' hoststatus/servicestatus objects themselves. contactnotificationmethod
// is filtered before persistence - only NEBTYPE_CONTACTNOTIFICATIONMETHOD_END
// (605) events are kept, then split into statusengine_host_notifications /
// statusengine_service_notifications - see newContactNotificationMethodHandler.
// core_restart has no matching table at all and is broadcast to WebSocket
// subscribers only. service_perfdata gets its own handler (see processor.go),
// routing each parsed metric to MySQL, Graphite, or both per perfdataRoute
// (CLAUDE.md rule 5).
//
// gc's Run loop is started and Flushed by the caller alongside the
// returned []Runner (see the runners slice below) even when perfdataRoute
// excludes Graphite: Enqueue is then simply never called on it, so no
// connection is ever dialed.
//
// The returned []Runner lets the caller start every BulkInserter's Run loop
// and Flush them on graceful shutdown without needing to know each one's
// concrete item type.
//
// nodeName is written into every hoststatus/servicestatus row's node_name
// column (worker-wide "nodename" config option, default "statusengine").
func NewRouter(sqlDB *sql.DB, hub *websocket.Hub, gc *graphite.Client, perfdataRoute PerfdataRoute, nodeName string) (Router, []Runner) {
	hostStatus := db.NewUpsertBulkInserter(sqlDB, "statusengine_hoststatus",
		hostStatusColumns, hostStatusUpdateColumns, newHostStatusRow(nodeName))

	serviceStatus := db.NewUpsertBulkInserter(sqlDB, "statusengine_servicestatus",
		serviceStatusColumns, serviceStatusUpdateColumns, newServiceStatusRow(nodeName))

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
		[]string{"hostname", "state_time", "state_time_usec", "state_change", "state", "is_hardstate",
			"current_check_attempt", "max_check_attempts", "last_state", "last_hard_state", "output", "long_output"},
		hostStateHistoryRow)

	serviceStateHistory := db.NewBulkInserter(sqlDB, "statusengine_service_statehistory",
		[]string{"service_description", "state_time", "state_time_usec", "hostname", "state_change", "state",
			"is_hardstate", "current_check_attempt", "max_check_attempts", "last_state", "last_hard_state",
			"output", "long_output"},
		serviceStateHistoryRow)

	hostAcks := db.NewBulkInserter(sqlDB, "statusengine_host_acknowledgements",
		[]string{"hostname", "entry_time", "state", "author_name", "comment_data",
			"acknowledgement_type", "is_sticky", "persistent_comment", "notify_contacts"},
		hostAcknowledgementRow)

	serviceAcks := db.NewBulkInserter(sqlDB, "statusengine_service_acknowledgements",
		[]string{"service_description", "entry_time", "hostname", "state", "author_name", "comment_data",
			"acknowledgement_type", "is_sticky", "persistent_comment", "notify_contacts"},
		serviceAcknowledgementRow)

	perfdata := db.NewBulkInserter(sqlDB, "statusengine_perfdata",
		[]string{"hostname", "service_description", "label", "timestamp", "timestamp_unix", "value", "unit"},
		perfdataRow)

	hostNotifications := db.NewBulkInserter(sqlDB, "statusengine_host_notifications",
		[]string{"hostname", "start_time", "start_time_usec", "contact_name", "command_name", "command_args",
			"state", "end_time", "reason_type", "output", "ack_author", "ack_data"},
		hostNotificationRow)

	serviceNotifications := db.NewBulkInserter(sqlDB, "statusengine_service_notifications",
		[]string{"service_description", "start_time", "start_time_usec", "hostname", "contact_name",
			"command_name", "command_args", "state", "end_time", "reason_type", "output", "ack_author", "ack_data"},
		serviceNotificationRow)

	router := Router{
		QueueHostChecks:                NewHandler(hub, QueueHostChecks, hostChecks, decodeHostCheck),
		QueueServiceChecks:             NewHandler(hub, QueueServiceChecks, serviceChecks, decodeServiceCheck),
		QueueLogEntries:                NewHandler(hub, QueueLogEntries, logEntries, decodeLogEntry),
		QueueStateChanges:              newStateChangeHandler(hub, QueueStateChanges, hostStateHistory, serviceStateHistory),
		QueueAcknowledgements:          newAcknowledgementHandler(hub, QueueAcknowledgements, hostAcks, serviceAcks),
		QueueServicePerfdata:           NewPerfdataHandler(hub, QueueServicePerfdata, perfdataRoute, perfdata, gc),
		QueueHostStatus:                NewHandler(hub, QueueHostStatus, hostStatus, decodeHostStatus),
		QueueServiceStatus:             NewHandler(hub, QueueServiceStatus, serviceStatus, decodeServiceStatus),
		QueueContactNotificationMethod: newContactNotificationMethodHandler(hub, QueueContactNotificationMethod, hostNotifications, serviceNotifications),

		QueueNotifications: NewBroadcastHandler(hub, QueueNotifications, decodeNotification),
		QueueDowntimes:     NewBroadcastHandler(hub, QueueDowntimes, decodeDowntime),
		QueueCoreRestart:   NewBroadcastHandler(hub, QueueCoreRestart, decodeCoreRestart),
	}

	runners := []Runner{
		hostStatus, serviceStatus,
		hostChecks, serviceChecks, logEntries,
		hostStateHistory, serviceStateHistory,
		hostAcks, serviceAcks,
		hostNotifications, serviceNotifications,
		perfdata, gc,
	}

	return router, runners
}

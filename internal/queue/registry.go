package queue

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"statusengine-worker/internal/db"
	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/metrics"
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
	return func(ev hostStatusEvent, dst []any) []any {
		p := ev.HostStatusPayload
		return append(dst,
			p.Name, ev.Timestamp, p.PluginOutput, p.LongPluginOutput, p.PerfData, p.CurrentState,
			p.CurrentAttempt, p.MaxAttempts, p.LastCheck, p.NextCheck, isPassiveCheck(p.CheckType),
			p.LastStateChange, p.LastHardStateChange, p.LastHardState, isHardState(p.StateType),
			p.LastNotification, p.NextNotification, p.NotificationsEnabled, p.ProblemHasBeenAcknowledged,
			p.AcknowledgementType, p.AcceptPassiveChecks, p.ChecksEnabled, p.EventHandlerEnabled,
			p.FlapDetectionEnabled, p.IsFlapping, p.Latency, p.ExecutionTime, p.ScheduledDowntimeDepth,
			p.ProcessPerformanceData, p.Obsess, int(p.CheckInterval), int(p.RetryInterval),
			p.CheckPeriod, nodeName, p.LastTimeUp, p.LastTimeDown, p.LastTimeUnreachable,
			p.CurrentNotificationNumber, p.PercentStateChange, p.EventHandler, p.CheckCommand,
		)
	}
}

// newServiceStatusRow is the statusengine_servicestatus equivalent of
// newHostStatusRow.
func newServiceStatusRow(nodeName string) db.RowFunc[serviceStatusEvent] {
	return func(ev serviceStatusEvent, dst []any) []any {
		p := ev.ServiceStatusPayload
		return append(dst,
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
		)
	}
}

func hostCheckRow(ev hostCheckEvent, dst []any) []any {
	return append(dst,
		ev.HostName, ev.StartTime, ev.TimestampUsec, ev.State, isHardState(ev.StateType), ev.EndTime,
		ev.Output, ev.LongOutput, ev.Timeout, ev.EarlyTimeout, ev.Latency,
		ev.ExecutionTime, ev.PerfData, ev.CommandLine, ev.CurrentAttempt, ev.MaxAttempts,
	)
}

func serviceCheckRow(ev serviceCheckEvent, dst []any) []any {
	return append(dst,
		ev.ServiceDescription, ev.StartTime, ev.TimestampUsec, ev.HostName, ev.State, isHardState(ev.StateType), ev.EndTime,
		ev.Output, ev.LongOutput, ev.Timeout, ev.EarlyTimeout, ev.Latency,
		ev.ExecutionTime, ev.PerfData, ev.CommandLine, ev.CurrentAttempt, ev.MaxAttempts,
	)
}

// newLogEntryRow returns a RowFunc for statusengine_logentries, closing over
// nodeName since it comes from process configuration rather than the event
// itself (CLAUDE.md: worker-wide "nodename" option) - the logentry payload
// carries no node identifier of its own (see .claude/specs/statusngin_logentries.json).
func newLogEntryRow(nodeName string) db.RowFunc[types.LogEntryPayload] {
	return func(p types.LogEntryPayload, dst []any) []any {
		return append(dst, p.EntryTime, p.DataType, p.Data, nodeName)
	}
}

func hostStateHistoryRow(ev stateChangeEvent, dst []any) []any {
	return append(dst,
		ev.HostName, ev.Timestamp, ev.TimestampUsec,
		// NOTE: Unlike standard NDOUtils where 'state_change' indicates a state transition occurrence,
		// Statusengine repurposes this field to differentiate between Host (0) and Service (1) state history.
		// In standard NDO, this field is hardcoded to TRUE.
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/dbhandlers.c#L2940
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/ndomod.c#L3435
		ev.StateChangeType,
		ev.State, isHardState(ev.StateType),
		ev.CurrentAttempt, ev.MaxAttempts, ev.LastState, ev.LastHardState, ev.Output, ev.LongOutput,
	)
}

func serviceStateHistoryRow(ev stateChangeEvent, dst []any) []any {
	return append(dst,
		ev.ServiceDescription, ev.Timestamp, ev.TimestampUsec, ev.HostName,
		// NOTE: Unlike standard NDOUtils where 'state_change' indicates a state transition occurrence,
		// Statusengine repurposes this field to differentiate between Host (0) and Service (1) state history.
		// In standard NDO, this field is hardcoded to TRUE.
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/dbhandlers.c#L2940
		// https://github.com/NagiosEnterprises/ndoutils/blob/2a7171e36e67c5476b2825fffa7bf6a52042a1f5/src/ndomod.c#L3435
		ev.StateChangeType,
		ev.State, isHardState(ev.StateType),
		ev.CurrentAttempt, ev.MaxAttempts, ev.LastState, ev.LastHardState, ev.Output, ev.LongOutput,
	)
}

func hostAcknowledgementRow(ev acknowledgementEvent, dst []any) []any {
	return append(dst,
		ev.HostName, ev.EntryTime, ev.EntryTimeUsec, ev.State, ev.AuthorName, ev.CommentData,
		ev.AcknowledgementType, ev.IsSticky, ev.PersistentComment, ev.NotifyContacts,
	)
}

func serviceAcknowledgementRow(ev acknowledgementEvent, dst []any) []any {
	return append(dst,
		ev.ServiceDescription, ev.EntryTime, ev.EntryTimeUsec, ev.HostName, ev.State, ev.AuthorName, ev.CommentData,
		ev.AcknowledgementType, ev.IsSticky, ev.PersistentComment, ev.NotifyContacts,
	)
}

// notificationTypeContactNotificationMethodEnd is the Nagios/Icinga/Naemon
// NEBTYPE_CONTACTNOTIFICATIONMETHOD_END event type: the only
// contactnotificationmethod event that represents a completed notification
// method delivery, and therefore the only one persisted to
// statusengine_host_notifications/statusengine_service_notifications. Every
// other type value on this queue is discarded immediately.
const notificationTypeContactNotificationMethodEnd = 605

func hostNotificationRow(ev notificationMethodEvent, dst []any) []any {
	return append(dst,
		ev.HostName, ev.Timestamp, ev.TimestampUsec, ev.ContactName, ev.CommandName, ev.CommandArgs,
		ev.State, ev.EndTime, ev.ReasonType, ev.Output, ev.AckAuthor, ev.AckData,
	)
}

func serviceNotificationRow(ev notificationMethodEvent, dst []any) []any {
	return append(dst,
		ev.ServiceDescription, ev.Timestamp, ev.TimestampUsec, ev.HostName, ev.ContactName, ev.CommandName,
		ev.CommandArgs, ev.State, ev.EndTime, ev.ReasonType, ev.Output, ev.AckAuthor, ev.AckData,
	)
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
			return decodeError(topic, err)
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

// notificationTypeEnd is the Nagios/Icinga/Naemon NEBTYPE_NOTIFICATION_END
// event type: the only notification event persisted to
// statusengine_host_notifications_log/statusengine_service_notifications_log,
// and only once it actually reached at least one contact.
const notificationTypeEnd = 601

func hostNotificationLogRow(ev notificationLogEvent, dst []any) []any {
	// long_output is identical to output in this context, so it is skipped to save database space.
	return append(dst,
		ev.HostName, ev.StartTime, ev.TimestampUsec, ev.EndTime, ev.State, ev.ReasonType,
		ev.Escalated, ev.ContactsNotified, ev.Output, ev.AckAuthor, ev.AckData,
	)
}

func serviceNotificationLogRow(ev notificationLogEvent, dst []any) []any {
	// long_output is identical to output in this context, so it is skipped to save database space.
	return append(dst,
		ev.HostName, ev.ServiceDescription, ev.StartTime, ev.TimestampUsec, ev.EndTime, ev.State,
		ev.ReasonType, ev.Escalated, ev.ContactsNotified, ev.Output, ev.AckAuthor, ev.AckData,
	)
}

// newNotificationHandler discards every event that isn't
// notificationTypeEnd or that reached no contacts at all
// (contacts_notified <= 0), then routes the rest to hostIns or serviceIns
// depending on whether service_description is set.
func newNotificationHandler(hub *websocket.Hub, topic string, hostIns, serviceIns enqueuer[notificationLogEvent]) Handler {
	return func(ctx context.Context, payload []byte) error {
		events, err := decodeNotificationLog(payload)
		if err != nil {
			return decodeError(topic, err)
		}

		for _, ev := range events {
			if ev.Type != notificationTypeEnd || ev.ContactsNotified <= 0 {
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

// coreRestartObjectType is the Nagios/Icinga/Naemon broker object type for a
// monitoring core restart (see .claude/specs/statusngin_core_restart.json).
// Every other object_type value on this queue is ignored.
const coreRestartObjectType = 102

// hoststatusStaleDeleteQuery/servicestatusStaleDeleteQuery replace what used
// to be an unconditional TRUNCATE: statusngin_hoststatus/statusngin_servicestatus
// are consumed on their own separate Gearman/RabbitMQ queues from
// statusngin_core_restart, with no ordering guarantee between them, so a
// handful of already-fresh post-restart status events can race ahead and
// land in the table before this handler's TRUNCATE ran - which a blind
// TRUNCATE would then wipe right back out, leaving those hosts/services
// without a row until their next check. Deleting only rows whose
// status_update_time predates the restart's own cutoff (see
// newCoreRestartHandler) keeps genuinely stale pre-restart rows gone while
// letting any row that raced ahead - its status_update_time is at or after
// the cutoff - survive.
const (
	hoststatusStaleDeleteQuery    = "DELETE FROM statusengine_hoststatus WHERE status_update_time < ?"
	servicestatusStaleDeleteQuery = "DELETE FROM statusengine_servicestatus WHERE status_update_time < ?"
)

// hoststatusOpenITCockpitDeleteQuery/servicestatusOpenITCockpitDeleteQuery
// are used instead of a plain TRUNCATE when ENABLE_OPENITCOCKPIT_TWEAKS is
// set: openITCockpit keeps its own hosts/services tables as the source of
// truth for which objects still exist, so only rows for objects that are
// gone or disabled there are removed - status for objects that still exist
// survives the restart instead of needing to be rebuilt from scratch.
const (
	hoststatusOpenITCockpitDeleteQuery = "DELETE FROM statusengine_hoststatus WHERE NOT EXISTS " +
		"(SELECT hosts.uuid FROM hosts WHERE statusengine_hoststatus.hostname = hosts.uuid AND hosts.disabled=0)"
	servicestatusOpenITCockpitDeleteQuery = "DELETE FROM statusengine_servicestatus WHERE NOT EXISTS " +
		"(SELECT services.uuid FROM services WHERE statusengine_servicestatus.service_description = services.uuid AND services.disabled=0)"
)

// tableCleaner is implemented by *db.BulkInserter[T] for any T: the subset
// newCoreRestartHandler needs to safely run the hoststatus/servicestatus
// cleanup query against the same table a BulkInserter writes to, without
// racing an in-flight bulk INSERT/UPSERT and risking a lock-wait/deadlock
// against MySQL (CLAUDE.md: pause the BulkInserter during this cleanup).
type tableCleaner interface {
	WithPaused(ctx context.Context, fn func(ctx context.Context) error) error
}

// newCoreRestartHandler reacts only to object_type 102 (a monitoring core
// restart): it logs, publishes the event, then clears out stale
// hoststatus/servicestatus rows - via the status_update_time-based DELETE
// above, or via the openITCockpit-aware existence-based DELETE queries when
// enableOpenITCockpitTweaks is set (that variant needs no cutoff: it never
// removes a row for a host/service that still exists, fresh or not, so it
// isn't exposed to the race hoststatusStaleDeleteQuery's cutoff guards
// against) - pausing each BulkInserter for the duration of its own table's
// cleanup query.
func newCoreRestartHandler(hub *websocket.Hub, topic string, sqlDB *sql.DB, hostStatus, serviceStatus tableCleaner, enableOpenITCockpitTweaks bool) Handler {
	hostQuery, serviceQuery := hoststatusStaleDeleteQuery, servicestatusStaleDeleteQuery
	if enableOpenITCockpitTweaks {
		hostQuery, serviceQuery = hoststatusOpenITCockpitDeleteQuery, servicestatusOpenITCockpitDeleteQuery
	}

	return func(ctx context.Context, payload []byte) error {
		events, err := decodeCoreRestart(payload)
		if err != nil {
			return decodeError(topic, err)
		}

		for _, ev := range events {
			if ev.ObjectType != coreRestartObjectType {
				continue
			}

			// cutoff is captured now, before WithPaused's own Flush-then-pause
			// sequence runs, so it reflects the moment the restart was
			// noticed rather than the (slightly later) moment the DELETE
			// actually executes - see hoststatusStaleDeleteQuery's doc
			// comment for why that matters. ev.Timestamp lets a future
			// Naemon that stamps this event override the wall clock
			// explicitly; today it's always 0 (unset), so the wall clock is
			// what's actually used.
			cutoff := ev.Timestamp
			if cutoff <= 0 {
				cutoff = time.Now().Unix()
			}
			var cleanupArgs []any
			if !enableOpenITCockpitTweaks {
				cleanupArgs = []any{cutoff}
			}

			slog.Info("Catch monitoring restart. Trigger callbacks...")
			publish(hub, topic, ev)

			if err := hostStatus.WithPaused(ctx, func(ctx context.Context) error {
				_, err := sqlDB.ExecContext(ctx, hostQuery, cleanupArgs...)
				return err
			}); err != nil {
				return fmt.Errorf("queue: %s hoststatus cleanup: %w", topic, err)
			}

			if err := serviceStatus.WithPaused(ctx, func(ctx context.Context) error {
				_, err := sqlDB.ExecContext(ctx, serviceQuery, cleanupArgs...)
				return err
			}); err != nil {
				return fmt.Errorf("queue: %s servicestatus cleanup: %w", topic, err)
			}
		}
		return nil
	}
}

// downtimeMetricsTable renders the concrete table name an executed
// DowntimeAction wrote to, for the "table" label on
// metrics.DBEventsWrittenTotal - mirrors db.downtimeTable's naming
// (statusengine_host_*/statusengine_service_* + scheduleddowntimes/
// downtimehistory) without needing that unexported helper itself.
func downtimeMetricsTable(action DowntimeAction) string {
	scope := "service"
	if action.Data.IsHostDowntime {
		scope = "host"
	}
	return downtimeTableName(scope, action.Table.String())
}

func downtimeTableName(scope, base string) string {
	return "statusengine_" + scope + "_" + base
}

// downtimeMetricsTables enumerates every table name downtimeMetricsTable
// can produce. The downtime path writes with a bare ExecContext instead of
// a BulkInserter, so its tables miss the metrics.InitTable call that
// db.NewBulkInserter makes for every other table - this is what gives them
// their zero-valued series too. Kept next to downtimeMetricsTable and
// built from the same helper so the two cannot spell a table differently;
// a test pins that they agree.
func downtimeMetricsTables() []string {
	bases := []string{ScheduledDowntimesTable.String(), DowntimeHistoryTable.String()}

	tables := make([]string, 0, 2*len(bases))
	for _, scope := range []string{"host", "service"} {
		for _, base := range bases {
			tables = append(tables, downtimeTableName(scope, base))
		}
	}
	return tables
}

// execDowntimeAction turns one DowntimeAction into its concrete (query,
// args) pair via the matching internal/db builder (Schritt 3), executes it,
// and reports the outcome through the same db-write instrumentation
// BulkInserter.flushBuffer uses for every other table - done by hand here
// since downtime writes deliberately bypass BulkInserter entirely (see
// .claude/specs/downtime_ablauf.txt section 6: a single downtime message
// can require an UPSERT, UPDATE or DELETE, not just an INSERT). Unlike
// BulkInserter's batch histograms (which track 100-item/250ms batching
// behaviour that plainly doesn't apply here, per downtime_ablauf.txt
// section 6), only DBEventsWrittenTotal/PipelineErrorsTotal apply to a
// single-row ExecContext like this one.
func execDowntimeAction(ctx context.Context, sqlDB *sql.DB, action DowntimeAction) error {
	row := db.DowntimeRow{
		IsHostDowntime:     action.Data.IsHostDowntime,
		HostName:           action.Data.HostName,
		ServiceDescription: action.Data.ServiceDescription,
		NodeName:           action.Data.NodeName,
		InternalDowntimeID: action.Data.InternalDowntimeID,
		EntryTime:          action.Data.EntryTime,
		EntryTimeUsec:      action.Data.EntryTimeUsec,
		AuthorName:         action.Data.AuthorName,
		CommentData:        action.Data.CommentData,
		TriggeredByID:      action.Data.TriggeredByID,
		IsFixed:            action.Data.IsFixed,
		Duration:           action.Data.Duration,
		ScheduledStartTime: action.Data.ScheduledStartTime,
		ScheduledEndTime:   action.Data.ScheduledEndTime,
		WasStarted:         action.Data.WasStarted,
		ActualStartTime:    action.Data.ActualStartTime,
		ActualEndTime:      action.Data.ActualEndTime,
		WasCancelled:       action.Data.WasCancelled,
	}

	var query string
	var args []any
	switch {
	case action.Table == ScheduledDowntimesTable && action.Action == DowntimeActionUpsert:
		query, args = db.UpsertScheduledDowntimeQuery(row)
	case action.Table == ScheduledDowntimesTable && action.Action == DowntimeActionDelete:
		query, args = db.DeleteScheduledDowntimeQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionUpsert:
		query, args = db.UpsertDowntimeHistoryQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionUpdateStarted:
		query, args = db.UpdateDowntimeHistoryStartedQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionUpdateStopped:
		query, args = db.UpdateDowntimeHistoryStoppedQuery(row)
	case action.Table == DowntimeHistoryTable && action.Action == DowntimeActionDelete:
		query, args = db.DeleteDowntimeHistoryQuery(row)
	default:
		// Unreachable given DetermineDowntimeActions' own switch, but kept
		// explicit rather than silently doing nothing if a future action
		// combination is added there without a matching case here.
		return fmt.Errorf("unhandled downtime action %s on %s", action.Action, action.Table)
	}

	table := downtimeMetricsTable(action)
	start := time.Now()
	_, err := sqlDB.ExecContext(ctx, query, args...)
	duration := time.Since(start)

	if err != nil {
		metrics.PipelineErrorsTotal.WithLabelValues(metrics.ComponentMySQL).Inc()
		slog.Error("queue: downtime write failed", "table", table, "action", action.Action, "duration", duration, "error", err)
		return fmt.Errorf("%s %s: %w", action.Action, table, err)
	}

	metrics.DBEventsWrittenTotal.WithLabelValues(table).Add(1)
	slog.Info("queue: downtime write", "table", table, "action", action.Action, "duration", duration)
	return nil
}

// newDowntimeHandler builds the Handler for the statusngin_downtimes queue.
// Like newCoreRestartHandler, it bypasses BulkInserter entirely and writes
// straight to sqlDB: DetermineDowntimeActions (downtime_processor.go)
// decides exactly which INSERT/UPSERT, UPDATE or DELETE calls a given
// message requires across statusengine_{host,service}_scheduleddowntimes
// and statusengine_{host,service}_downtimehistory, and execDowntimeAction
// runs each in the order returned - see .claude/specs/downtime_ablauf.txt
// for why a single BulkInserter-based table can't express this.
//
// The full decoded message (envelope Type/Attr included, not just the
// downtime payload) is published to hub for every event, same as every
// other Handler in this file - so WebSocket subscribers can tell an ADD
// from a START/STOP/DELETE, which the payload alone doesn't carry.
func newDowntimeHandler(hub *websocket.Hub, topic string, sqlDB *sql.DB, nodeName string) Handler {
	return func(ctx context.Context, payload []byte) error {
		msg, err := decodeDowntimeMessage(payload)
		if err != nil {
			return decodeError(topic, err)
		}

		publish(hub, topic, msg)

		for _, action := range DetermineDowntimeActions(msg, nodeName) {
			if err := execDowntimeAction(ctx, sqlDB, action); err != nil {
				return fmt.Errorf("queue: %s: %w", topic, err)
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
			return decodeError(topic, err)
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
			return decodeError(topic, err)
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
// core_restart has no matching table of its own; on object_type 102 it
// instead clears out statusengine_hoststatus/statusengine_servicestatus -
// see newCoreRestartHandler and the enableOpenITCockpitTweaks parameter.
// downtimes also bypasses BulkInserter, for the opposite reason: a single
// message can require an INSERT/UPSERT, UPDATE or DELETE across two tables
// depending on its type - see newDowntimeHandler, downtime_processor.go's
// DetermineDowntimeActions and .claude/specs/downtime_ablauf.txt.
// service_perfdata gets its own handler (see processor.go), routing each
// parsed metric to MySQL, Graphite, or both per perfdataRoute (CLAUDE.md
// rule 5).
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
// graphitePrefix is prepended to every Graphite path NewPerfdataHandler
// builds (worker-wide "graphite_prefix" config option, default
// "statusengine") - see processor.go's graphiteMetricPath.
// enableOpenITCockpitTweaks selects which query newCoreRestartHandler uses
// to clear hoststatus/servicestatus on a core restart (worker-wide
// "ENABLE_OPENITCOCKPIT_TWEAKS" config option, default false).
// statusMaxAge is how old a hoststatus/servicestatus event may be before it
// is discarded instead of processed ("status_max_age" config option,
// default 5m, zero disables) - see NewStaleDroppingHandler, and note it
// applies to those two queues only.
// newRedeliverySafeInserter builds a BulkInserter for a table with a natural
// PRIMARY KEY, whose flush survives the same job being delivered twice.
//
// Queue delivery is at-least-once and cannot be made otherwise. A worker that
// is killed, OOM-killed or loses power between finishing a job and its
// acknowledgement reaching the broker will be handed that job again on
// restart, and its rows are already in MySQL. Without this clause the
// redelivered rows collide on the PRIMARY KEY, MySQL aborts the *entire*
// multi-row INSERT with Error 1062, and flushBuffer drops the batch - taking
// with it every fresh row that happened to share that batch, since batches
// are cut at the batch size regardless of job boundaries. Measured at 1.1% of
// all events lost on a single SIGTERM under load (CLAUDE.md rule 6); see
// cmd/losstest, which reproduces it.
//
// What the clause is for, since this is easy to misread: the goal is an
// INSERT that adds the rows MySQL does not have yet and does nothing at all
// for the ones it already has. MySQL has no such statement, but
// "ON DUPLICATE KEY UPDATE" gets there - provided the update it performs is
// worthless.
//
// The column named below is therefore NOT a key specification. MySQL decides
// what counts as a duplicate entirely on its own, matching the *complete*
// index - for statusengine_servicechecks that is all three of
// (service_description, start_time, start_time_usec). The list only says what
// to write once a duplicate has been found, and MySQL rejects an empty one as
// a syntax error, so something has to be named.
//
// Naming a PRIMARY KEY column is what makes that something a genuine no-op: a
// collision means every key column already matches, so "col = VALUES(col)"
// writes back the value the row already holds. MySQL sees no change, creates
// no new row version, touches no index and reports zero affected rows. Any of
// the key's columns would do equally well - listing all three would be just as
// correct and three times as long for an identical result. The FIRST one is a
// convention, chosen because it can be checked mechanically against the schema
// (TestRedeliverySafePKColumnsMatchSchema).
//
// Note the contrast with hoststatus/servicestatus a few lines up in NewRouter:
// their updateColumns list the entire payload, because there a collision is
// supposed to overwrite - they receive repeated snapshots of the same logical
// row. Same mechanism, opposite intent.
//
// INSERT IGNORE would express the same intent more briefly and is deliberately
// not used - it downgrades *every* error to a warning, including truncation and
// NOT NULL violations, which would turn real data problems invisible.
func newRedeliverySafeInserter[T any](sqlDB *sql.DB, table string, columns []string, toRow db.RowFunc[T], opts ...db.Option) *db.BulkInserter[T] {
	pkColumn, ok := redeliverySafePKColumn[table]
	if !ok {
		// A table added above without an entry below would silently go back
		// to plain INSERT and lose a batch on the next redelivery. Fail at
		// construction instead, where every NewRouter test catches it.
		panic("queue: no redelivery-safe PRIMARY KEY column declared for " + table)
	}
	return db.NewUpsertBulkInserter(sqlDB, table, columns, []string{pkColumn}, toRow, opts...)
}

// redeliverySafePKColumn maps every table written through
// newRedeliverySafeInserter to the first column of its PRIMARY KEY. Kept as
// one table rather than spelled out at each call site so the value cannot
// drift from the schema unnoticed - TestRedeliverySafePKColumnsMatchSchema
// checks every entry against .claude/specs/mysql_schema.sql.
//
// statusengine_logentries and statusengine_perfdata are absent on purpose:
// neither has a PRIMARY KEY a redelivery could collide on. See the comment
// at their constructors in NewRouter.
var redeliverySafePKColumn = map[string]string{
	"statusengine_hostchecks":                "hostname",
	"statusengine_servicechecks":             "service_description",
	"statusengine_host_statehistory":         "hostname",
	"statusengine_service_statehistory":      "service_description",
	"statusengine_host_acknowledgements":     "hostname",
	"statusengine_service_acknowledgements":  "service_description",
	"statusengine_host_notifications":        "hostname",
	"statusengine_service_notifications":     "service_description",
	"statusengine_host_notifications_log":    "hostname",
	"statusengine_service_notifications_log": "hostname",
}

func NewRouter(sqlDB *sql.DB, hub *websocket.Hub, gc *graphite.Client, perfdataRoute PerfdataRoute, graphitePrefix, nodeName string, enableOpenITCockpitTweaks bool, statusMaxAge time.Duration, mysqlBatchSize int) (Router, []Runner) {
	// Every table shares one batch size, built once here and spread into
	// each constructor below, so a table added later cannot quietly keep
	// the default. db.WithMaxBatchSize clamps; cmd/app is what rejects an
	// out-of-range value outright.
	batch := []db.Option{db.WithMaxBatchSize(mysqlBatchSize)}

	hostStatus := db.NewUpsertBulkInserter(sqlDB, "statusengine_hoststatus",
		hostStatusColumns, hostStatusUpdateColumns, newHostStatusRow(nodeName), batch...)

	serviceStatus := db.NewUpsertBulkInserter(sqlDB, "statusengine_servicestatus",
		serviceStatusColumns, serviceStatusUpdateColumns, newServiceStatusRow(nodeName), batch...)

	hostChecks := newRedeliverySafeInserter(sqlDB, "statusengine_hostchecks",
		[]string{"hostname", "start_time", "start_time_usec", "state", "is_hardstate", "end_time", "output", "long_output",
			"timeout", "early_timeout", "latency", "execution_time", "perfdata", "command",
			"current_check_attempt", "max_check_attempts"},
		hostCheckRow, batch...)

	serviceChecks := newRedeliverySafeInserter(sqlDB, "statusengine_servicechecks",
		[]string{"service_description", "start_time", "start_time_usec", "hostname", "state", "is_hardstate", "end_time", "output",
			"long_output", "timeout", "early_timeout", "latency", "execution_time", "perfdata", "command",
			"current_check_attempt", "max_check_attempts"},
		serviceCheckRow, batch...)

	// logEntries and perfdata below are deliberately NOT redelivery-safe, and
	// cannot be made so from here: statusengine_logentries is keyed on an
	// AUTO_INCREMENT id and statusengine_perfdata has no PRIMARY KEY at all,
	// so a redelivered job collides with nothing and simply inserts its rows
	// a second time. That is a silent duplicate rather than a dropped batch -
	// no error, nothing in the log - and it is the accepted trade-off here:
	// both are retention-managed history, and fixing it would need a UNIQUE
	// index, i.e. a schema change.
	logEntries := db.NewBulkInserter(sqlDB, "statusengine_logentries",
		[]string{"entry_time", "logentry_type", "logentry_data", "node_name"},
		newLogEntryRow(nodeName), batch...)

	hostStateHistory := newRedeliverySafeInserter(sqlDB, "statusengine_host_statehistory",
		[]string{"hostname", "state_time", "state_time_usec", "state_change", "state", "is_hardstate",
			"current_check_attempt", "max_check_attempts", "last_state", "last_hard_state", "output", "long_output"},
		hostStateHistoryRow, batch...)

	serviceStateHistory := newRedeliverySafeInserter(sqlDB, "statusengine_service_statehistory",
		[]string{"service_description", "state_time", "state_time_usec", "hostname", "state_change", "state",
			"is_hardstate", "current_check_attempt", "max_check_attempts", "last_state", "last_hard_state",
			"output", "long_output"},
		serviceStateHistoryRow, batch...)

	hostAcks := newRedeliverySafeInserter(sqlDB, "statusengine_host_acknowledgements",
		[]string{"hostname", "entry_time", "entry_time_usec", "state", "author_name", "comment_data",
			"acknowledgement_type", "is_sticky", "persistent_comment", "notify_contacts"},
		hostAcknowledgementRow, batch...)

	serviceAcks := newRedeliverySafeInserter(sqlDB, "statusengine_service_acknowledgements",
		[]string{"service_description", "entry_time", "entry_time_usec", "hostname", "state", "author_name", "comment_data",
			"acknowledgement_type", "is_sticky", "persistent_comment", "notify_contacts"},
		serviceAcknowledgementRow, batch...)

	// See the comment above why perfdat uses db.NewBulkInserter
	perfdata := db.NewBulkInserter(sqlDB, "statusengine_perfdata",
		[]string{"hostname", "service_description", "label", "timestamp", "timestamp_unix", "value", "unit"},
		perfdataRow, batch...)

	hostNotifications := newRedeliverySafeInserter(sqlDB, "statusengine_host_notifications",
		[]string{"hostname", "start_time", "start_time_usec", "contact_name", "command_name", "command_args",
			"state", "end_time", "reason_type", "output", "ack_author", "ack_data"},
		hostNotificationRow, batch...)

	serviceNotifications := newRedeliverySafeInserter(sqlDB, "statusengine_service_notifications",
		[]string{"service_description", "start_time", "start_time_usec", "hostname", "contact_name",
			"command_name", "command_args", "state", "end_time", "reason_type", "output", "ack_author", "ack_data"},
		serviceNotificationRow, batch...)

	hostNotificationsLog := newRedeliverySafeInserter(sqlDB, "statusengine_host_notifications_log",
		[]string{"hostname", "start_time", "start_time_usec", "end_time", "state", "reason_type",
			"is_escalated", "contacts_notified_count", "output", "ack_author", "ack_data"},
		hostNotificationLogRow, batch...)

	serviceNotificationsLog := newRedeliverySafeInserter(sqlDB, "statusengine_service_notifications_log",
		[]string{"hostname", "service_description", "start_time", "start_time_usec", "end_time", "state",
			"reason_type", "is_escalated", "contacts_notified_count", "output", "ack_author", "ack_data"},
		serviceNotificationLogRow, batch...)

	router := Router{
		QueueHostChecks:       NewHandler(hub, QueueHostChecks, hostChecks, decodeHostCheck),
		QueueServiceChecks:    NewHandler(hub, QueueServiceChecks, serviceChecks, decodeServiceCheck),
		QueueLogEntries:       NewHandler(hub, QueueLogEntries, logEntries, decodeLogEntry),
		QueueStateChanges:     newStateChangeHandler(hub, QueueStateChanges, hostStateHistory, serviceStateHistory),
		QueueAcknowledgements: newAcknowledgementHandler(hub, QueueAcknowledgements, hostAcks, serviceAcks),
		QueueServicePerfdata:  NewPerfdataHandler(hub, QueueServicePerfdata, perfdataRoute, perfdata, gc, graphitePrefix),
		// The only two queues that may discard on age - see
		// NewStaleDroppingHandler for why that is safe here and nowhere else.
		QueueHostStatus:                NewStaleDroppingHandler(hub, QueueHostStatus, hostStatus, decodeHostStatus, statusMaxAge),
		QueueServiceStatus:             NewStaleDroppingHandler(hub, QueueServiceStatus, serviceStatus, decodeServiceStatus, statusMaxAge),
		QueueContactNotificationMethod: newContactNotificationMethodHandler(hub, QueueContactNotificationMethod, hostNotifications, serviceNotifications),
		QueueNotifications:             newNotificationHandler(hub, QueueNotifications, hostNotificationsLog, serviceNotificationsLog),

		QueueDowntimes:   newDowntimeHandler(hub, QueueDowntimes, sqlDB, nodeName),
		QueueCoreRestart: newCoreRestartHandler(hub, QueueCoreRestart, sqlDB, hostStatus, serviceStatus, enableOpenITCockpitTweaks),
	}

	// Give every queue and every downtime table its zero-valued series
	// before the first message arrives. The BulkInserter tables above got
	// theirs from db.NewBulkInserter; these two sets have no constructor
	// to hang it on, and driving the queue names off the router itself
	// means a queue added to the map above is covered without a second
	// list to remember.
	for queueName := range router {
		metrics.InitQueue(queueName)
	}
	for _, queueName := range []string{QueueHostStatus, QueueServiceStatus} {
		metrics.InitStaleDiscards(queueName)
	}
	for _, table := range downtimeMetricsTables() {
		metrics.InitTable(table)
	}

	runners := []Runner{
		hostStatus, serviceStatus,
		hostChecks, serviceChecks, logEntries,
		hostStateHistory, serviceStateHistory,
		hostAcks, serviceAcks,
		hostNotifications, serviceNotifications,
		hostNotificationsLog, serviceNotificationsLog,
		perfdata, gc,
	}

	return router, runners
}

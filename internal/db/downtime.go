package db

import "strings"

// This file implements Schritt 3 of .claude/specs/downtime_ablauf.txt: the
// concrete (query, args) pairs for the six reachable (table, action)
// combinations internal/queue.DetermineDowntimeActions can decide on. It is
// deliberately plain query building, not a BulkInserter - see
// downtime_ablauf.txt section 6 for why downtimes need INSERT/UPSERT,
// UPDATE and DELETE across two tables from one message, which doesn't fit
// BulkInserter's "one RowFunc, one INSERT" model.
//
// DowntimeRow mirrors internal/queue's DowntimeRowData field-for-field
// rather than importing it: internal/queue already imports internal/db
// (for BulkInserter), so the reverse import would create a cycle.
// internal/queue's downtime handler (registry.go) converts one into the
// other before calling any function below.
type DowntimeRow struct {
	IsHostDowntime     bool
	HostName           string
	ServiceDescription string
	NodeName           string
	InternalDowntimeID int
	EntryTime          int64
	EntryTimeUsec      int
	AuthorName         string
	CommentData        string
	TriggeredByID      int
	IsFixed            bool
	Duration           int
	ScheduledStartTime int64
	ScheduledEndTime   int64
	WasStarted         bool
	ActualStartTime    int64
	ActualEndTime      int64
	WasCancelled       bool
}

// downtimeTable returns the concrete statusengine_host_* or
// statusengine_service_* table name for base ("scheduleddowntimes" or
// "downtimehistory") - see downtime_ablauf.txt section 3/4.
func downtimeTable(base string, isHost bool) string {
	if isHost {
		return "statusengine_host_" + base
	}
	return "statusengine_service_" + base
}

// downtimeIdentityColumns/downtimeIdentityArgs are the one part of a
// downtime row that differs in shape between the host and service table
// variants: service tables key on (hostname, service_description), host
// tables on hostname alone. Every query below starts with these before its
// operation-specific columns.
func downtimeIdentityColumns(isHost bool) []string {
	if isHost {
		return []string{"hostname"}
	}
	return []string{"hostname", "service_description"}
}

func downtimeIdentityArgs(row DowntimeRow) []any {
	if row.IsHostDowntime {
		return []any{row.HostName}
	}
	return []any{row.HostName, row.ServiceDescription}
}

// downtimePrimaryKeyWhere renders the "hostname=? [AND
// service_description=?] AND node_name=? AND scheduled_start_time=? AND
// internal_downtime_id=?" clause shared by every UPDATE/DELETE below - the
// full primary key of both tables (mysql_schema.sql).
func downtimePrimaryKeyWhere(row DowntimeRow) (string, []any) {
	cols := append(downtimeIdentityColumns(row.IsHostDowntime), "node_name", "scheduled_start_time", "internal_downtime_id")
	args := append(downtimeIdentityArgs(row), row.NodeName, row.ScheduledStartTime, row.InternalDowntimeID)

	clauses := make([]string, len(cols))
	for i, c := range cols {
		clauses[i] = c + "=?"
	}
	return strings.Join(clauses, " AND "), args
}

// buildDowntimeUpsert renders "INSERT INTO table (cols...) VALUES (?,...)
// ON DUPLICATE KEY UPDATE updateCols[i]=VALUES(updateCols[i]), ...",
// mirroring BulkInserter.buildInsert's style in db.go.
func buildDowntimeUpsert(table string, cols, updateCols []string) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(") VALUES (")
	b.WriteString(strings.TrimSuffix(strings.Repeat("?,", len(cols)), ","))
	b.WriteString(") ON DUPLICATE KEY UPDATE ")
	for i, c := range updateCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c)
		b.WriteString("=VALUES(")
		b.WriteString(c)
		b.WriteString(")")
	}
	return b.String()
}

// UpsertScheduledDowntimeQuery builds the statusengine_host_scheduleddowntimes
// / statusengine_service_scheduleddowntimes upsert used for the ADD and
// START Envelope.Type actions (downtime_ablauf.txt section 5b). Unlike
// downtimehistory, this table has no entry_time_usec/actual_end_time/
// was_cancelled columns (mysql_schema.sql).
func UpsertScheduledDowntimeQuery(row DowntimeRow) (string, []any) {
	table := downtimeTable("scheduleddowntimes", row.IsHostDowntime)

	cols := append(downtimeIdentityColumns(row.IsHostDowntime),
		"internal_downtime_id", "scheduled_start_time", "node_name",
		"entry_time", "author_name", "comment_data", "triggered_by_id",
		"is_fixed", "duration", "scheduled_end_time", "was_started", "actual_start_time")
	args := append(downtimeIdentityArgs(row),
		row.InternalDowntimeID, row.ScheduledStartTime, row.NodeName,
		row.EntryTime, row.AuthorName, row.CommentData, row.TriggeredByID,
		row.IsFixed, row.Duration, row.ScheduledEndTime, row.WasStarted, row.ActualStartTime)

	updateCols := []string{
		"entry_time", "author_name", "comment_data", "triggered_by_id",
		"is_fixed", "duration", "scheduled_end_time", "was_started", "actual_start_time",
	}

	return buildDowntimeUpsert(table, cols, updateCols), args
}

// DeleteScheduledDowntimeQuery builds the DELETE used for the STOP and
// DELETE Envelope.Type actions (downtime_ablauf.txt section 5): the
// downtime is over, so its row leaves the "currently active/scheduled"
// table.
func DeleteScheduledDowntimeQuery(row DowntimeRow) (string, []any) {
	table := downtimeTable("scheduleddowntimes", row.IsHostDowntime)
	where, args := downtimePrimaryKeyWhere(row)
	return "DELETE FROM " + table + " WHERE " + where, args
}

// UpsertDowntimeHistoryQuery builds the statusengine_host_downtimehistory /
// statusengine_service_downtimehistory upsert used for the ADD
// Envelope.Type action (downtime_ablauf.txt section 5a) - "freshly created,
// nothing has happened yet" (was_started/actual_start_time/actual_end_time
// all zero, was_cancelled false).
func UpsertDowntimeHistoryQuery(row DowntimeRow) (string, []any) {
	table := downtimeTable("downtimehistory", row.IsHostDowntime)

	cols := append(downtimeIdentityColumns(row.IsHostDowntime),
		"internal_downtime_id", "scheduled_start_time", "node_name",
		"entry_time", "entry_time_usec", "author_name", "comment_data", "triggered_by_id",
		"is_fixed", "duration", "scheduled_end_time",
		"was_started", "actual_start_time", "actual_end_time", "was_cancelled")
	args := append(downtimeIdentityArgs(row),
		row.InternalDowntimeID, row.ScheduledStartTime, row.NodeName,
		row.EntryTime, row.EntryTimeUsec, row.AuthorName, row.CommentData, row.TriggeredByID,
		row.IsFixed, row.Duration, row.ScheduledEndTime,
		row.WasStarted, row.ActualStartTime, row.ActualEndTime, row.WasCancelled)

	updateCols := []string{
		"entry_time", "entry_time_usec", "author_name", "comment_data", "triggered_by_id",
		"is_fixed", "duration", "scheduled_end_time",
		"was_started", "actual_start_time", "actual_end_time", "was_cancelled",
	}

	return buildDowntimeUpsert(table, cols, updateCols), args
}

// UpdateDowntimeHistoryStartedQuery builds the UPDATE used for the START
// Envelope.Type action: the downtime has begun, record when.
func UpdateDowntimeHistoryStartedQuery(row DowntimeRow) (string, []any) {
	table := downtimeTable("downtimehistory", row.IsHostDowntime)
	where, whereArgs := downtimePrimaryKeyWhere(row)

	query := "UPDATE " + table + " SET was_started=?, actual_start_time=? WHERE " + where
	args := append([]any{row.WasStarted, row.ActualStartTime}, whereArgs...)
	return query, args
}

// UpdateDowntimeHistoryStoppedQuery builds the UPDATE used for the STOP
// Envelope.Type action: the downtime has ended, record when and whether it
// was cancelled early rather than running to its scheduled end_time.
func UpdateDowntimeHistoryStoppedQuery(row DowntimeRow) (string, []any) {
	table := downtimeTable("downtimehistory", row.IsHostDowntime)
	where, whereArgs := downtimePrimaryKeyWhere(row)

	query := "UPDATE " + table + " SET actual_end_time=?, was_cancelled=? WHERE " + where
	args := append([]any{row.ActualEndTime, row.WasCancelled}, whereArgs...)
	return query, args
}

// DeleteDowntimeHistoryQuery builds the DELETE used only for the DELETE
// Envelope.Type action's "wasNeverStarted" case (downtime_ablauf.txt
// section 5): a downtime removed before its scheduled start_time was ever
// reached never had any effect, so its ADD-written history row is purged
// again instead of lingering as "never started, never ended".
func DeleteDowntimeHistoryQuery(row DowntimeRow) (string, []any) {
	table := downtimeTable("downtimehistory", row.IsHostDowntime)
	where, args := downtimePrimaryKeyWhere(row)
	return "DELETE FROM " + table + " WHERE " + where, args
}

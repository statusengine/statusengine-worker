package queue

import "statusengine-worker/internal/types"

// This file implements Schritt 2 of .claude/specs/downtime_ablauf.txt: a
// pure, database-free function that turns one decoded downtime message into
// the exact sequence of SQL operations it requires. No *sql.DB, no
// io - DetermineDowntimeActions only computes; step 4 (a handler analogous
// to newCoreRestartHandler) is responsible for actually executing the
// returned actions against sqlDB.ExecContext, in order.
//
// Reconstructed directly from the legacy PHP worker's
// MiscChild::handleDowntime() (see downtime_ablauf.txt's source list) -
// re-verified line by line against that source while writing this file,
// which surfaced two corrections against downtime_ablauf.txt itself:
//
//  1. EventTypeDowntimeLoad (a downtime restored from retention.dat on a
//     monitoring core restart) is a NO-OP on both tables, not "handled like
//     ADD". The legacy code's own comment is explicit about this:
//     "Filter delete and load events" gates the downtimehistory write, and
//     the same filter (implicitly, via the surrounding if/else) also skips
//     the scheduleddowntimes write. This makes sense once you consider that
//     a LOAD only ever fires for a downtime that was already ADDed - and
//     therefore already persisted - before the core restarted; there is
//     nothing new to write.
//  2. EventTypeDowntimeDelete does NOT unconditionally update
//     downtimehistory's actual_end_time/was_cancelled. The legacy code's
//     downtimehistory saveDowntime() call is only reached for ADD/START/STOP
//     (DELETE and LOAD are both filtered out before that call) - so on a
//     plain DELETE (started downtime being removed after an earlier STOP
//     already updated actual_end_time), downtimehistory is left untouched.
//     DELETE only ever touches downtimehistory via the separate
//     "never started" deleteDowntime() call below.
//
// The corrected, verified matrix (supersedes downtime_ablauf.txt section 7):
//
//	Type    scheduleddowntimes          downtimehistory
//	------  ---------------------------  -----------------------------------
//	ADD     UPSERT (was_started=0)       UPSERT (was_started=0, actual_*=0)
//	LOAD    NONE                         NONE
//	START   UPSERT (was_started=1,       UPDATE (was_started=1,
//	        actual_start_time=ts)        actual_start_time=ts)
//	STOP    DELETE                       UPDATE (actual_end_time=ts,
//	                                     was_cancelled=attr==CANCELLED)
//	DELETE  DELETE                       NONE, unless start_time>ts
//	                                     ("never started"): DELETE

// DowntimeTargetTable identifies which of the two per-object-type table
// pairs a DowntimeAction applies to. Which concrete table (host_* vs
// service_*) that resolves to is determined separately, from
// DowntimeRowData.IsHostDowntime - kept out of this type so the decision
// engine stays independent of any table-name string.
type DowntimeTargetTable int

const (
	ScheduledDowntimesTable DowntimeTargetTable = iota
	DowntimeHistoryTable
)

func (t DowntimeTargetTable) String() string {
	if t == DowntimeHistoryTable {
		return "downtimehistory"
	}
	return "scheduleddowntimes"
}

// DowntimeActionType is the SQL operation a DowntimeAction represents.
type DowntimeActionType int

const (
	DowntimeActionUpsert DowntimeActionType = iota
	DowntimeActionUpdate
	DowntimeActionDelete
)

func (a DowntimeActionType) String() string {
	switch a {
	case DowntimeActionUpsert:
		return "UPSERT"
	case DowntimeActionUpdate:
		return "UPDATE"
	case DowntimeActionDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// DowntimeRowData carries every column value a downtime action might need to
// build its query, pre-resolved from the incoming message plus the
// caller-supplied node name. Which fields are actually meaningful depends on
// the owning DowntimeAction's Table/Action - e.g. a DowntimeActionDelete
// only needs the primary-key fields (HostName, ServiceDescription, NodeName,
// ScheduledStartTime, InternalDowntimeID); EntryTimeUsec is only ever
// written to DowntimeHistoryTable, since *_scheduleddowntimes has no such
// column (see mysql_schema.sql).
type DowntimeRowData struct {
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

// DowntimeAction is a single SQL operation DetermineDowntimeActions has
// decided must run, against exactly one table.
type DowntimeAction struct {
	Table  DowntimeTargetTable
	Action DowntimeActionType
	Data   DowntimeRowData
}

// DetermineDowntimeActions computes the exact, ordered sequence of SQL
// actions a single downtime message requires. It performs no I/O: given the
// same msg and nodeName, it always returns the same result. Order matters -
// callers must execute the returned actions in the order given, matching
// the legacy worker's own write order (downtimehistory before
// scheduleddowntimes on ADD/START/STOP; scheduleddowntimes before
// downtimehistory on DELETE).
//
// nodeName is not part of the message (it comes from worker configuration,
// like everywhere else in this package) but is required to populate the
// node_name column, part of every table's primary key.
func DetermineDowntimeActions(msg types.DowntimeMessage, nodeName string) []DowntimeAction {
	p := msg.Downtime

	base := DowntimeRowData{
		IsHostDowntime:     p.DowntimeType == types.DowntimeTypeHost,
		HostName:           p.HostName,
		ServiceDescription: p.ServiceDescription,
		NodeName:           nodeName,
		InternalDowntimeID: p.DowntimeID,
		EntryTime:          p.EntryTime,
		EntryTimeUsec:      msg.TimestampUsec,
		AuthorName:         p.AuthorName,
		CommentData:        p.CommentData,
		TriggeredByID:      p.TriggeredBy,
		IsFixed:            p.Fixed != 0,
		Duration:           p.Duration,
		ScheduledStartTime: p.StartTime,
		ScheduledEndTime:   p.EndTime,
	}

	switch msg.Type {
	case types.EventTypeDowntimeAdd:
		data := base
		data.WasStarted = false
		data.ActualStartTime = 0
		data.ActualEndTime = 0
		data.WasCancelled = false
		return []DowntimeAction{
			{Table: DowntimeHistoryTable, Action: DowntimeActionUpsert, Data: data},
			{Table: ScheduledDowntimesTable, Action: DowntimeActionUpsert, Data: data},
		}

	case types.EventTypeDowntimeLoad:
		// See the package-level doc comment above: the row was already
		// persisted by the original ADD before the core restarted, so
		// there is nothing to write here.
		return nil

	case types.EventTypeDowntimeStart:
		data := base
		data.WasStarted = true
		data.ActualStartTime = msg.Timestamp
		return []DowntimeAction{
			{Table: DowntimeHistoryTable, Action: DowntimeActionUpdate, Data: data},
			{Table: ScheduledDowntimesTable, Action: DowntimeActionUpsert, Data: data},
		}

	case types.EventTypeDowntimeStop:
		data := base
		data.ActualEndTime = msg.Timestamp
		data.WasCancelled = msg.Attr == types.DowntimeAttrStopCancelled
		return []DowntimeAction{
			{Table: DowntimeHistoryTable, Action: DowntimeActionUpdate, Data: data},
			{Table: ScheduledDowntimesTable, Action: DowntimeActionDelete, Data: data},
		}

	case types.EventTypeDowntimeDelete:
		data := base
		actions := []DowntimeAction{
			{Table: ScheduledDowntimesTable, Action: DowntimeActionDelete, Data: data},
		}
		if wasDowntimeNeverStarted(p.StartTime, msg.Timestamp) {
			actions = append(actions, DowntimeAction{Table: DowntimeHistoryTable, Action: DowntimeActionDelete, Data: data})
		}
		return actions

	default:
		// Unknown Envelope.Type on the downtimes queue - nothing defined to
		// do with it.
		return nil
	}
}

// wasDowntimeNeverStarted mirrors the legacy Downtime::wasDowntimeNeverStarted():
// true only for a DELETE event whose scheduled start_time was still in the
// future (or exactly at, per the strict ">") when the delete happened - i.e.
// the downtime was removed before it ever took effect, so it must be purged
// from downtimehistory too instead of lingering as a "never started, never
// ended" row. Only called from the EventTypeDowntimeDelete case above, since
// the underlying comparison is meaningless for any other type.
func wasDowntimeNeverStarted(scheduledStartTime, eventTimestamp int64) bool {
	return scheduledStartTime > eventTimestamp
}

package db

import (
	"reflect"
	"testing"
)

// sampleDowntimeRow gives every field a distinct value so a query builder
// that accidentally transposes two args (e.g. swaps ActualStartTime and
// ActualEndTime) shows up as a test failure instead of passing by
// coincidence.
func sampleDowntimeRow(isHost bool) DowntimeRow {
	row := DowntimeRow{
		IsHostDowntime:     isHost,
		HostName:           "web01",
		NodeName:           "statusengine",
		InternalDowntimeID: 42,
		EntryTime:          1000,
		EntryTimeUsec:      250,
		AuthorName:         "Daniel Z",
		CommentData:        "In maintenance",
		TriggeredByID:      7,
		IsFixed:            true,
		Duration:           500,
		ScheduledStartTime: 2000,
		ScheduledEndTime:   2500,
		WasStarted:         true,
		ActualStartTime:    2005,
		ActualEndTime:      2500,
		WasCancelled:       true,
	}
	if !isHost {
		row.ServiceDescription = "Swap Usage"
	}
	return row
}

func TestDowntimeQueryBuilders(t *testing.T) {
	tests := []struct {
		name      string
		build     func(DowntimeRow) (string, []any)
		isHost    bool
		wantQuery string
		wantArgs  []any
	}{
		{
			name:      "UpsertScheduledDowntimeQuery host",
			build:     UpsertScheduledDowntimeQuery,
			isHost:    true,
			wantQuery: "INSERT INTO statusengine_host_scheduleddowntimes (hostname, internal_downtime_id, scheduled_start_time, node_name, entry_time, author_name, comment_data, triggered_by_id, is_fixed, duration, scheduled_end_time, was_started, actual_start_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE entry_time=VALUES(entry_time), author_name=VALUES(author_name), comment_data=VALUES(comment_data), triggered_by_id=VALUES(triggered_by_id), is_fixed=VALUES(is_fixed), duration=VALUES(duration), scheduled_end_time=VALUES(scheduled_end_time), was_started=VALUES(was_started), actual_start_time=VALUES(actual_start_time)",
			wantArgs:  []any{"web01", 42, int64(2000), "statusengine", int64(1000), "Daniel Z", "In maintenance", 7, true, 500, int64(2500), true, int64(2005)},
		},
		{
			name:      "UpsertScheduledDowntimeQuery service",
			build:     UpsertScheduledDowntimeQuery,
			isHost:    false,
			wantQuery: "INSERT INTO statusengine_service_scheduleddowntimes (hostname, service_description, internal_downtime_id, scheduled_start_time, node_name, entry_time, author_name, comment_data, triggered_by_id, is_fixed, duration, scheduled_end_time, was_started, actual_start_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE entry_time=VALUES(entry_time), author_name=VALUES(author_name), comment_data=VALUES(comment_data), triggered_by_id=VALUES(triggered_by_id), is_fixed=VALUES(is_fixed), duration=VALUES(duration), scheduled_end_time=VALUES(scheduled_end_time), was_started=VALUES(was_started), actual_start_time=VALUES(actual_start_time)",
			wantArgs:  []any{"web01", "Swap Usage", 42, int64(2000), "statusengine", int64(1000), "Daniel Z", "In maintenance", 7, true, 500, int64(2500), true, int64(2005)},
		},
		{
			name:      "DeleteScheduledDowntimeQuery host",
			build:     DeleteScheduledDowntimeQuery,
			isHost:    true,
			wantQuery: "DELETE FROM statusengine_host_scheduleddowntimes WHERE hostname=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{"web01", "statusengine", int64(2000), 42},
		},
		{
			name:      "DeleteScheduledDowntimeQuery service",
			build:     DeleteScheduledDowntimeQuery,
			isHost:    false,
			wantQuery: "DELETE FROM statusengine_service_scheduleddowntimes WHERE hostname=? AND service_description=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{"web01", "Swap Usage", "statusengine", int64(2000), 42},
		},
		{
			name:      "UpsertDowntimeHistoryQuery host",
			build:     UpsertDowntimeHistoryQuery,
			isHost:    true,
			wantQuery: "INSERT INTO statusengine_host_downtimehistory (hostname, internal_downtime_id, scheduled_start_time, node_name, entry_time, entry_time_usec, author_name, comment_data, triggered_by_id, is_fixed, duration, scheduled_end_time, was_started, actual_start_time, actual_end_time, was_cancelled) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE entry_time=VALUES(entry_time), entry_time_usec=VALUES(entry_time_usec), author_name=VALUES(author_name), comment_data=VALUES(comment_data), triggered_by_id=VALUES(triggered_by_id), is_fixed=VALUES(is_fixed), duration=VALUES(duration), scheduled_end_time=VALUES(scheduled_end_time), was_started=VALUES(was_started), actual_start_time=VALUES(actual_start_time), actual_end_time=VALUES(actual_end_time), was_cancelled=VALUES(was_cancelled)",
			wantArgs:  []any{"web01", 42, int64(2000), "statusengine", int64(1000), 250, "Daniel Z", "In maintenance", 7, true, 500, int64(2500), true, int64(2005), int64(2500), true},
		},
		{
			name:      "UpsertDowntimeHistoryQuery service",
			build:     UpsertDowntimeHistoryQuery,
			isHost:    false,
			wantQuery: "INSERT INTO statusengine_service_downtimehistory (hostname, service_description, internal_downtime_id, scheduled_start_time, node_name, entry_time, entry_time_usec, author_name, comment_data, triggered_by_id, is_fixed, duration, scheduled_end_time, was_started, actual_start_time, actual_end_time, was_cancelled) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE entry_time=VALUES(entry_time), entry_time_usec=VALUES(entry_time_usec), author_name=VALUES(author_name), comment_data=VALUES(comment_data), triggered_by_id=VALUES(triggered_by_id), is_fixed=VALUES(is_fixed), duration=VALUES(duration), scheduled_end_time=VALUES(scheduled_end_time), was_started=VALUES(was_started), actual_start_time=VALUES(actual_start_time), actual_end_time=VALUES(actual_end_time), was_cancelled=VALUES(was_cancelled)",
			wantArgs:  []any{"web01", "Swap Usage", 42, int64(2000), "statusengine", int64(1000), 250, "Daniel Z", "In maintenance", 7, true, 500, int64(2500), true, int64(2005), int64(2500), true},
		},
		{
			name:      "UpdateDowntimeHistoryStartedQuery host",
			build:     UpdateDowntimeHistoryStartedQuery,
			isHost:    true,
			wantQuery: "UPDATE statusengine_host_downtimehistory SET was_started=?, actual_start_time=? WHERE hostname=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{true, int64(2005), "web01", "statusengine", int64(2000), 42},
		},
		{
			name:      "UpdateDowntimeHistoryStartedQuery service",
			build:     UpdateDowntimeHistoryStartedQuery,
			isHost:    false,
			wantQuery: "UPDATE statusengine_service_downtimehistory SET was_started=?, actual_start_time=? WHERE hostname=? AND service_description=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{true, int64(2005), "web01", "Swap Usage", "statusengine", int64(2000), 42},
		},
		{
			name:      "UpdateDowntimeHistoryStoppedQuery host",
			build:     UpdateDowntimeHistoryStoppedQuery,
			isHost:    true,
			wantQuery: "UPDATE statusengine_host_downtimehistory SET actual_end_time=?, was_cancelled=? WHERE hostname=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{int64(2500), true, "web01", "statusengine", int64(2000), 42},
		},
		{
			name:      "UpdateDowntimeHistoryStoppedQuery service",
			build:     UpdateDowntimeHistoryStoppedQuery,
			isHost:    false,
			wantQuery: "UPDATE statusengine_service_downtimehistory SET actual_end_time=?, was_cancelled=? WHERE hostname=? AND service_description=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{int64(2500), true, "web01", "Swap Usage", "statusengine", int64(2000), 42},
		},
		{
			name:      "DeleteDowntimeHistoryQuery host",
			build:     DeleteDowntimeHistoryQuery,
			isHost:    true,
			wantQuery: "DELETE FROM statusengine_host_downtimehistory WHERE hostname=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{"web01", "statusengine", int64(2000), 42},
		},
		{
			name:      "DeleteDowntimeHistoryQuery service",
			build:     DeleteDowntimeHistoryQuery,
			isHost:    false,
			wantQuery: "DELETE FROM statusengine_service_downtimehistory WHERE hostname=? AND service_description=? AND node_name=? AND scheduled_start_time=? AND internal_downtime_id=?",
			wantArgs:  []any{"web01", "Swap Usage", "statusengine", int64(2000), 42},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotQuery, gotArgs := tc.build(sampleDowntimeRow(tc.isHost))
			if gotQuery != tc.wantQuery {
				t.Errorf("query =\n  %s\nwant\n  %s", gotQuery, tc.wantQuery)
			}
			if !reflect.DeepEqual(gotArgs, tc.wantArgs) {
				t.Errorf("args =\n  %#v\nwant\n  %#v", gotArgs, tc.wantArgs)
			}
		})
	}
}

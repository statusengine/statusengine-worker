package queue

import (
	"reflect"
	"testing"

	"statusengine-worker/internal/types"
)

// TestDetermineDowntimeActions proves DetermineDowntimeActions reproduces
// the full per-Envelope.Type matrix from downtime_ablauf.txt section 7 (as
// corrected in downtime_processor.go's package doc comment) - one subtest
// per Envelope.Type/host-vs-service/attr combination from
// downtime_ablauf.txt section 8, plus the wasNeverStarted boundary.
func TestDetermineDowntimeActions(t *testing.T) {
	const nodeName = "statusengine"

	// hostPayload/servicePayload share every field except HostName/
	// ServiceDescription/DowntimeType, so each subtest only has to override
	// StartTime/EndTime/DowntimeID where the scenario actually needs
	// different values.
	hostPayload := types.DowntimePayload{
		HostName:     "web01",
		AuthorName:   "Daniel Z",
		CommentData:  "In maintenance",
		DowntimeType: types.DowntimeTypeHost,
		EntryTime:    1000,
		StartTime:    2000,
		EndTime:      2500,
		TriggeredBy:  0,
		DowntimeID:   42,
		Fixed:        1,
		Duration:     500,
	}
	servicePayload := types.DowntimePayload{
		HostName:           "web01",
		ServiceDescription: "Swap Usage",
		AuthorName:         "Daniel Z",
		CommentData:        "In maintenance",
		DowntimeType:       types.DowntimeTypeService,
		EntryTime:          1000,
		StartTime:          2000,
		EndTime:            2500,
		TriggeredBy:        0,
		DowntimeID:         42,
		Fixed:              1,
		Duration:           500,
	}

	baseRow := func(p types.DowntimePayload, isHost bool) DowntimeRowData {
		return DowntimeRowData{
			IsHostDowntime:     isHost,
			HostName:           p.HostName,
			ServiceDescription: p.ServiceDescription,
			NodeName:           nodeName,
			InternalDowntimeID: p.DowntimeID,
			EntryTime:          p.EntryTime,
			AuthorName:         p.AuthorName,
			CommentData:        p.CommentData,
			TriggeredByID:      p.TriggeredBy,
			IsFixed:            p.Fixed != 0,
			Duration:           p.Duration,
			ScheduledStartTime: p.StartTime,
			ScheduledEndTime:   p.EndTime,
		}
	}

	tests := []struct {
		name     string
		msg      types.DowntimeMessage
		wantActs []DowntimeAction
	}{
		{
			name: "ADD host",
			msg: types.DowntimeMessage{
				Envelope: types.Envelope{Type: types.EventTypeDowntimeAdd, Timestamp: 1000, TimestampUsec: 250},
				Downtime: hostPayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(hostPayload, true)
				row.EntryTimeUsec = 250 // only downtimehistory has this column, but it's fine to carry it unconditionally
				return []DowntimeAction{
					{Table: DowntimeHistoryTable, Action: DowntimeActionUpsert, Data: row},
					{Table: ScheduledDowntimesTable, Action: DowntimeActionUpsert, Data: row},
				}
			}(),
		},
		{
			name: "ADD service",
			msg: types.DowntimeMessage{
				Envelope: types.Envelope{Type: types.EventTypeDowntimeAdd, Timestamp: 1000, TimestampUsec: 250},
				Downtime: servicePayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(servicePayload, false)
				row.EntryTimeUsec = 250
				return []DowntimeAction{
					{Table: DowntimeHistoryTable, Action: DowntimeActionUpsert, Data: row},
					{Table: ScheduledDowntimesTable, Action: DowntimeActionUpsert, Data: row},
				}
			}(),
		},
		{
			name: "LOAD is a no-op on both tables",
			msg: types.DowntimeMessage{
				Envelope: types.Envelope{Type: types.EventTypeDowntimeLoad, Timestamp: 1500},
				Downtime: hostPayload,
			},
			wantActs: nil,
		},
		{
			name: "START host",
			msg: types.DowntimeMessage{
				Envelope: types.Envelope{Type: types.EventTypeDowntimeStart, Timestamp: 2005},
				Downtime: hostPayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(hostPayload, true)
				row.WasStarted = true
				row.ActualStartTime = 2005
				return []DowntimeAction{
					{Table: DowntimeHistoryTable, Action: DowntimeActionUpdateStarted, Data: row},
					{Table: ScheduledDowntimesTable, Action: DowntimeActionUpsert, Data: row},
				}
			}(),
		},
		{
			name: "START service",
			msg: types.DowntimeMessage{
				Envelope: types.Envelope{Type: types.EventTypeDowntimeStart, Timestamp: 2005},
				Downtime: servicePayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(servicePayload, false)
				row.WasStarted = true
				row.ActualStartTime = 2005
				return []DowntimeAction{
					{Table: DowntimeHistoryTable, Action: DowntimeActionUpdateStarted, Data: row},
					{Table: ScheduledDowntimesTable, Action: DowntimeActionUpsert, Data: row},
				}
			}(),
		},
		{
			name: "STOP host, normal (ran to scheduled end_time)",
			msg: types.DowntimeMessage{
				Envelope: types.Envelope{Type: types.EventTypeDowntimeStop, Attr: types.DowntimeAttrStopNormal, Timestamp: 2500},
				Downtime: hostPayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(hostPayload, true)
				row.ActualEndTime = 2500
				row.WasCancelled = false
				return []DowntimeAction{
					{Table: DowntimeHistoryTable, Action: DowntimeActionUpdateStopped, Data: row},
					{Table: ScheduledDowntimesTable, Action: DowntimeActionDelete, Data: row},
				}
			}(),
		},
		{
			name: "STOP service, cancelled by user before end_time",
			msg: types.DowntimeMessage{
				Envelope: types.Envelope{Type: types.EventTypeDowntimeStop, Attr: types.DowntimeAttrStopCancelled, Timestamp: 2200},
				Downtime: servicePayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(servicePayload, false)
				row.ActualEndTime = 2200
				row.WasCancelled = true
				return []DowntimeAction{
					{Table: DowntimeHistoryTable, Action: DowntimeActionUpdateStopped, Data: row},
					{Table: ScheduledDowntimesTable, Action: DowntimeActionDelete, Data: row},
				}
			}(),
		},
		{
			name: "DELETE host after it already started - downtimehistory untouched",
			msg: types.DowntimeMessage{
				// start_time (2000) < timestamp (3000): the downtime had
				// already started (and, in the normal flow, already been
				// STOPped) before this DELETE arrived.
				Envelope: types.Envelope{Type: types.EventTypeDowntimeDelete, Timestamp: 3000},
				Downtime: hostPayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(hostPayload, true)
				return []DowntimeAction{
					{Table: ScheduledDowntimesTable, Action: DowntimeActionDelete, Data: row},
				}
			}(),
		},
		{
			name: "DELETE service before it ever started (wasNeverStarted) - purged from history too",
			msg: types.DowntimeMessage{
				// start_time (2000) > timestamp (500): scheduled start was
				// never reached before the user deleted the downtime.
				Envelope: types.Envelope{Type: types.EventTypeDowntimeDelete, Timestamp: 500},
				Downtime: servicePayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(servicePayload, false)
				return []DowntimeAction{
					{Table: ScheduledDowntimesTable, Action: DowntimeActionDelete, Data: row},
					{Table: DowntimeHistoryTable, Action: DowntimeActionDelete, Data: row},
				}
			}(),
		},
		{
			name: "DELETE exactly at start_time is NOT wasNeverStarted (strict greater-than)",
			msg: types.DowntimeMessage{
				// start_time (2000) == timestamp (2000): must NOT count as
				// never-started, matching the legacy ">" (not ">=") check.
				Envelope: types.Envelope{Type: types.EventTypeDowntimeDelete, Timestamp: 2000},
				Downtime: hostPayload,
			},
			wantActs: func() []DowntimeAction {
				row := baseRow(hostPayload, true)
				return []DowntimeAction{
					{Table: ScheduledDowntimesTable, Action: DowntimeActionDelete, Data: row},
				}
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DetermineDowntimeActions(tc.msg, nodeName)
			if !reflect.DeepEqual(got, tc.wantActs) {
				t.Fatalf("DetermineDowntimeActions() =\n  %+v\nwant\n  %+v", got, tc.wantActs)
			}
		})
	}
}

// TestDetermineDowntimeActionsIsPure proves the function has no hidden
// state or side effects: calling it twice with the same input must produce
// equal, independent results.
func TestDetermineDowntimeActionsIsPure(t *testing.T) {
	msg := types.DowntimeMessage{
		Envelope: types.Envelope{Type: types.EventTypeDowntimeAdd, Timestamp: 1000, TimestampUsec: 250},
		Downtime: types.DowntimePayload{HostName: "web01", DowntimeType: types.DowntimeTypeHost, DowntimeID: 1},
	}

	first := DetermineDowntimeActions(msg, "statusengine")
	second := DetermineDowntimeActions(msg, "statusengine")

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("DetermineDowntimeActions is not deterministic:\n  %+v\nvs\n  %+v", first, second)
	}
}

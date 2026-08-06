package main

import (
	"encoding/json"
	"testing"
)

func TestBumpUsecAssignsDistinctValuesPerEvent(t *testing.T) {
	raw := `{"messages":[
		{"timestamp":1,"timestamp_usec":42,"statechange":{"host_name":"a"}},
		{"timestamp":1,"timestamp_usec":42,"statechange":{"host_name":"b"}}
	]}`
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	counter := 0
	bumpUsec(v, &counter)

	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded struct {
		Messages []struct {
			TimestampUsec int `json:"timestamp_usec"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if len(decoded.Messages) != 2 {
		t.Fatalf("got %d messages, want 2", len(decoded.Messages))
	}
	if decoded.Messages[0].TimestampUsec == decoded.Messages[1].TimestampUsec {
		t.Fatalf("both events got the same timestamp_usec: %d", decoded.Messages[0].TimestampUsec)
	}
}

// TestWithUniqueTimestampsProducesDistinctUsecAcrossFixtureEvents proves
// the fix for the reported Error 1062 duplicate-entry scenario: several
// events sharing the exact same fixture timestamp/timestamp_usec, inside
// a single call's bulk payload, must come out with distinct timestamp_usec
// values from each other after withUniqueTimestamps runs.
func TestWithUniqueTimestampsProducesDistinctUsecAcrossFixtureEvents(t *testing.T) {
	raw := []byte(`{"messages":[
		{"timestamp":100,"timestamp_usec":1,"x":1},
		{"timestamp":100,"timestamp_usec":1,"x":2},
		{"timestamp":100,"timestamp_usec":1,"x":3}
	]}`)

	out := withUniqueTimestamps(raw, 500)

	var decoded struct {
		Messages []struct {
			Timestamp     int64 `json:"timestamp"`
			TimestampUsec int   `json:"timestamp_usec"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	seen := make(map[int]bool)
	for _, m := range decoded.Messages {
		if m.Timestamp != 600 {
			t.Fatalf("timestamp = %d, want 600 (100+500)", m.Timestamp)
		}
		if seen[m.TimestampUsec] {
			t.Fatalf("duplicate timestamp_usec %d across events in the same call", m.TimestampUsec)
		}
		seen[m.TimestampUsec] = true
	}
}

// TestWithUniqueTimestampsHandlesNonBulkEnvelope covers CLAUDE.md's bulk
// exception queues (acknowledgements, contactnotificationmethod, ...),
// whose envelope sits at the top level rather than nested under
// "messages".
func TestWithUniqueTimestampsHandlesNonBulkEnvelope(t *testing.T) {
	raw := []byte(`{"type":1700,"timestamp":100,"timestamp_usec":732213,"acknowledgement":{"host_name":"a"}}`)

	out := withUniqueTimestamps(raw, 500)

	var decoded struct {
		Timestamp     int64 `json:"timestamp"`
		TimestampUsec int   `json:"timestamp_usec"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Timestamp != 600 {
		t.Fatalf("timestamp = %d, want 600 (100+500)", decoded.Timestamp)
	}
	if decoded.TimestampUsec != 0 {
		t.Fatalf("timestamp_usec = %d, want 0 (first event of a fresh call)", decoded.TimestampUsec)
	}
}

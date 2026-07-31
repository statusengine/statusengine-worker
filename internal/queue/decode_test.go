package queue

import (
	"os"
	"path/filepath"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".claude", "specs", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func TestDecodeBulkQueues(t *testing.T) {
	t.Run("HostStatus", func(t *testing.T) {
		items, err := decodeHostStatus(readFixture(t, "statusngin_hoststatus.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 2 || items[0].Name != "demo.statusengine.org" {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("ServiceStatus", func(t *testing.T) {
		items, err := decodeServiceStatus(readFixture(t, "statusngin_servicestatus.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 2 || items[0].HostName != "foobar" || items[0].Description != "Swap Usage" {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("HostCheck", func(t *testing.T) {
		items, err := decodeHostCheck(readFixture(t, "statusngin_hostchecks.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 1 || items[0].HostName != "localhost" || items[0].ReturnCode != 0 {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("ServiceCheck", func(t *testing.T) {
		items, err := decodeServiceCheck(readFixture(t, "statusngin_servicechecks.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 2 || items[1].ServiceDescription != "PING" {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("ServicePerfdata sharing ServiceCheck shape", func(t *testing.T) {
		items, err := decodeServiceCheck(readFixture(t, "statusngin_service_perfdata.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 3 || items[0].PerfData != "users=0;20;50;0" {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("StateChange", func(t *testing.T) {
		events, err := decodeStateChange(readFixture(t, "statusngin_statechanges.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(events) != 2 || events[0].Timestamp != 1785470683 || events[0].ServiceDescription != "Flapping" {
			t.Fatalf("unexpected events: %+v", events)
		}
	})

	t.Run("LogEntry", func(t *testing.T) {
		items, err := decodeLogEntry(readFixture(t, "statusngin_logentries.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 3 || items[2].DataType != 1048576 {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("Notification", func(t *testing.T) {
		items, err := decodeNotification(readFixture(t, "statusngin_notifications.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 1 || items[0].ContactsNotified != 1 {
			t.Fatalf("unexpected items: %+v", items)
		}
	})
}

func TestDecodeNonBulkQueues(t *testing.T) {
	t.Run("ContactNotificationMethod", func(t *testing.T) {
		items, err := decodeContactNotificationMethod(readFixture(t, "statusngin_contactnotificationmethod.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 1 || items[0].ContactName != "286a2700-006a-4d7d-a6fe-6df5b3533ad2" {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("Acknowledgement", func(t *testing.T) {
		events, err := decodeAcknowledgement(readFixture(t, "statusngin_acknowledgements.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(events) != 1 || events[0].EntryTime != 1785516972 || events[0].AuthorName != "Daniel Z" {
			t.Fatalf("unexpected events: %+v", events)
		}
	})

	t.Run("Downtime", func(t *testing.T) {
		items, err := decodeDowntime(readFixture(t, "statusngin_downtimes.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 1 || items[0].DowntimeID != 1 {
			t.Fatalf("unexpected items: %+v", items)
		}
	})

	t.Run("CoreRestart", func(t *testing.T) {
		items, err := decodeCoreRestart(readFixture(t, "statusngin_core_restart.json"))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(items) != 1 || items[0].ObjectType != 102 {
			t.Fatalf("unexpected items: %+v", items)
		}
	})
}

func TestDecodeInvalidJSON(t *testing.T) {
	if _, err := decodeHostCheck([]byte(`not json`)); err == nil {
		t.Fatal("expected an error for invalid JSON")
	}
}

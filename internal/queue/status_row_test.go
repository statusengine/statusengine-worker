package queue

import "testing"

func TestIsPassiveCheck(t *testing.T) {
	cases := []struct {
		checkType int
		want      int
	}{
		{checkType: 0, want: 1}, // active-check convention: 0 == active -> is_passive_check
		{checkType: 1, want: 0},
		{checkType: 2, want: 0},
	}
	for _, c := range cases {
		if got := isPassiveCheck(c.checkType); got != c.want {
			t.Errorf("isPassiveCheck(%d) = %d, want %d", c.checkType, got, c.want)
		}
	}
}

func TestNewHostStatusRowMatchesColumns(t *testing.T) {
	items, err := decodeHostStatus(readFixture(t, "statusngin_hoststatus.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	row := newHostStatusRow("statusengine-test")(items[0], nil)
	if len(row) != len(hostStatusColumns) {
		t.Fatalf("row has %d values, want %d matching hostStatusColumns", len(row), len(hostStatusColumns))
	}

	col := func(name string) any {
		for i, c := range hostStatusColumns {
			if c == name {
				return row[i]
			}
		}
		t.Fatalf("column %q not found in hostStatusColumns", name)
		return nil
	}

	if got := col("hostname"); got != items[0].Name {
		t.Errorf("hostname = %v, want %v", got, items[0].Name)
	}
	if got := col("node_name"); got != "statusengine-test" {
		t.Errorf("node_name = %v, want %q", got, "statusengine-test")
	}
	// items[0] has check_type 0 in the fixture -> is_passive_check must be 1.
	if got := col("is_passive_check"); got != 1 {
		t.Errorf("is_passive_check = %v, want 1 (check_type == 0)", got)
	}
	if got := col("status_update_time"); got != items[0].Timestamp {
		t.Errorf("status_update_time = %v, want envelope timestamp %v", got, items[0].Timestamp)
	}
}

func TestNewServiceStatusRowMatchesColumns(t *testing.T) {
	items, err := decodeServiceStatus(readFixture(t, "statusngin_servicestatus.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	row := newServiceStatusRow("statusengine-test")(items[0], nil)
	if len(row) != len(serviceStatusColumns) {
		t.Fatalf("row has %d values, want %d matching serviceStatusColumns", len(row), len(serviceStatusColumns))
	}

	col := func(name string) any {
		for i, c := range serviceStatusColumns {
			if c == name {
				return row[i]
			}
		}
		t.Fatalf("column %q not found in serviceStatusColumns", name)
		return nil
	}

	if got := col("hostname"); got != items[0].HostName {
		t.Errorf("hostname = %v, want %v", got, items[0].HostName)
	}
	if got := col("service_description"); got != items[0].Description {
		t.Errorf("service_description = %v, want %v", got, items[0].Description)
	}
	if got := col("node_name"); got != "statusengine-test" {
		t.Errorf("node_name = %v, want %q", got, "statusengine-test")
	}
	// items[0] has check_type 0 in the fixture -> is_passive_check must be 1.
	if got := col("is_passive_check"); got != 1 {
		t.Errorf("is_passive_check = %v, want 1 (check_type == 0)", got)
	}
}

func TestHostStatusUpdateColumnsExcludePrimaryKey(t *testing.T) {
	for _, c := range hostStatusUpdateColumns {
		if c == "hostname" {
			t.Fatalf("hostStatusUpdateColumns must not include the primary key %q", c)
		}
	}
	if len(hostStatusUpdateColumns) != len(hostStatusColumns)-1 {
		t.Fatalf("hostStatusUpdateColumns has %d entries, want %d", len(hostStatusUpdateColumns), len(hostStatusColumns)-1)
	}
}

func TestServiceStatusUpdateColumnsExcludePrimaryKey(t *testing.T) {
	for _, c := range serviceStatusUpdateColumns {
		if c == "hostname" || c == "service_description" {
			t.Fatalf("serviceStatusUpdateColumns must not include primary key column %q", c)
		}
	}
	if len(serviceStatusUpdateColumns) != len(serviceStatusColumns)-2 {
		t.Fatalf("serviceStatusUpdateColumns has %d entries, want %d", len(serviceStatusUpdateColumns), len(serviceStatusColumns)-2)
	}
}

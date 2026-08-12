package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func intPtr(v int) *int { return &v }

func TestResolveOptionalInt(t *testing.T) {
	tests := []struct {
		name     string
		explicit map[string]bool
		flagVal  int
		envVal   string
		fileVal  *int
		want     int
		wantErr  bool
	}{
		{
			name:     "explicit flag wins over everything",
			explicit: map[string]bool{"x": true},
			flagVal:  1,
			envVal:   "2",
			fileVal:  intPtr(3),
			want:     1,
		},
		{
			name:    "env wins over file when flag not explicit",
			flagVal: 60,
			envVal:  "2",
			fileVal: intPtr(3),
			want:    2,
		},
		{
			name:    "file wins over default when neither flag nor env set",
			flagVal: 60,
			fileVal: intPtr(3),
			want:    3,
		},
		{
			name:    "falls back to default when nothing else is set",
			flagVal: 60,
			want:    60,
		},
		{
			// The whole reason this helper exists instead of cmd/app's
			// resolveInt: there, a file value of 0 means "key absent" and
			// loses to the default. Here it must win, or configuring
			// age_hostchecks: 0 to keep everything would instead delete
			// everything older than the 5-day default.
			name:    "file value of zero wins over a non-zero default",
			flagVal: 5,
			fileVal: intPtr(0),
			want:    0,
		},
		{
			name:    "env value of zero wins over a non-zero default",
			flagVal: 5,
			envVal:  "0",
			want:    0,
		},
		{
			name:     "explicit flag of zero wins",
			explicit: map[string]bool{"x": true},
			flagVal:  0,
			fileVal:  intPtr(60),
			want:     0,
		},
		{
			// A silent fall-through to the default here would delete data
			// the operator meant to keep, so it is an error instead.
			name:    "unparseable env value is an error",
			flagVal: 60,
			envVal:  "6O",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const envKey = "STATUSENGINE_TEST_RESOLVE_OPTIONAL_INT"
			if tt.envVal != "" {
				t.Setenv(envKey, tt.envVal)
			} else {
				os.Unsetenv(envKey)
			}

			explicit := tt.explicit
			if explicit == nil {
				explicit = map[string]bool{}
			}

			got, err := resolveOptionalInt(explicit, "x", tt.flagVal, envKey, tt.fileVal)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("got %d, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveOptionalInt: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestResolveDuration(t *testing.T) {
	tests := []struct {
		name     string
		explicit map[string]bool
		flagVal  time.Duration
		envVal   string
		fileVal  string
		want     time.Duration
		wantErr  bool
	}{
		{
			name:     "explicit flag wins over everything",
			explicit: map[string]bool{"x": true},
			flagVal:  time.Second,
			envVal:   "2s",
			fileVal:  "3s",
			want:     time.Second,
		},
		{
			name:    "env wins over file when flag not explicit",
			envVal:  "20ms",
			fileVal: "3s",
			want:    20 * time.Millisecond,
		},
		{
			name:    "file wins over default when neither flag nor env set",
			fileVal: "50ms",
			want:    50 * time.Millisecond,
		},
		{
			name:    "falls back to default when nothing else is set",
			flagVal: 0,
			want:    0,
		},
		{
			name:    `explicit "0s" in the file is honoured`,
			flagVal: time.Second,
			fileVal: "0s",
			want:    0,
		},
		{
			name:    "unparseable env value is an error",
			envVal:  "50",
			wantErr: true,
		},
		{
			name:    "unparseable file value is an error",
			fileVal: "half a second",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const envKey = "STATUSENGINE_TEST_RESOLVE_DURATION"
			if tt.envVal != "" {
				t.Setenv(envKey, tt.envVal)
			} else {
				os.Unsetenv(envKey)
			}

			explicit := tt.explicit
			if explicit == nil {
				explicit = map[string]bool{}
			}

			got, err := resolveDuration(explicit, "x", tt.flagVal, envKey, tt.fileVal)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("got %v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDuration: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRetentionTablesAreConsistent guards the hand-written list against
// copy-paste slips - a duplicated table name or a column pasted from the
// row above would otherwise clean the wrong data, and every entry looks
// alike enough that a reviewer's eye slides right over it.
func TestRetentionTablesAreConsistent(t *testing.T) {
	tables := retentionTables()

	if len(tables) != 14 {
		t.Errorf("got %d tables, want 14", len(tables))
	}

	seenTable := map[string]bool{}
	seenFlag := map[string]bool{}

	for _, rt := range tables {
		if rt.flagName == "" || rt.table == "" || rt.column == "" {
			t.Errorf("incomplete entry: %+v", rt)
			continue
		}
		if rt.def < 0 {
			t.Errorf("%s: negative default %d", rt.flagName, rt.def)
		}
		if rt.file == nil {
			t.Errorf("%s: no config-file accessor", rt.flagName)
		}
		if seenTable[rt.table] {
			t.Errorf("table %s listed twice", rt.table)
		}
		if seenFlag[rt.flagName] {
			t.Errorf("flag %s listed twice", rt.flagName)
		}
		seenTable[rt.table] = true
		seenFlag[rt.flagName] = true

		if !strings.HasPrefix(rt.flagName, "age-") {
			t.Errorf("%s: retention flags are named age-*", rt.flagName)
		}
		if !strings.HasPrefix(rt.table, "statusengine_") {
			t.Errorf("%s: unexpected table name %q", rt.flagName, rt.table)
		}
	}
}

// TestRetentionTablesMatchSchema cross-checks every entry against the
// schema dump rather than against a second hand-written list: a column
// that does not exist on that table would only surface as a MySQL error
// at 3am in a cronjob nobody watches.
func TestRetentionTablesMatchSchema(t *testing.T) {
	schema, err := os.ReadFile(filepath.Join("..", "..", ".claude", "specs", "mysql_schema.sql"))
	if err != nil {
		t.Skipf("schema dump not available: %v", err)
	}

	for _, rt := range retentionTables() {
		t.Run(rt.table, func(t *testing.T) {
			create := createTableBlock(string(schema), rt.table)
			if create == "" {
				t.Fatalf("table %s not found in the schema dump", rt.table)
			}
			if !strings.Contains(create, "`"+rt.column+"`") {
				t.Errorf("column %s not found in %s", rt.column, rt.table)
			}
		})
	}
}

// TestFileConfigCoversEveryRetentionTable makes sure each entry's file
// accessor reads its own key: with fourteen near-identical closures, a
// pasted "return fc.AgeHostNotifications" in the service row would be
// invisible on review but would silently apply the host retention to
// service data.
func TestFileConfigCoversEveryRetentionTable(t *testing.T) {
	tables := retentionTables()

	for i, rt := range tables {
		t.Run(rt.yamlKey(), func(t *testing.T) {
			// A file that sets only this one key, to a value no other
			// entry uses as its default.
			const marker = 4242
			doc := rt.yamlKey() + ": " + "4242\n"

			var fc fileConfig
			if err := yaml.Unmarshal([]byte(doc), &fc); err != nil {
				t.Fatalf("yaml.Unmarshal(%q): %v", doc, err)
			}

			got := rt.file(fc)
			if got == nil {
				t.Fatalf("accessor returned nil - %s is not wired to a fileConfig field", rt.yamlKey())
			}
			if *got != marker {
				t.Fatalf("accessor returned %d, want %d", *got, marker)
			}

			// And no other entry may see it, which is what catches a
			// duplicated closure.
			for j, other := range tables {
				if i == j {
					continue
				}
				if v := other.file(fc); v != nil {
					t.Errorf("setting %s also filled %s - the two share an accessor",
						rt.yamlKey(), other.yamlKey())
				}
			}
		})
	}
}

func TestEnvAndYAMLKeysAreDerivedFromTheFlag(t *testing.T) {
	rt := retentionTable{flagName: "age-host-notifications-log"}

	if got, want := rt.yamlKey(), "age_host_notifications_log"; got != want {
		t.Errorf("yamlKey() = %q, want %q", got, want)
	}
	if got, want := rt.envKey(), "STATUSENGINE_AGE_HOST_NOTIFICATIONS_LOG"; got != want {
		t.Errorf("envKey() = %q, want %q", got, want)
	}
}

// TestLoadFileConfigSharesTheWorkerFile is the premise the whole "same
// YAML file" requirement rests on: this binary must read the keys it cares
// about out of a file full of the worker's settings, and ignore the rest
// rather than failing on them.
func TestLoadFileConfigSharesTheWorkerFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	doc := `
consumer: gearman
gearman_addr: 127.0.0.1:4730
mysql_dsn: user:pass@tcp(db:3306)/statusengine
api_keys:
  - some-secret
enable_openitcockpit_tweaks: true

age_hostchecks: 0
age_perfdata: 30
cleanup_batch_size: 250
cleanup_batch_pause: 50ms
`
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	fc := loadFileConfig(path)

	if fc.MySQLDSN != "user:pass@tcp(db:3306)/statusengine" {
		t.Errorf("MySQLDSN = %q", fc.MySQLDSN)
	}
	if fc.CleanupBatchSize != 250 {
		t.Errorf("CleanupBatchSize = %d, want 250", fc.CleanupBatchSize)
	}
	if fc.CleanupBatchPause != "50ms" {
		t.Errorf("CleanupBatchPause = %q, want %q", fc.CleanupBatchPause, "50ms")
	}
	if fc.AgeHostchecks == nil || *fc.AgeHostchecks != 0 {
		t.Errorf("AgeHostchecks = %v, want a pointer to 0", fc.AgeHostchecks)
	}
	if fc.AgePerfdata == nil || *fc.AgePerfdata != 30 {
		t.Errorf("AgePerfdata = %v, want a pointer to 30", fc.AgePerfdata)
	}
	// Not mentioned in the file at all - must stay nil so it falls
	// through to the built-in default rather than to zero.
	if fc.AgeLogentries != nil {
		t.Errorf("AgeLogentries = %v, want nil for an absent key", *fc.AgeLogentries)
	}
}

// TestExampleConfigParses keeps config.example.yaml honest: it is the
// file operators copy, and a key misspelled there is a retention setting
// that silently does nothing.
func TestExampleConfigParses(t *testing.T) {
	path := filepath.Join("..", "..", "config.example.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("config.example.yaml not available: %v", err)
	}

	fc := loadFileConfig(path)

	for _, rt := range retentionTables() {
		if v := rt.file(fc); v == nil {
			t.Errorf("config.example.yaml does not set %s (or spells it differently)", rt.yamlKey())
		} else if *v != rt.def {
			t.Errorf("config.example.yaml sets %s to %d, but the built-in default is %d",
				rt.yamlKey(), *v, rt.def)
		}
	}

	if fc.CleanupBatchSize == 0 {
		t.Error("config.example.yaml does not set cleanup_batch_size")
	}
	if fc.CleanupBatchPause == "" {
		t.Error("config.example.yaml does not set cleanup_batch_pause")
	}
	if _, err := time.ParseDuration(fc.CleanupBatchPause); err != nil {
		t.Errorf("cleanup_batch_pause in config.example.yaml is not a duration: %v", err)
	}
}

// createTableBlock returns the CREATE TABLE statement for name from a
// mysqldump-style schema, or "" if there is none.
func createTableBlock(schema, name string) string {
	marker := "CREATE TABLE `" + name + "` ("
	start := strings.Index(schema, marker)
	if start < 0 {
		return ""
	}
	rest := schema[start:]
	if end := strings.Index(rest, ";"); end >= 0 {
		return rest[:end]
	}
	return rest
}

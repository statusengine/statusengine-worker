package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveString(t *testing.T) {
	tests := []struct {
		name     string
		explicit map[string]bool
		flagVal  string
		envKey   string
		envVal   string
		fileVal  string
		want     string
	}{
		{
			name:     "explicit flag wins over everything",
			explicit: map[string]bool{"x": true},
			flagVal:  "from-flag",
			envVal:   "from-env",
			fileVal:  "from-file",
			want:     "from-flag",
		},
		{
			name:    "env wins over file when flag not explicit",
			flagVal: "default-value",
			envVal:  "from-env",
			fileVal: "from-file",
			want:    "from-env",
		},
		{
			name:    "file wins over default when neither flag nor env set",
			flagVal: "default-value",
			fileVal: "from-file",
			want:    "from-file",
		},
		{
			name:    "falls back to default (flagVal) when nothing else is set",
			flagVal: "default-value",
			want:    "default-value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const envKey = "STATUSENGINE_TEST_RESOLVE_STRING"
			if tt.envVal != "" {
				t.Setenv(envKey, tt.envVal)
			} else {
				os.Unsetenv(envKey)
			}
			explicit := tt.explicit
			if explicit == nil {
				explicit = map[string]bool{}
			}
			got := resolveString(explicit, "x", tt.flagVal, envKey, tt.fileVal)
			if got != tt.want {
				t.Fatalf("resolveString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveBool(t *testing.T) {
	trueVal := true
	falseVal := false

	tests := []struct {
		name     string
		explicit map[string]bool
		flagVal  bool
		envVal   string // "" means unset
		fileVal  *bool
		want     bool
	}{
		{
			name:     "explicit flag wins over everything",
			explicit: map[string]bool{"x": true},
			flagVal:  true,
			envVal:   "false",
			fileVal:  &falseVal,
			want:     true,
		},
		{
			name:    "env wins over file when flag not explicit",
			flagVal: false,
			envVal:  "true",
			fileVal: &falseVal,
			want:    true,
		},
		{
			name:    "file explicitly false is honored (not confused with unset)",
			flagVal: true,
			fileVal: &falseVal,
			want:    false,
		},
		{
			name:    "file explicitly true wins over default",
			flagVal: false,
			fileVal: &trueVal,
			want:    true,
		},
		{
			name:    "falls back to default (flagVal) when nothing else is set",
			flagVal: true,
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const envKey = "STATUSENGINE_TEST_RESOLVE_BOOL"
			if tt.envVal != "" {
				t.Setenv(envKey, tt.envVal)
			} else {
				os.Unsetenv(envKey)
			}
			explicit := tt.explicit
			if explicit == nil {
				explicit = map[string]bool{}
			}
			got := resolveBool(explicit, "x", tt.flagVal, envKey, tt.fileVal)
			if got != tt.want {
				t.Fatalf("resolveBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadFileConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
consumer: rabbitmq
mysql_dsn: "user:pass@tcp(db:3306)/statusengine"
api_keys:
  - key-one
  - key-two
enable_openitcockpit_tweaks: true
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	fc := loadFileConfig(path)

	if fc.Consumer != "rabbitmq" {
		t.Errorf("Consumer = %q, want %q", fc.Consumer, "rabbitmq")
	}
	if fc.MySQLDSN != "user:pass@tcp(db:3306)/statusengine" {
		t.Errorf("MySQLDSN = %q, want the configured DSN", fc.MySQLDSN)
	}
	if len(fc.APIKeys) != 2 || fc.APIKeys[0] != "key-one" || fc.APIKeys[1] != "key-two" {
		t.Errorf("APIKeys = %v, want [key-one key-two]", fc.APIKeys)
	}
	if fc.EnableOpenITCockpitTweaks == nil || !*fc.EnableOpenITCockpitTweaks {
		t.Errorf("EnableOpenITCockpitTweaks = %v, want true", fc.EnableOpenITCockpitTweaks)
	}
	// A key the file never mentions must stay unset (nil/zero), not
	// silently default to something - resolveString/resolveBool rely on
	// this to know the file didn't weigh in.
	if fc.GraphitePrefix != "" {
		t.Errorf("GraphitePrefix = %q, want empty (not mentioned in the file)", fc.GraphitePrefix)
	}
}

// TestExampleConfigParses guards config.example.yaml against silently
// going stale (e.g. a typo'd key after a rename) - it must always parse
// cleanly, since it's the first thing a user copies.
func TestExampleConfigParses(t *testing.T) {
	fc := loadFileConfig("../../config.example.yaml")
	if fc.Consumer != "gearman" {
		t.Errorf("Consumer = %q, want %q", fc.Consumer, "gearman")
	}
	if fc.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", fc.LogLevel, "info")
	}
}

package command

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func env(t *testing.T, body string) *Envelope {
	t.Helper()
	var e Envelope
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("test fixture is not valid JSON: %v", err)
	}
	return &e
}

func TestValidateAcceptsEveryKnownCommand(t *testing.T) {
	cases := map[string]string{
		CommandCheckResult:    `{"Command":"check_result","Data":{"host_name":"h","output":"OK"}}`,
		CommandScheduleCheck:  `{"Command":"schedule_check","Data":{"host_name":"h","schedule_time":1700000000}}`,
		CommandDeleteDowntime: `{"Command":"delete_downtime","Data":{"host_name":"h"}}`,
		CommandRaw:            `{"Command":"raw","Data":"SCHEDULE_FORCED_SVC_CHECK;h;s;1700000000"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := Validate(env(t, body))
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if len(got) != 1 || got[0] != name {
				t.Errorf("command names = %v, want [%s]", got, name)
			}
		})
	}
}

// The four names are not a style choice - they are the entire dispatch in
// the broker's ProcessMessage. If this list and the broker's ever disagree,
// the extra name is a command that is published and then silently dropped.
func TestKnownCommandsMatchTheBrokerDispatch(t *testing.T) {
	want := []string{"check_result", "schedule_check", "delete_downtime", "raw"}
	if len(KnownCommands) != len(want) {
		t.Fatalf("KnownCommands = %v, want %v", KnownCommands, want)
	}
	for i := range want {
		if KnownCommands[i] != want[i] {
			t.Errorf("KnownCommands[%d] = %q, want %q", i, KnownCommands[i], want[i])
		}
	}
}

func TestValidateRejectsUnknownCommand(t *testing.T) {
	_, err := Validate(env(t, `{"Command":"check_results","Data":{"host_name":"h"}}`))
	assertReason(t, err, ReasonUnknownCommand)
	// The near-miss plural is the realistic typo, and the whole reason this
	// endpoint validates at all: the broker would drop it without logging.
	if !strings.Contains(err.Error(), "check_results") {
		t.Errorf("error should name the offending command, got %q", err)
	}
}

func TestValidateRejectsMissingFields(t *testing.T) {
	for name, body := range map[string]string{
		"no Command": `{"Data":{"host_name":"h"}}`,
		"no Data":    `{"Command":"raw"}`,
		"empty":      `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Validate(env(t, body))
			assertReason(t, err, ReasonMalformed)
		})
	}
}

func TestValidateAcceptsBulkOfMixedCommands(t *testing.T) {
	body := `{"messages":[
		{"Command":"check_result","Data":{"host_name":"h","output":"OK"}},
		{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"},
		{"Command":"schedule_check","Data":{"host_name":"h","schedule_time":1}}
	]}`
	got, err := Validate(env(t, body))
	if err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
	want := []string{CommandCheckResult, CommandRaw, CommandScheduleCheck}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("command names = %v, want %v", got, want)
	}
}

// The broker recurses into "messages" from the same handler that dispatches
// Command/Data, so a bulk may mix command types - it is not a check_result
// feature, which is the thing that was uncertain.
func TestBulkIsNotLimitedToCheckResults(t *testing.T) {
	for _, name := range KnownCommands {
		t.Run(name, func(t *testing.T) {
			data := `{"host_name":"h","output":"OK","schedule_time":1}`
			if name == CommandRaw {
				data = `"ENABLE_HOST_FLAP_DETECTION;h"`
			}
			body := fmt.Sprintf(`{"messages":[{"Command":%q,"Data":%s}]}`, name, data)
			if _, err := Validate(env(t, body)); err != nil {
				t.Errorf("bulk of %s: %v", name, err)
			}
		})
	}
}

// The broker's haveList flag makes it process "messages" and ignore a
// Command/Data pair sitting beside it. Accepting that shape would return a
// 202 for a command that is never going to run.
func TestValidateRejectsBulkMixedWithASingleCommand(t *testing.T) {
	body := `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h",
	          "messages":[{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h2"}]}`
	_, err := Validate(env(t, body))
	assertReason(t, err, ReasonMalformed)
}

func TestValidateRejectsOversizedBulk(t *testing.T) {
	items := make([]string, MaxBulkMessages+1)
	for i := range items {
		items[i] = `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}`
	}
	body := `{"messages":[` + strings.Join(items, ",") + `]}`
	_, err := Validate(env(t, body))
	assertReason(t, err, ReasonTooLarge)
}

func TestValidateRejectsNestedBulk(t *testing.T) {
	body := `{"messages":[{"messages":[{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"}]}]}`
	_, err := Validate(env(t, body))
	assertReason(t, err, ReasonMalformed)
}

func TestValidateRejectsBadRawData(t *testing.T) {
	for name, body := range map[string]string{
		"object instead of string": `{"Command":"raw","Data":{"cmd":"ENABLE_HOST_FLAP_DETECTION;h"}}`,
		"empty string":             `{"Command":"raw","Data":""}`,
		"whitespace only":          `{"Command":"raw","Data":"   "}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Validate(env(t, body))
			assertReason(t, err, ReasonMalformed)
		})
	}
}

// A newline does not inject a second command - Naemon's
// process_external_command() strips and then parses exactly one. It does
// reach nm_log("EXTERNAL COMMAND: %s;%s\n", ...), so it forges a line break
// in naemon.log, which is itself an ingested data source.
func TestValidateRejectsControlCharactersInRaw(t *testing.T) {
	for name, data := range map[string]string{
		"newline":         "ADD_SVC_COMMENT;h;s;1;author;comment\nSHUTDOWN_PROGRAM",
		"carriage return": "ADD_SVC_COMMENT;h;s;1;author;comment\rmore",
		"null byte":       "ADD_SVC_COMMENT;h;s;1;author;com\x00ment",
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(data)
			body := fmt.Sprintf(`{"Command":"raw","Data":%s}`, raw)
			_, err := Validate(env(t, body))
			assertReason(t, err, ReasonMalformed)
		})
	}
}

// A tab is legal inside plugin output and comment text, so rejecting every
// control character wholesale would turn away valid commands.
func TestValidateAllowsTabInRaw(t *testing.T) {
	raw, _ := json.Marshal("ADD_SVC_COMMENT;h;s;1;author;two\tcolumns")
	body := fmt.Sprintf(`{"Command":"raw","Data":%s}`, raw)
	if _, err := Validate(env(t, body)); err != nil {
		t.Errorf("a tab should be accepted, got %v", err)
	}
}

func TestValidateDeniesEveryDangerousCommand(t *testing.T) {
	for _, name := range DeniedCommands() {
		t.Run(name, func(t *testing.T) {
			body := fmt.Sprintf(`{"Command":"raw","Data":%q}`, name+";arg")
			_, err := Validate(env(t, body))
			assertReason(t, err, ReasonDenied)
		})
	}
}

// The list itself, spelled out. Naemon registers SHUTDOWN_PROGRAM and
// SHUTDOWN_PROCESS against the same shutdown_handler, and RESTART_PROGRAM
// and RESTART_PROCESS against the same restart_handler - so denying only
// the _PROGRAM spelling, which is the one everybody knows, is a filter that
// stops nothing. PROCESS_FILE is what makes the rest mean anything: it has
// Naemon read commands out of a file, which walks straight around any
// denylist. A test that only iterated DeniedCommands() would pass happily
// on a list that had lost three of the five.
func TestDenylistCoversBothSpellingsAndTheFileBypass(t *testing.T) {
	required := []string{
		"SHUTDOWN_PROGRAM", "SHUTDOWN_PROCESS",
		"RESTART_PROGRAM", "RESTART_PROCESS",
		"PROCESS_FILE",
	}
	have := make(map[string]bool)
	for _, name := range DeniedCommands() {
		have[name] = true
	}
	for _, name := range required {
		if !have[name] {
			t.Errorf("%s is not denied - see the comment on deniedRawCommands", name)
		}
	}
}

// Naemon upper-cases when it looks a command up in its register, so a
// denylist that only matched the exact upper-case spelling could be stepped
// around with lower-case letters.
func TestDenylistIsCaseInsensitive(t *testing.T) {
	for _, data := range []string{"shutdown_program", "Shutdown_Program", "sHuTdOwN_pRoCeSs"} {
		_, err := Validate(env(t, fmt.Sprintf(`{"Command":"raw","Data":%q}`, data)))
		assertReason(t, err, ReasonDenied)
	}
}

// Naemon tolerates an optional "[timestamp] " ahead of the command name, so
// the name is not always the first token - and a denylist that assumed it
// was would be bypassed by the documented syntax.
func TestDenylistSeesThroughTheTimestampPrefix(t *testing.T) {
	for _, data := range []string{
		"[1700000000] SHUTDOWN_PROGRAM",
		"[1700000000] PROCESS_FILE;/tmp/cmds;0",
	} {
		_, err := Validate(env(t, fmt.Sprintf(`{"Command":"raw","Data":%q}`, data)))
		assertReason(t, err, ReasonDenied)
	}
}

// A denylist entry buried inside a bulk must be caught too - otherwise the
// way around it is one extra level of JSON.
func TestDenylistAppliesInsideABulk(t *testing.T) {
	body := `{"messages":[
		{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;h"},
		{"Command":"raw","Data":"SHUTDOWN_PROGRAM"}
	]}`
	_, err := Validate(env(t, body))
	assertReason(t, err, ReasonDenied)
}

// Ordinary commands that merely look alarming must still get through -
// the denylist is against accidents, not a policy engine.
func TestDenylistDoesNotOverreach(t *testing.T) {
	for _, data := range []string{
		"DISABLE_NOTIFICATIONS",
		"PROCESS_HOST_CHECK_RESULT;h;0;output",
		"PROCESS_SERVICE_CHECK_RESULT;h;s;0;output",
		"DEL_ALL_HOST_COMMENTS;h",
	} {
		if _, err := Validate(env(t, fmt.Sprintf(`{"Command":"raw","Data":%q}`, data))); err != nil {
			t.Errorf("%s should be allowed, got %v", data, err)
		}
	}
}

func assertReason(t *testing.T, err error, want RejectReason) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error with reason %q, got nil", want)
	}
	got, ok := ReasonOf(err)
	if !ok {
		t.Fatalf("error %v does not carry a reject reason", err)
	}
	if got != want {
		t.Errorf("reason = %q, want %q (error: %v)", got, want, err)
	}
}

// Naemon's command_parse() refuses any external command that does not begin
// with "[<timestamp>]" - unconditionally, there is no syntax mode where it
// is optional. Measured against a real Naemon before this was added:
// "External command parse error ENABLE_HOST_FLAP_DETECTION;localhost
// (Commands must begin with a timestamp inside square brackets)". The core
// logs that warning and tells nobody else, so without this the caller would
// get a 202 for a command that did precisely nothing.
func TestRawCommandsGetAnEntryTime(t *testing.T) {
	e := env(t, `{"Command":"raw","Data":"ENABLE_HOST_FLAP_DETECTION;localhost"}`)
	if _, err := Validate(e); err != nil {
		t.Fatalf("Validate() = %v", err)
	}

	var got string
	if err := json.Unmarshal(e.Data, &got); err != nil {
		t.Fatalf("Data is no longer a string: %v", err)
	}
	if !strings.HasPrefix(got, "[") {
		t.Fatalf("Data = %q, want a leading [timestamp] Naemon will accept", got)
	}
	if !strings.HasSuffix(got, "] ENABLE_HOST_FLAP_DETECTION;localhost") {
		t.Errorf("Data = %q, the command itself must be unchanged after the prefix", got)
	}
	var ts int64
	if _, err := fmt.Sscanf(got, "[%d]", &ts); err != nil {
		t.Errorf("prefix of %q is not a unix timestamp: %v", got, err)
	}
	if ts < 1_600_000_000 {
		t.Errorf("timestamp %d does not look like the current time", ts)
	}
}

// A caller that supplied its own entry time meant it, so it survives
// untouched - the prefix is filled in, never overwritten.
func TestExistingEntryTimeIsLeftAlone(t *testing.T) {
	e := env(t, `{"Command":"raw","Data":"[1700000000] ENABLE_HOST_FLAP_DETECTION;localhost"}`)
	if _, err := Validate(e); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	var got string
	_ = json.Unmarshal(e.Data, &got)
	if got != "[1700000000] ENABLE_HOST_FLAP_DETECTION;localhost" {
		t.Errorf("Data = %q, want it unchanged", got)
	}
}

// The entry time is added after the denylist has been consulted, and
// rawCommandName looks past a prefix either way - so neither supplying one
// nor omitting one is a route around the list.
func TestEntryTimeIsNotARouteAroundTheDenylist(t *testing.T) {
	for _, data := range []string{"SHUTDOWN_PROGRAM", "[1700000000] SHUTDOWN_PROGRAM"} {
		_, err := Validate(env(t, fmt.Sprintf(`{"Command":"raw","Data":%q}`, data)))
		assertReason(t, err, ReasonDenied)
	}
}

// Only raw carries a command string; the other three take an object of
// named fields that Naemon never parses as a command line, so nothing must
// be prepended to those.
func TestEntryTimeIsOnlyAddedToRaw(t *testing.T) {
	e := env(t, `{"Command":"schedule_check","Data":{"host_name":"h","schedule_time":1}}`)
	before := string(e.Data)
	if _, err := Validate(e); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	if string(e.Data) != before {
		t.Errorf("Data = %s, want it unchanged (%s)", e.Data, before)
	}
}

// Package command implements the worker's one *writing* endpoint: an HTTP
// API that accepts Naemon external commands and publishes them onto the
// statusngin_cmd queue, where the Statusengine NEB broker module picks them
// up and hands them to the monitoring core.
//
// The direction is the opposite of everything else in this worker. Every
// other queue is something the NEB module publishes and this worker
// consumes; statusngin_cmd is one of the module's three WorkerQueue values
// (src/Queue.h: enum class WorkerQueue { OCSP, OCHP, Command }), which the
// module consumes on both Gearman and RabbitMQ. So what is needed here is a
// publisher, not a consumer - see publisher.go.
package command

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Queue is the queue name the NEB broker module consumes external commands
// from. It matches the WorkerCommand entry in statusengine.toml; the module
// will not see anything published anywhere else.
const Queue = "statusngin_cmd"

// The four command names the NEB broker module understands, from the
// dispatch in ProcessMessage (src/MessageHandler/MessageHandler.h).
const (
	CommandCheckResult    = "check_result"
	CommandScheduleCheck  = "schedule_check"
	CommandDeleteDowntime = "delete_downtime"
	CommandRaw            = "raw"
)

// KnownCommands lists every command name the broker acts on, in the order
// its dispatch checks them.
var KnownCommands = []string{
	CommandCheckResult,
	CommandScheduleCheck,
	CommandDeleteDowntime,
	CommandRaw,
}

// MaxBulkMessages caps how many commands one request may carry. The broker
// walks the array recursively with no limit of its own, and the request
// body is already bounded by MaxBodyBytes - this exists so that a rejection
// names the actual problem ("too many commands") instead of surfacing as a
// truncated body.
const MaxBulkMessages = 1000

// MaxBodyBytes caps the request body. Generous enough for a full bulk of
// check results with long plugin output, small enough that an unauthenticated
// caller cannot make the process allocate on a whim.
const MaxBodyBytes = 8 << 20 // 8 MiB

// deniedRawCommands are the Naemon external commands this API refuses even
// with a valid key. The list is deliberately short, and every entry is here
// for a reason that was checked in naemon-core rather than assumed:
//
//   - SHUTDOWN_PROGRAM / SHUTDOWN_PROCESS and RESTART_PROGRAM /
//     RESTART_PROCESS are two names each for one thing. Naemon registers
//     both spellings against the same shutdown_handler/restart_handler
//     (src/naemon/commands.c, register_core_commands), so denying only the
//     _PROGRAM spelling - the one everybody knows - would be a filter that
//     looks right and stops nothing.
//   - PROCESS_FILE is the entry that makes the rest of the list mean
//     anything. It calls process_external_commands_from_file(), which reads
//     a file and runs each line as an external command, so without it a
//     caller can put SHUTDOWN_PROGRAM in a file and have Naemon read it -
//     the denylist would be decoration.
//
// What is deliberately *not* here: the CHANGE_*_CHECK_COMMAND and
// CHANGE_*_EVENT_HANDLER commands, which would be the obvious route to
// running arbitrary code. Naemon disables all of them internally
// (`/*disabled*/ return ERROR` in commands.c), so there is nothing to deny.
//
// This is a denylist and therefore protects against an accident, not against
// intent: a caller holding a valid key can still DISABLE_NOTIFICATIONS. The
// real control is which key exists and who holds it (see auth.go).
var deniedRawCommands = map[string]struct{}{
	"SHUTDOWN_PROGRAM": {},
	"SHUTDOWN_PROCESS": {},
	"RESTART_PROGRAM":  {},
	"RESTART_PROCESS":  {},
	"PROCESS_FILE":     {},
}

// Envelope is the message format the NEB broker module reads. It is
// reproduced exactly rather than translated: the field names are
// case-sensitive on the broker side ("Command" and "Data" capitalised,
// "messages" not), and a second format would be one more thing to keep in
// step with a component that lives in another repository.
//
// A message carries either Command+Data or messages, never both in
// practice: the broker checks for "messages" while walking the same object,
// and if it finds one it processes the array and ignores Command/Data
// entirely (its haveList flag).
type Envelope struct {
	Command  string          `json:"Command,omitempty"`
	Data     json.RawMessage `json:"Data,omitempty"`
	Messages []Envelope      `json:"messages,omitempty"`
}

// RejectReason classifies why a request was refused. The values double as
// the "reason" label on statusengine_commands_rejected_total, so they are a
// small fixed set rather than free-form text.
type RejectReason string

const (
	ReasonAuth           RejectReason = "auth"
	ReasonMalformed      RejectReason = "malformed"
	ReasonUnknownCommand RejectReason = "unknown_command"
	ReasonDenied         RejectReason = "denied"
	ReasonTooLarge       RejectReason = "too_large"
)

// ValidationError carries both the message shown to the caller and the
// reason the metric is labeled with, so the handler never has to infer one
// from the other.
type ValidationError struct {
	Reason  RejectReason
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

func reject(reason RejectReason, format string, args ...any) error {
	return &ValidationError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ReasonOf reports the reject reason behind err, and whether err was a
// ValidationError at all.
func ReasonOf(err error) (RejectReason, bool) {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return ve.Reason, true
	}
	return "", false
}

// Validate checks one envelope, recursing into a bulk, and returns the
// command names it contains in order (so the caller can count them per
// name) - or the first problem it finds.
//
// Validating at all is the point of putting this endpoint in front of the
// queue. The broker's dispatch has no else branch: an envelope whose
// Command is not one of the four falls through every comparison and is
// dropped **without a single log line**, on a queue nobody is watching. Once
// a message is published there is no feedback path at all, so a typo caught
// here is the difference between a 400 and a command that silently never
// happened.
func Validate(env *Envelope) ([]string, error) {
	return validate(env, 0)
}

func validate(env *Envelope, depth int) ([]string, error) {
	// The broker recurses through "messages" with no depth limit of its
	// own. One level is what every real client sends and what the PHP
	// examples in .claude/specs show; refusing deeper nesting keeps this
	// walk bounded without turning away anything anyone actually does.
	if depth > 1 {
		return nil, reject(ReasonMalformed, "nested bulk messages are not supported")
	}

	if len(env.Messages) > 0 {
		if env.Command != "" || len(env.Data) > 0 {
			// Not merely untidy: the broker would silently ignore the
			// Command/Data pair here, so accepting this would confirm a
			// command that is never going to run.
			return nil, reject(ReasonMalformed,
				`a bulk carries "messages" only - a "Command"/"Data" pair alongside it is silently ignored by the broker`)
		}
		if len(env.Messages) > MaxBulkMessages {
			return nil, reject(ReasonTooLarge, "bulk carries %d commands, the limit is %d",
				len(env.Messages), MaxBulkMessages)
		}
		var names []string
		for i := range env.Messages {
			inner, err := validate(&env.Messages[i], depth+1)
			if err != nil {
				return nil, fmt.Errorf("messages[%d]: %w", i, err)
			}
			names = append(names, inner...)
		}
		return names, nil
	}

	if env.Command == "" {
		return nil, reject(ReasonMalformed, `missing "Command"`)
	}
	if len(env.Data) == 0 {
		return nil, reject(ReasonMalformed, `missing "Data"`)
	}
	if !isKnownCommand(env.Command) {
		return nil, reject(ReasonUnknownCommand,
			"unknown command %q - the broker accepts only %s, and drops anything else without logging it",
			env.Command, strings.Join(KnownCommands, ", "))
	}

	if env.Command == CommandRaw {
		normalized, err := validateRaw(env.Data)
		if err != nil {
			return nil, err
		}
		env.Data = normalized
	}

	return []string{env.Command}, nil
}

func isKnownCommand(name string) bool {
	for _, known := range KnownCommands {
		if name == known {
			return true
		}
	}
	return false
}

// validateRaw checks the Data of a "raw" command, which - unlike the other
// three, whose Data is an object of named fields - is a bare string handed
// straight to Naemon's process_external_command1(). It returns the Data to
// publish, which is not always the Data that came in: see addEntryTime.
func validateRaw(data json.RawMessage) (json.RawMessage, error) {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, reject(ReasonMalformed, `"Data" for a raw command must be a string, e.g. "SCHEDULE_FORCED_SVC_CHECK;host;service;1700000000"`)
	}

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, reject(ReasonMalformed, "raw command is empty")
	}

	// Control characters are refused, but not for the reason one might
	// expect. An embedded newline does *not* inject a second command:
	// process_external_command() calls strip() and then parses exactly one
	// command, with no loop. What it does do is reach nm_log(), which
	// writes "EXTERNAL COMMAND: %s;%s\n" - so a newline inside an argument
	// forges a line break in naemon.log, and that log is itself an ingested
	// data source. Refusing them also means a client bug surfaces as a 400
	// here rather than as a puzzling log file later.
	for _, r := range raw {
		if r == '\t' {
			continue // legal inside a comment or plugin output
		}
		if unicode.IsControl(r) {
			return nil, reject(ReasonMalformed,
				"raw command contains a control character (%q); newlines in particular would forge a line break in naemon.log", r)
		}
	}

	// Naemon accepts an optional "[timestamp] " prefix ahead of the command
	// name, so the name is not always the first token.
	name := rawCommandName(trimmed)
	if _, denied := deniedRawCommands[name]; denied {
		return nil, reject(ReasonDenied,
			"the external command %s is not permitted through this API", name)
	}

	out, err := json.Marshal(addEntryTime(trimmed))
	if err != nil {
		return nil, reject(ReasonMalformed, "could not re-encode the raw command: %v", err)
	}
	return out, nil
}

// addEntryTime prefixes a raw command with "[<unix timestamp>] " unless it
// already carries one.
//
// Naemon requires it, unconditionally: command_parse() begins by reading the
// entry time and returns "Commands must begin with a timestamp inside square
// brackets" if it is not there (src/naemon/commands.c). There is no syntax
// mode in which it is optional. A command without it is refused by the core,
// which logs a warning into naemon.log and tells nobody else - so a caller
// that omitted it would get a 202 from this API and no effect whatsoever,
// which is the exact failure mode this endpoint exists to prevent.
//
// Supplying it here rather than rejecting the request is a deliberate
// choice, and the narrow kind: the entry time is mechanical syntax carrying
// no intent a caller could express better - it is *when the command was
// submitted*, and that is now. Requiring every client to prepend a
// timestamp to every command, on pain of a 400, would be boilerplate on
// every single request. A timestamp the caller did supply is left exactly
// as it is, so anything that does care keeps control.
func addEntryTime(raw string) string {
	if strings.HasPrefix(raw, "[") && strings.Contains(raw, "]") {
		return raw
	}
	return fmt.Sprintf("[%d] %s", time.Now().Unix(), raw)
}

// rawCommandName extracts the Naemon command name from a raw command
// string: everything before the first ';', minus an optional leading
// "[timestamp]" that Naemon's command_parse also tolerates. Upper-cased,
// since Naemon's command register is upper-case and a denylist that can be
// stepped around with lower-case letters is no denylist.
func rawCommandName(raw string) string {
	name := raw
	if idx := strings.IndexByte(name, ';'); idx >= 0 {
		name = name[:idx]
	}
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, "[") {
		if idx := strings.IndexByte(name, ']'); idx >= 0 {
			name = strings.TrimSpace(name[idx+1:])
		}
	}
	return strings.ToUpper(name)
}

// DeniedCommands returns the denied external command names, for the startup
// log line and for tests that insist the list has not quietly shrunk.
func DeniedCommands() []string {
	names := make([]string, 0, len(deniedRawCommands))
	for name := range deniedRawCommands {
		names = append(names, name)
	}
	return names
}

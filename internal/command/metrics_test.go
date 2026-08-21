package command

import (
	"testing"

	"statusengine-worker/internal/metrics"
)

// internal/metrics cannot import this package - this package imports it -
// so the command names and reject reasons exist twice. Duplicated constants
// that nothing compares are duplicated constants that drift, and the way
// this one would fail is quiet: a new command name would simply have no
// pre-created series, so its counter would read "No data" until the first
// one arrived, which is exactly the problem the Init* helpers exist to
// prevent.
func TestMetricLabelsMatchTheCommandPackage(t *testing.T) {
	if len(metrics.CommandNames) != len(KnownCommands) {
		t.Fatalf("metrics.CommandNames = %v, KnownCommands = %v", metrics.CommandNames, KnownCommands)
	}
	for i := range KnownCommands {
		if metrics.CommandNames[i] != KnownCommands[i] {
			t.Errorf("metrics.CommandNames[%d] = %q, want %q", i, metrics.CommandNames[i], KnownCommands[i])
		}
	}

	want := []RejectReason{ReasonAuth, ReasonMalformed, ReasonUnknownCommand, ReasonDenied, ReasonTooLarge}
	if len(metrics.CommandRejectReasons) != len(want) {
		t.Fatalf("metrics.CommandRejectReasons = %v, want %v", metrics.CommandRejectReasons, want)
	}
	have := make(map[string]bool, len(metrics.CommandRejectReasons))
	for _, r := range metrics.CommandRejectReasons {
		have[r] = true
	}
	for _, reason := range want {
		if !have[string(reason)] {
			t.Errorf("reject reason %q has no pre-created metric series", reason)
		}
	}
}

// ComponentCommand must be in Components, or pipeline_errors_total for this
// component is never pre-created and an alert on it cannot fire the first
// time the broker is unreachable.
func TestCommandComponentIsRegistered(t *testing.T) {
	for _, c := range metrics.Components {
		if c == metrics.ComponentCommand {
			return
		}
	}
	t.Fatalf("metrics.ComponentCommand is not in metrics.Components (%v)", metrics.Components)
}

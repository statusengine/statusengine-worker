package version

import (
	"runtime"
	"strings"
	"testing"
)

// TestStringAlwaysNamesTheToolchain: whatever else is or is not stamped
// in, the Go version is always available at runtime, so -version can never
// come back with nothing useful. A build with no ldflags and no VCS data
// (a source tarball) is the worst case, and it still has to answer.
func TestStringAlwaysNamesTheToolchain(t *testing.T) {
	if !strings.Contains(String(), runtime.Version()) {
		t.Errorf("String() = %q, want it to contain the Go version %q", String(), runtime.Version())
	}
	if !strings.Contains(String(), Version) {
		t.Errorf("String() = %q, want it to contain the version %q", String(), Version)
	}
}

// TestRevisionPrefersLdflags: the Makefile does not set Commit, leaving the
// toolchain's VCS stamp to fill it. A release built elsewhere may set it
// explicitly, and then that value has to win rather than be silently
// replaced by whatever the build environment happened to check out.
func TestRevisionPrefersLdflags(t *testing.T) {
	original := Commit
	t.Cleanup(func() { Commit = original })

	Commit = "abcdef123456"
	if got := Revision(); got != "abcdef123456" {
		t.Errorf("Revision() = %q, want the explicitly set commit", got)
	}
}

// TestRevisionIsNeverEmpty: "unknown" is a usable answer in a log line,
// an empty string is not - it reads as a formatting bug rather than as
// missing information.
func TestRevisionIsNeverEmpty(t *testing.T) {
	original := Commit
	t.Cleanup(func() { Commit = original })

	Commit = ""
	if got := Revision(); got == "" {
		t.Error("Revision() returned an empty string; want a commit or \"unknown\"")
	}
}

// TestDefaultVersionIsNotANumber guards a small but real trap: defaulting
// to something like "0.0.0" would make an unversioned build look like a
// release in a bug report. "dev" cannot be mistaken for one.
func TestDefaultVersionIsNotANumber(t *testing.T) {
	// Only meaningful for a build that did not set it - i.e. `go test`.
	if Version != "dev" {
		t.Skipf("built with -ldflags, Version = %q", Version)
	}
	if strings.ContainsAny(Version, "0123456789") {
		t.Errorf("default Version = %q, want something that cannot be read as a release number", Version)
	}
}

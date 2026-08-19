// Package version carries the build identity of the binaries in this
// repository: what was built, from which commit, and when.
//
// It exists because "which build is running?" is not answerable from a
// running worker otherwise, and that question comes up at exactly the
// wrong moment - when production is behaving oddly and nobody is sure
// whether the fix from last week is actually deployed. A stale binary and
// a real regression look identical in the logs.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Set at build time via -ldflags, see the Makefile:
//
//	-X statusengine-worker/internal/version.Version=1.2.3
//
// The defaults are what a plain `go build` or `go run` produces. "dev" is
// deliberately not a version number: a binary that reports "dev" was built
// without the Makefile, and saying so is more useful than inventing "0.0.0"
// and having it look like a release.
var (
	Version = "dev"
	Commit  = ""
	Date    = ""
)

// commitFromBuildInfo returns the VCS revision the Go toolchain stamps
// into the binary on its own, so a plain `go build` from a git checkout
// still reports a usable commit without the Makefile. Returns "" when the
// build had no VCS information (a source tarball, or -buildvcs=false).
func commitFromBuildInfo() (rev string, dirty bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			rev = setting.Value
		case "vcs.modified":
			dirty = setting.Value == "true"
		}
	}
	return rev, dirty
}

// Revision is the commit this binary was built from: the ldflags value if
// one was set, otherwise whatever the toolchain stamped in. A "-dirty"
// suffix means the working tree had uncommitted changes, which is worth
// knowing before trusting a bug report against it.
func Revision() string {
	if Commit != "" {
		return Commit
	}
	rev, dirty := commitFromBuildInfo()
	if rev == "" {
		return "unknown"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		return rev + "-dirty"
	}
	return rev
}

// GoVersion is the toolchain that produced this binary.
func GoVersion() string { return runtime.Version() }

// String is the one-line form used by -version and the startup log.
func String() string {
	s := fmt.Sprintf("%s (commit %s, %s)", Version, Revision(), GoVersion())
	if Date != "" {
		s = fmt.Sprintf("%s (commit %s, built %s, %s)", Version, Revision(), Date, GoVersion())
	}
	return s
}

package queue

import (
	"os"
	"strconv"
	"testing"
)

// requireServicesEnv is the environment variable that turns a missing dev
// service from a skip into a failure. CI sets it; a developer's machine
// normally does not.
const requireServicesEnv = "STATUSENGINE_TEST_REQUIRE_SERVICES"

// servicesRequired reports whether a missing MySQL, Gearman or RabbitMQ
// must fail the test run rather than skip it.
//
// Any value Go's strconv.ParseBool accepts works ("1", "true", ...); an
// unset or unparseable value means "not required", so a typo cannot
// accidentally make a developer's test run fail on services they never
// installed.
func servicesRequired() bool {
	required, err := strconv.ParseBool(os.Getenv(requireServicesEnv))
	return err == nil && required
}

// skipOrFailService reports an unreachable dev service. Locally that is a
// skip - not everyone runs gearmand, RabbitMQ and MySQL to work on the
// decode logic. In CI it is a failure, and that distinction is the entire
// point of this helper.
//
// Fourteen call sites across eight files skip when a service is missing,
// and they cover the properties this worker was hardest to get right: that
// one busy queue cannot starve another, that a redelivered job does not
// duplicate rows, that Stop drains rather than drops. A CI job without
// those services would run `go test ./...`, print `ok`, and have verified
// none of it - green and meaningless, which is worse than red. So the
// pipeline sets STATUSENGINE_TEST_REQUIRE_SERVICES=1 and a broken or
// missing service container fails the build loudly.
func skipOrFailService(t *testing.T, format string, args ...any) {
	t.Helper()

	if servicesRequired() {
		t.Fatalf("%s=1 but this service is unreachable: "+format,
			append([]any{requireServicesEnv}, args...)...)
	}
	t.Skipf(format, args...)
}

package queue

import (
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
)

// repairLogInterval is how often a single queue may report repaired
// payloads. A misconfigured host emits non-UTF-8 on every check it runs,
// so logging per occurrence would bury everything else - but staying
// silent hides a problem that belongs fixed at the source, not here.
const repairLogInterval = 5 * time.Minute

var repairLog struct {
	sync.Mutex
	lastLogged map[string]time.Time
}

// logRepair reports that a queue delivered a non-UTF-8 payload, at most
// once per repairLogInterval per queue. Only ever called after a repair
// actually happened, so the lock stays off the hot path.
func logRepair(queueName string) {
	repairLog.Lock()
	defer repairLog.Unlock()

	if repairLog.lastLogged == nil {
		repairLog.lastLogged = make(map[string]time.Time)
	}
	if time.Since(repairLog.lastLogged[queueName]) < repairLogInterval {
		return
	}
	repairLog.lastLogged[queueName] = time.Now()

	slog.Warn("queue: payload was not valid UTF-8, invalid bytes read as Windows-1252",
		"queue", queueName,
		"hint", "a monitoring host is emitting non-UTF-8 plugin output; see statusengine_queue_payloads_repaired_total",
		"log_interval", repairLogInterval)
}

// repairUTF8 returns payload with any byte that is not valid UTF-8
// reinterpreted as Windows-1252, plus whether anything had to be
// changed. A payload that is already valid UTF-8 is returned unchanged,
// sharing its backing array - no copy, no allocation.
//
// Monitoring plugins on Windows commonly emit umlauts in CP1252, and
// Naemon passes that straight into the JSON payload (json-c does not
// care about encoding), so a byte like 0xFC for 'ü' arrives raw inside a
// JSON string, where only UTF-8 is legal.
//
// Go does not reject that the way PHP's json_decode does. encoding/json
// leaves its fast path on the first invalid byte (decode.go's
// unquoteBytes) and substitutes U+FFFD instead, so "Datenträger C:"
// silently becomes "Datentr<U+FFFD>ger C:" - still valid UTF-8, so MySQL
// stores it without complaint and it reaches WebSocket subscribers the
// same way. Where the legacy PHP worker repaired such a message,
// this one would quietly corrupt it. Hence repairing before parsing.
//
// The repair is deliberately per byte rather than per document. The
// legacy worker's src/JSONUTF8.php runs mb_detect_encoding and iconv
// over the whole string, which breaks as soon as one payload carries
// output from several hosts: a bulk array holding correct UTF-8 from one
// host and CP1252 from another gets the correct one mangled, because
// 'ü' (0xC3 0xBC) read as CP1252 becomes 'Ã¼'. Touching only bytes that
// are provably invalid keeps mixed payloads intact.
//
// What this cannot catch: bytes that were meant as CP1252 but happen to
// form a valid UTF-8 sequence. Detecting those would mean guessing at
// data that is already well-formed, and guessing wrong corrupts correct
// input - so the rule is to repair only what is definitely broken.
//
// Operating on raw bytes is safe here because every structural
// character in JSON is ASCII: an invalid byte can only occur inside a
// string literal, and the UTF-8 this produces for it is legal there
// unescaped.
func repairUTF8(payload []byte) ([]byte, bool) {
	if utf8.Valid(payload) {
		return payload, false
	}

	// One invalid byte becomes at most utf8.UTFMax bytes, but that bound
	// applied to the whole payload would over-allocate wildly for input
	// that is mostly fine. Repairs are rare, so start from the input
	// length plus some headroom and let append handle the rest.
	out := make([]byte, 0, len(payload)+len(payload)/8+utf8.UTFMax)

	for i := 0; i < len(payload); {
		r, size := utf8.DecodeRune(payload[i:])
		if r == utf8.RuneError && size == 1 {
			// Not valid UTF-8: read this single byte as CP1252.
			out = utf8.AppendRune(out, charmap.Windows1252.DecodeByte(payload[i]))
			i++
			continue
		}
		out = append(out, payload[i:i+size]...)
		i += size
	}

	return out, true
}

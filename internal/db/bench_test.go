package db

import (
	"strings"
	"testing"
	"time"
)

// These benchmarks are the record of why RowFunc[T] appends into a
// caller-supplied slice instead of returning its own. The older form,
//
//	func(item T) []any
//
// made every row allocate a slice that buildInsert copied out of and then
// discarded. BenchmarkBuildInsert measures the shipping code;
// BenchmarkBuildInsertReturningRow keeps the old shape alive locally so
// the comparison stays reproducible instead of living only in a commit
// message. Last measured, 5 runs each on an AMD Ryzen 7 7700X:
//
//	returning form   58700 ns/op   182053 B/op   2103 allocs/op
//	append form      41600 ns/op   111649 B/op   2003 allocs/op
//
// Note where the win is: the allocation *count* barely moves, since
// boxing each value into an any allocates either way. The bytes and the
// time are what drop.
//
// Shape is taken from the widest real table: statusengine_hoststatus has
// 40 columns of mixed strings, ints, bools and times (see newHostStatusRow
// in internal/queue/registry.go), flushed MaxBatchSize rows at a time.

const benchColumns = 40

type benchRow struct {
	name         string
	pluginOutput string
	checkCommand string
	checkPeriod  string
	nodeName     string
	timestamp    int64
	state        int
	attempt      int
	maxAttempts  int
	lastCheck    time.Time
	enabled      bool
	flapping     bool
	latency      float64
}

// benchToRowReturning is the pre-refactor shape: one fresh []any per row.
// Kept only to feed BenchmarkBuildInsertReturningRow.
func benchToRowReturning(r benchRow) []any {
	return []any{
		r.name, r.timestamp, r.pluginOutput, r.pluginOutput, r.pluginOutput, r.state,
		r.attempt, r.maxAttempts, r.lastCheck, r.lastCheck, r.enabled,
		r.lastCheck, r.lastCheck, r.state, r.enabled,
		r.lastCheck, r.lastCheck, r.enabled, r.enabled,
		r.state, r.enabled, r.enabled, r.enabled,
		r.enabled, r.flapping, r.latency, r.latency, r.state,
		r.enabled, r.enabled, r.attempt, r.maxAttempts,
		r.checkPeriod, r.nodeName, r.lastCheck, r.lastCheck, r.lastCheck,
		r.attempt, r.latency, r.checkCommand,
	}
}

// benchToRow mirrors the shape of the real RowFuncs: a mix of strings,
// ints, bools, floats and times appended into the batch's args slice.
func benchToRow(r benchRow, dst []any) []any {
	return append(dst,
		r.name, r.timestamp, r.pluginOutput, r.pluginOutput, r.pluginOutput, r.state,
		r.attempt, r.maxAttempts, r.lastCheck, r.lastCheck, r.enabled,
		r.lastCheck, r.lastCheck, r.state, r.enabled,
		r.lastCheck, r.lastCheck, r.enabled, r.enabled,
		r.state, r.enabled, r.enabled, r.enabled,
		r.enabled, r.flapping, r.latency, r.latency, r.state,
		r.enabled, r.enabled, r.attempt, r.maxAttempts,
		r.checkPeriod, r.nodeName, r.lastCheck, r.lastCheck, r.lastCheck,
		r.attempt, r.latency, r.checkCommand,
	)
}

func benchInserter(tb testing.TB) (*BulkInserter[benchRow], []benchRow) {
	tb.Helper()

	columns := make([]string, benchColumns)
	for i := range columns {
		columns[i] = "column_" + strings.Repeat("x", 1+i%12)
	}

	b := NewUpsertBulkInserter[benchRow](nil, "statusengine_hoststatus",
		columns, columns[:8], benchToRow)

	items := make([]benchRow, MaxBatchSize)
	for i := range items {
		items[i] = benchRow{
			name:         "host-with-a-reasonably-long-name.example.org",
			pluginOutput: "OK - 1 of 1 hosts up, rta 0.417ms, lost 0%",
			checkCommand: "check_host_alive!5000.0,80%!5000.0,100%",
			checkPeriod:  "24x7",
			nodeName:     "statusengine",
			timestamp:    time.Now().Unix(),
			state:        i % 4,
			attempt:      1,
			maxAttempts:  3,
			lastCheck:    time.Now(),
			enabled:      i%2 == 0,
			latency:      0.417,
		}
	}
	return b, items
}

// sanity check that the two row shapes really are equivalent, so the
// benchmark comparison below is honest.
func TestBenchRowShapesMatch(t *testing.T) {
	r := benchRow{name: "h", state: 2, enabled: true, latency: 1.5}

	direct := benchToRowReturning(r)
	appended := benchToRow(r, make([]any, 0, benchColumns))

	if len(direct) != benchColumns {
		t.Fatalf("benchToRow returned %d values, want %d", len(direct), benchColumns)
	}
	if len(appended) != len(direct) {
		t.Fatalf("append form returned %d values, want %d", len(appended), len(direct))
	}
	for i := range direct {
		if direct[i] != appended[i] {
			t.Fatalf("value %d differs: %v vs %v", i, direct[i], appended[i])
		}
	}
}

func BenchmarkBuildInsert(b *testing.B) {
	inserter, items := benchInserter(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query, args := inserter.buildInsert(items)
		_, _ = query, args
	}
}

// BenchmarkBuildInsertReturningRow is buildInsert as it looked while
// RowFunc returned a fresh slice per row.
func BenchmarkBuildInsertReturningRow(b *testing.B) {
	inserter, items := benchInserter(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query, args := buildInsertReturning(inserter, items)
		_, _ = query, args
	}
}

// buildInsertReturning is a copy of buildInsert with the single line the
// refactor changed, restored: instead of appending into args, each row
// allocates its own slice which is then copied in.
func buildInsertReturning(b *BulkInserter[benchRow], items []benchRow) (string, []any) {
	const prefix, infix = "INSERT INTO ", ") VALUES "

	columnList := strings.Join(b.columns, ", ")

	var query strings.Builder
	query.Grow(len(prefix) + len(b.table) + 2 + len(columnList) + len(infix) +
		len(items)*(len(b.rowPlaceholder)+2) + len(b.updateClause))

	query.WriteString(prefix)
	query.WriteString(b.table)
	query.WriteString(" (")
	query.WriteString(columnList)
	query.WriteString(infix)

	args := make([]any, 0, len(items)*len(b.columns))
	for i, item := range items {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(b.rowPlaceholder)
		args = append(args, benchToRowReturning(item)...)
	}

	query.WriteString(b.updateClause)

	return query.String(), args
}

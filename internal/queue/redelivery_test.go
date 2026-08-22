package queue

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"statusengine-worker/internal/graphite"
	"statusengine-worker/internal/websocket"
)

// schemaPath is the authoritative schema CLAUDE.md points at.
const schemaPath = "../../.claude/specs/mysql_schema.sql"

// tableBlock returns the body of one CREATE TABLE statement from the schema
// dump.
//
// Anchored on the backticked name and terminated at the closing paren, which
// is not cosmetic: without it statusengine_host_notifications matches
// statusengine_host_notifications_log's block, and every check built on top
// of this would then be reading the wrong table's definition.
func tableBlock(t *testing.T, schema, table string) string {
	t.Helper()

	m := regexp.MustCompile("(?s)CREATE TABLE `" + regexp.QuoteMeta(table) + "` \\((.*?)\n\\) ENGINE").
		FindStringSubmatch(schema)
	if m == nil {
		t.Fatalf("table %s not found in %s", table, schemaPath)
	}
	return m[1]
}

// firstPrimaryKeyColumn returns the first column of table's PRIMARY KEY as
// declared in the schema dump, so the tests below compare against the real
// database rather than against a second copy of the same assumption.
func firstPrimaryKeyColumn(t *testing.T, schema, table string) string {
	t.Helper()

	pk := regexp.MustCompile("PRIMARY KEY \\(([^)]*)\\)").FindStringSubmatch(tableBlock(t, schema, table))
	if pk == nil {
		t.Fatalf("table %s has no PRIMARY KEY in %s", table, schemaPath)
	}

	columns := regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(pk[1], -1)
	if len(columns) == 0 {
		t.Fatalf("could not parse PRIMARY KEY of %s: %q", table, pk[1])
	}
	return columns[0][1]
}

func readSchema(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		skipOrFailService(t, "schema dump unavailable: %v", err)
	}
	return string(raw)
}

// TestRedeliverySafePKColumnsMatchSchema is the test that actually protects
// the fix. The ON DUPLICATE KEY UPDATE clause is only a no-op while the named
// column is the first column of the PRIMARY KEY - name any other column and
// every redelivered row turns into a real write instead of being skipped.
func TestRedeliverySafePKColumnsMatchSchema(t *testing.T) {
	schema := readSchema(t)

	for table, column := range redeliverySafePKColumn {
		want := firstPrimaryKeyColumn(t, schema, table)
		if column != want {
			t.Errorf("%s: declared %q, but the PRIMARY KEY starts with %q",
				table, column, want)
		}
	}
}

// TestRedeliverySafeTablesAreExactlyTheExpectedSet pins the membership of the
// map. Adding a table here without a natural PRIMARY KEY, or dropping one that
// has one, both silently change which flushes survive a redelivery.
func TestRedeliverySafeTablesAreExactlyTheExpectedSet(t *testing.T) {
	want := []string{
		"statusengine_host_acknowledgements",
		"statusengine_host_notifications",
		"statusengine_host_notifications_log",
		"statusengine_host_statehistory",
		"statusengine_hostchecks",
		"statusengine_service_acknowledgements",
		"statusengine_service_notifications",
		"statusengine_service_notifications_log",
		"statusengine_service_statehistory",
		"statusengine_servicechecks",
	}

	got := make([]string, 0, len(redeliverySafePKColumn))
	for table := range redeliverySafePKColumn {
		got = append(got, table)
	}
	sort.Strings(got)

	if len(got) != len(want) {
		t.Fatalf("redeliverySafePKColumn has %d tables, want %d:\ngot  %v\nwant %v",
			len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("table %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

// TestExcludedTablesReallyCannotCollide justifies the two omissions rather
// than merely asserting them: statusengine_logentries is keyed on an
// AUTO_INCREMENT id and statusengine_perfdata has no PRIMARY KEY at all, so
// neither can raise Error 1062 on a redelivery - it duplicates rows silently
// instead. If a schema change ever gave them a natural key, this test fails
// and the table belongs in the map above.
func TestExcludedTablesReallyCannotCollide(t *testing.T) {
	schema := readSchema(t)

	logentries := tableBlock(t, schema, "statusengine_logentries")
	if !regexp.MustCompile("(?i)AUTO_INCREMENT").MatchString(logentries) {
		t.Error("statusengine_logentries no longer has an AUTO_INCREMENT key - a redelivery can now collide, so it belongs in redeliverySafePKColumn")
	}

	perfdata := tableBlock(t, schema, "statusengine_perfdata")
	if regexp.MustCompile("PRIMARY KEY").MatchString(perfdata) {
		t.Error("statusengine_perfdata now has a PRIMARY KEY - a redelivery can now collide, so it belongs in redeliverySafePKColumn")
	}
}

// TestNewRouterEmitsUpsertForCheckTables is the end-to-end proof: a real
// hostchecks payload goes through the Router's Handler into the BulkInserter,
// and the statement that reaches MySQL must carry the ON DUPLICATE KEY UPDATE
// clause. Asserting on the map alone would not catch a constructor that was
// left on plain db.NewBulkInserter.
func TestNewRouterEmitsUpsertForCheckTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec("INSERT INTO statusengine_hostchecks .* ON DUPLICATE KEY UPDATE hostname = VALUES\\(hostname\\)").
		WillReturnResult(sqlmock.NewResult(0, 1))

	hub := websocket.NewHub()
	router, runners := NewRouter(mockDB, hub, graphite.NewClient("127.0.0.1:2003"),
		PerfdataRouteMySQL, "statusengine-test", "statusengine-test", false, noAgeFilter, testBatchSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, r := range runners {
		go r.Run(ctx)
	}

	payload := []byte(`{"messages":[{"timestamp_usec":1,"hostcheck":{"host_name":"localhost","start_time":1,"end_time":2}}],"format":"none"}`)
	if err := router[QueueHostChecks](ctx, payload); err != nil {
		t.Fatalf("hostchecks handler: %v", err)
	}

	// The 250ms ticker triggers the flush; one event never reaches the
	// 100-row batch threshold.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := mock.ExpectationsWereMet(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no upsert statement reached MySQL: %v", mock.ExpectationsWereMet())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestNewRouterPanicsOnUndeclaredTable documents the guard in
// newRedeliverySafeInserter: a table wired up without an entry in the map must
// fail loudly at construction, not degrade to a plain INSERT that loses a
// batch on the next redelivery.
func TestNewRouterPanicsOnUndeclaredTable(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic for a table missing from redeliverySafePKColumn")
		}
	}()

	// A DSN is never dialed here - the constructor only builds structs.
	sqlDB, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/db")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer sqlDB.Close()

	newRedeliverySafeInserter(sqlDB, "statusengine_not_declared",
		[]string{"a"}, func(_ int, dst []any) []any { return append(dst, 1) })
}

// upsertInserter is what every BulkInserter written as an upsert satisfies.
// An interface rather than a concrete type because runners is []Runner and
// the inserters have different type parameters - the same reason
// batchsize_test.go declares sizedInserter.
type upsertInserter interface {
	Table() string
	IsUpsert() bool
}

// TestUpsertTablesHaveNoSecondaryUniqueIndex closes the one assumption rule 6
// argues from without ever stating it.
//
// The redelivery fix rests on ON DUPLICATE KEY UPDATE naming the first column
// of the PRIMARY KEY, which makes the update a genuine no-op: the row was only
// matched because that column is already equal. That holds exactly as long as
// the PRIMARY KEY is the *only* unique index on the table, because the clause
// fires on a violation of ANY unique index, not just the primary one.
//
// Give one of these tables a secondary UNIQUE index and the row that gets
// updated is the one that index matched - a different row than the primary key
// identifies. Two things then happen at once, both without an error and
// without a log line: the new row is not inserted at all (a real event is
// lost), and an unrelated existing row has its primary-key column overwritten
// with the new row's value. For statusengine_hoststatus and
// statusengine_servicestatus it is worse still, since their update clause
// covers every data column: one object's status would be written over
// another's. MySQL itself does not define which row is matched when several
// unique indexes are.
//
// The schema carries no UNIQUE index today, so this test is green because
// there is nothing there - which is precisely why it has to exist before one
// appears. Adding an index is an ordinary schema change that nobody would
// connect to this file.
//
// The table list is derived rather than written down: NewRouter's own
// inserters report whether they are upserts, so a table added later is covered
// without a second list to keep in sync. The four downtime tables are appended
// because they cannot be reached that way - they bypass BulkInserter entirely
// but still write through db.buildDowntimeUpsert.
func TestUpsertTablesHaveNoSecondaryUniqueIndex(t *testing.T) {
	schema := readSchema(t)

	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	hub := websocket.NewHub()
	_, runners := NewRouter(mockDB, hub, graphite.NewClient("127.0.0.1:2003"),
		PerfdataRouteMySQL, "statusengine-test", "statusengine-test", false, noAgeFilter, testBatchSize)

	tables := make([]string, 0, 16)
	for _, r := range runners {
		// The Graphite client is a Runner too, and logentries/perfdata are
		// plain inserts - TestExcludedTablesReallyCannotCollide covers why
		// those two are allowed to be.
		bi, ok := r.(upsertInserter)
		if !ok || !bi.IsUpsert() {
			continue
		}
		tables = append(tables, bi.Table())
	}
	tables = append(tables, downtimeMetricsTables()...)

	// A count, for the same reason batchsize_test.go asserts one: if the
	// derivation above ever stops finding anything, every assertion below
	// becomes vacuous and the test stays green while checking nothing.
	const wantTables = 16 // 10 redelivery-safe + 2 status + 4 downtime
	if len(tables) != wantTables {
		t.Fatalf("derived %d upsert tables, want %d: %v", len(tables), wantTables, tables)
	}

	unique := regexp.MustCompile("(?i)UNIQUE\\s+(KEY|INDEX)")
	for _, table := range tables {
		if m := unique.FindString(tableBlock(t, schema, table)); m != "" {
			t.Errorf("%s has a %s: its ON DUPLICATE KEY UPDATE can now match a row the PRIMARY KEY does not "+
				"identify, which silently drops the incoming row and overwrites an unrelated one",
				table, m)
		}
	}
}

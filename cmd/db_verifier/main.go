// Command db_verifier is a shadow-testing comparison tool: it connects to
// two MySQL databases fed by the legacy PHP Statusengine Worker and this Go
// rewrite respectively (see CLAUDE.md's "Legacy Reference"), both consuming
// the exact same event stream, and diffs their most recent rows table by
// table, column by column, to prove the two pipelines persist byte-for-byte
// identical data. It never writes to either database - read-only, safe to
// run against live/production data.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ANSI colors for the terminal SUCCESS/MISMATCH lines.
const (
	colorGreen = "\033[32m"
	colorRed   = "\033[31m"
	colorReset = "\033[0m"
)

// tableSpec describes how to page through a table's most recent rows and
// which columns uniquely identify a row across both databases: pkColumns
// must match the table's actual PRIMARY KEY (see .claude/specs/mysql_schema.sql)
// so that a row found in one database can be matched against its counterpart
// in the other, and orderBy picks the columns that make "most recent" mean
// something for that table (usually the PK's own time columns).
type tableSpec struct {
	pkColumns []string
	orderBy   []string
}

// tableSpecs covers every table in .claude/specs/mysql_schema.sql, including
// the four (statusengine_dbversion, statusengine_nodes, statusengine_tasks,
// statusengine_users) this worker's pipeline never writes to itself - a
// shadow-test run showing those as identical (or flagging a real gap) is
// still useful signal, so they're included rather than assumed irrelevant.
// None of those four - nor statusengine_perfdata - have a real PRIMARY KEY
// in the schema, so their pkColumns below are a best-effort composite of
// columns that make a row unique in practice, not a documented PK.
var tableSpecs = map[string]tableSpec{
	"statusengine_hoststatus": {
		pkColumns: []string{"hostname"},
		orderBy:   []string{"status_update_time"},
	},
	"statusengine_servicestatus": {
		pkColumns: []string{"hostname", "service_description"},
		orderBy:   []string{"status_update_time"},
	},
	"statusengine_hostchecks": {
		pkColumns: []string{"hostname", "start_time", "start_time_usec"},
		orderBy:   []string{"start_time", "start_time_usec"},
	},
	"statusengine_servicechecks": {
		pkColumns: []string{"service_description", "start_time", "start_time_usec"},
		orderBy:   []string{"start_time", "start_time_usec"},
	},
	"statusengine_host_statehistory": {
		pkColumns: []string{"hostname", "state_time", "state_time_usec"},
		orderBy:   []string{"state_time", "state_time_usec"},
	},
	"statusengine_service_statehistory": {
		pkColumns: []string{"service_description", "state_time", "state_time_usec"},
		orderBy:   []string{"state_time", "state_time_usec"},
	},
	"statusengine_host_downtimehistory": {
		pkColumns: []string{"hostname", "node_name", "scheduled_start_time", "internal_downtime_id"},
		orderBy:   []string{"entry_time", "entry_time_usec"},
	},
	"statusengine_service_downtimehistory": {
		pkColumns: []string{"hostname", "service_description", "node_name", "scheduled_start_time", "internal_downtime_id"},
		orderBy:   []string{"entry_time", "entry_time_usec"},
	},
	"statusengine_host_notifications": {
		pkColumns: []string{"hostname", "start_time", "start_time_usec"},
		orderBy:   []string{"start_time", "start_time_usec"},
	},
	"statusengine_host_notifications_log": {
		pkColumns: []string{"hostname", "start_time", "start_time_usec"},
		orderBy:   []string{"start_time", "start_time_usec"},
	},
	"statusengine_service_notifications": {
		pkColumns: []string{"service_description", "start_time", "start_time_usec"},
		orderBy:   []string{"start_time", "start_time_usec"},
	},
	"statusengine_service_notifications_log": {
		pkColumns: []string{"hostname", "service_description", "start_time", "start_time_usec"},
		orderBy:   []string{"start_time", "start_time_usec"},
	},
	"statusengine_host_acknowledgements": {
		pkColumns: []string{"hostname", "entry_time", "entry_time_usec"},
		orderBy:   []string{"entry_time", "entry_time_usec"},
	},
	"statusengine_service_acknowledgements": {
		pkColumns: []string{"service_description", "entry_time", "entry_time_usec"},
		orderBy:   []string{"entry_time", "entry_time_usec"},
	},
	"statusengine_host_scheduleddowntimes": {
		pkColumns: []string{"hostname", "node_name", "scheduled_start_time", "internal_downtime_id"},
		orderBy:   []string{"entry_time"},
	},
	"statusengine_service_scheduleddowntimes": {
		pkColumns: []string{"hostname", "service_description", "node_name", "scheduled_start_time", "internal_downtime_id"},
		orderBy:   []string{"entry_time"},
	},
	"statusengine_logentries": {
		pkColumns: []string{"id", "entry_time"},
		orderBy:   []string{"entry_time", "id"},
	},
	"statusengine_dbversion": {
		pkColumns: []string{"id"},
		orderBy:   []string{"id"},
	},
	"statusengine_nodes": {
		pkColumns: []string{"node_name"},
		orderBy:   []string{"node_name"},
	},
	"statusengine_perfdata": {
		pkColumns: []string{"hostname", "service_description", "label", "timestamp", "timestamp_unix"},
		orderBy:   []string{"timestamp_unix"},
	},
	"statusengine_tasks": {
		pkColumns: []string{"uuid", "entry_time"},
		orderBy:   []string{"entry_time"},
	},
	"statusengine_users": {
		pkColumns: []string{"username"},
		orderBy:   []string{"username"},
	},
}

// defaultTables intentionally excludes statusengine_dbversion,
// statusengine_nodes, statusengine_perfdata, statusengine_tasks and
// statusengine_users from the default run; -tables can still name any of
// those five explicitly (they stay in tableSpecs).
var defaultTables = []string{
	"statusengine_hoststatus",
	"statusengine_servicestatus",
	"statusengine_hostchecks",
	"statusengine_servicechecks",
	"statusengine_host_statehistory",
	"statusengine_service_statehistory",
	"statusengine_host_downtimehistory",
	"statusengine_service_downtimehistory",
	"statusengine_host_notifications",
	"statusengine_host_notifications_log",
	"statusengine_service_notifications",
	"statusengine_service_notifications_log",
	"statusengine_host_acknowledgements",
	"statusengine_service_acknowledgements",
	"statusengine_host_scheduleddowntimes",
	"statusengine_service_scheduleddowntimes",
	"statusengine_logentries",
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	dsnPHP := flag.String("dsn-php", "", "MySQL DSN of the legacy PHP worker's database (go-sql-driver/mysql format, e.g. user:pass@tcp(127.0.0.1:3306)/statusengine_php)")
	dsnGo := flag.String("dsn-go", "", "MySQL DSN of this Go worker's database")
	tablesFlag := flag.String("tables", strings.Join(defaultTables, ","), "comma-separated list of tables to verify (default: all Status/Check/History/Notification/Acknowledgement/Downtime/Logentries tables)")
	limit := flag.Int("limit", 5000, "maximum number of most-recent rows to compare per table")
	flag.Parse()

	if *dsnPHP == "" || *dsnGo == "" {
		fatal("-dsn-php and -dsn-go are both required")
	}
	if *limit < 1 {
		fatal("-limit must be >= 1", "limit", *limit)
	}

	tables, err := resolveTables(*tablesFlag)
	if err != nil {
		fatal("invalid -tables", "error", err)
	}

	ctx := context.Background()

	phpDB, err := openAndPing(ctx, *dsnPHP)
	if err != nil {
		fatal("php database unreachable", "error", err)
	}
	defer phpDB.Close()

	goDB, err := openAndPing(ctx, *dsnGo)
	if err != nil {
		fatal("go database unreachable", "error", err)
	}
	defer goDB.Close()

	var (
		totalMismatches int
		tablesFailed    int
	)
	for _, table := range tables {
		mismatches, rowsVerified, err := verifyTable(ctx, phpDB, goDB, table, tableSpecs[table], *limit)
		if err != nil {
			fmt.Printf("%sERROR verifying table %s: %v%s\n", colorRed, table, err, colorReset)
			tablesFailed++
			continue
		}
		if len(mismatches) == 0 {
			fmt.Printf("%sSUCCESS: Table %s is 100%% identical [%d rows verified]%s\n", colorGreen, table, rowsVerified, colorReset)
			continue
		}
		for _, m := range mismatches {
			fmt.Printf("%sMISMATCH in Table %s, PK %s, Column %s: PHP='%s' vs GO='%s'%s\n",
				colorRed, table, m.pk, m.column, m.phpValue, m.goValue, colorReset)
		}
		totalMismatches += len(mismatches)
	}

	if totalMismatches > 0 || tablesFailed > 0 {
		os.Exit(1)
	}
}

// resolveTables splits csv into a sorted, deduplicated table list and
// validates every name against tableSpecs, so a typo fails fast instead of
// silently comparing nothing.
func resolveTables(csv string) ([]string, error) {
	seen := make(map[string]bool)
	var tables []string
	for _, raw := range strings.Split(csv, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, ok := tableSpecs[name]; !ok {
			known := make([]string, 0, len(tableSpecs))
			for k := range tableSpecs {
				known = append(known, k)
			}
			sort.Strings(known)
			return nil, fmt.Errorf("unknown table %q, known tables: %s", name, strings.Join(known, ", "))
		}
		if !seen[name] {
			seen[name] = true
			tables = append(tables, name)
		}
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables given")
	}
	sort.Strings(tables)
	return tables, nil
}

func openAndPing(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return db, nil
}

// mismatch is one column-level or row-level difference found in a table.
type mismatch struct {
	pk       string
	column   string
	phpValue string
	goValue  string
}

// row is one fetched table row, keyed by column name, read generically so
// this tool works against any table in tableSpecs without a struct per
// table.
type row map[string]sql.NullString

// verifyTable fetches the spec.orderBy-most-recent limit rows from both
// databases, matches them up by spec.pkColumns, and compares every shared
// column. It returns every mismatch found (empty if the tables are
// identical) plus how many PK rows were present and identical in both.
func verifyTable(ctx context.Context, phpDB, goDB *sql.DB, table string, spec tableSpec, limit int) ([]mismatch, int, error) {
	phpRows, columns, err := fetchLatestRows(ctx, phpDB, table, spec, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch from php db: %w", err)
	}
	goRows, _, err := fetchLatestRows(ctx, goDB, table, spec, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch from go db: %w", err)
	}

	// Both databases mirror the same schema (see .claude/specs/mysql_schema.sql),
	// so it's enough to ask either one which columns are DOUBLE/FLOAT.
	floatCols, err := floatColumns(ctx, phpDB, table)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve float columns for %s: %w", table, err)
	}

	var mismatches []mismatch
	rowsVerified := 0

	pks := unionKeys(phpRows, goRows)
	for _, pk := range pks {
		phpRow, inPHP := phpRows[pk]
		goRow, inGo := goRows[pk]

		switch {
		case inPHP && !inGo:
			mismatches = append(mismatches, mismatch{pk: pk, column: "(row)", phpValue: "present", goValue: "missing"})
			continue
		case !inPHP && inGo:
			mismatches = append(mismatches, mismatch{pk: pk, column: "(row)", phpValue: "missing", goValue: "present"})
			continue
		}

		rowIdentical := true
		for _, col := range columns {
			if valuesEqual(phpRow[col], goRow[col], floatCols[col]) {
				continue
			}
			mismatches = append(mismatches, mismatch{
				pk:       pk,
				column:   col,
				phpValue: displayValue(phpRow[col]),
				goValue:  displayValue(goRow[col]),
			})
			rowIdentical = false
		}
		if rowIdentical {
			rowsVerified++
		}
	}

	return mismatches, rowsVerified, nil
}

// fetchLatestRows reads the spec.orderBy-most-recent limit rows from db,
// returning them keyed by their spec.pkColumns values joined with "|" (safe
// as a map key since every PK column here is either numeric or a
// hostname/service_description that MySQL itself already treats as an
// identifier boundary), plus the full ordered column list of the table as
// MySQL reports it.
func fetchLatestRows(ctx context.Context, db *sql.DB, table string, spec tableSpec, limit int) (map[string]row, []string, error) {
	orderBy := strings.Join(spec.orderBy, " DESC, ") + " DESC"
	query := fmt.Sprintf("SELECT * FROM `%s` ORDER BY %s LIMIT ?", table, orderBy)

	rows, err := db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("query %s: %w", table, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("columns: %w", err)
	}

	result := make(map[string]row)
	for rows.Next() {
		scanTargets := make([]any, len(columns))
		values := make([]sql.NullString, len(columns))
		for i := range values {
			scanTargets[i] = &values[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, nil, fmt.Errorf("scan %s: %w", table, err)
		}

		r := make(row, len(columns))
		for i, col := range columns {
			r[col] = values[i]
		}

		pk, err := pkKey(r, spec.pkColumns)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", table, err)
		}
		result[pk] = r
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate %s: %w", table, err)
	}
	return result, columns, nil
}

func pkKey(r row, pkColumns []string) (string, error) {
	parts := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		val, ok := r[col]
		if !ok {
			return "", fmt.Errorf("primary key column %q not found in result set", col)
		}
		parts[i] = displayValue(val)
	}
	return strings.Join(parts, "|"), nil
}

// unionKeys returns the sorted union of both maps' keys, so mismatch output
// order is stable across runs instead of depending on Go's randomized map
// iteration.
func unionKeys(a, b map[string]row) []string {
	seen := make(map[string]bool, len(a)+len(b))
	keys := make([]string, 0, len(a)+len(b))
	for k := range a {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for k := range b {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// normalize collapses the semantic subtleties CLAUDE.md's two pipelines are
// known to differ on without it being a real data bug: PHP's mysqli driver
// and Go's database/sql driver don't always agree on NULL vs. an empty
// string for optional varchar columns (e.g. author_name, comment_data), so
// a SQL NULL and a zero-length string compare as equal here.
func normalize(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

// phpFloatPrecision matches PHP's default `precision` ini setting: PHP's
// float-to-string conversion (what mysqli sends over the wire for a bound
// float parameter) keeps only this many significant decimal digits. Go's
// driver, in contrast, returns MySQL's full round-tripped float64 text for
// a DOUBLE column. So the same event, persisted correctly by both workers,
// legitimately ends up stored as two different-looking (but equally
// correct) DOUBLE values - e.g. PHP's `0.145022` vs Go's
// `0.14502199999999998`. Rounding both to this same precision before
// comparing is what makes valuesEqual treat them as identical instead of
// reporting a false MISMATCH.
const phpFloatPrecision = 14

// roundLikePHP formats f at phpFloatPrecision significant digits, the same
// way PHP's string cast would when it originally wrote this value.
func roundLikePHP(f float64) string {
	return strconv.FormatFloat(f, 'g', phpFloatPrecision, 64)
}

// valuesEqual compares one column's PHP-side and Go-side value. Non-float
// columns (and float columns that fail to parse, e.g. because both sides
// are NULL) are compared exactly via normalize. Float columns get an extra
// chance: if the exact strings differ only because of the phpFloatPrecision
// rounding noise described above, they still count as equal.
func valuesEqual(phpVal, goVal sql.NullString, isFloatCol bool) bool {
	a, b := normalize(phpVal), normalize(goVal)
	if a == b {
		return true
	}
	if !isFloatCol {
		return false
	}
	af, aErr := strconv.ParseFloat(a, 64)
	bf, bErr := strconv.ParseFloat(b, 64)
	if aErr != nil || bErr != nil {
		return false
	}
	return roundLikePHP(af) == roundLikePHP(bf)
}

// floatColumns asks db which of table's columns are DOUBLE/FLOAT, so
// valuesEqual knows which columns are eligible for PHP-precision rounding.
// Determined dynamically via INFORMATION_SCHEMA rather than a hardcoded
// column list, so it stays correct even if the schema changes.
func floatColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND DATA_TYPE IN ('double', 'float')",
		table)
	if err != nil {
		return nil, fmt.Errorf("query information_schema for %s: %w", table, err)
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan information_schema for %s: %w", table, err)
		}
		cols[col] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate information_schema for %s: %w", table, err)
	}
	return cols, nil
}

// displayValue renders v the way a human reads a MISMATCH line: SQL NULL as
// the literal "NULL" (distinguishable from an empty string) even though
// normalize treats the two as equal for comparison purposes.
func displayValue(v sql.NullString) string {
	if !v.Valid {
		return "NULL"
	}
	return v.String
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

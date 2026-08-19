// Command db_cleanup enforces data retention on the Statusengine MySQL
// database: it deletes rows older than a configurable number of days from
// the history tables the worker keeps appending to, which would otherwise
// grow without bound (statusengine_servicechecks and statusengine_perfdata
// by orders of magnitude faster than the rest).
//
// It is a one-shot tool meant for cron or a systemd timer - it starts,
// works through every table once, and exits. The legacy PHP worker's
// equivalent is "bin/Console.php cleanup", and the retention settings use
// that worker's config key names (age_hostchecks, age_host_statehistory,
// ...) so an existing config can be carried over value for value,
// including its convention that 0 disables cleanup of a table.
//
// In a clustered setup, run this on exactly one node - or on several at
// clearly different times. Two simultaneous runs are not dangerous, but
// they compete for the same locks and finish no sooner.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"statusengine-worker/internal/cleanup"
	"statusengine-worker/internal/version"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

// pingTimeout bounds the initial connectivity check. The cleanup run
// itself is deliberately unbounded - a first run against a database that
// has never been cleaned can legitimately take a long time, and cutting
// it off would just mean it never finishes. A timer that needs a limit
// should use systemd's own TimeoutStopSec and send SIGTERM, which this
// tool handles cleanly between batches.
const pingTimeout = 5 * time.Second

// config holds the resolved settings for one run.
type config struct {
	configFile string
	mysqlDSN   string
	batchSize  int
	batchPause time.Duration
	logLevel   string
	logFormat  string

	tables []cleanup.Table
}

// fileConfig mirrors config for the YAML file named by -config. This is
// the same file cmd/app reads: both binaries decode it with a plain
// yaml.Unmarshal into their own struct, so each simply ignores the other's
// keys. That is why mysql_dsn, log_level and log_format can be shared here
// while gearman_addr and friends are silently skipped.
//
// The fourteen age fields are *int rather than int for a reason that is
// specific to retention: 0 is a meaningful value ("never clean this
// table"), so it cannot double as the "key absent from the file" sentinel
// the way it does for cmd/app's counts. nil is the only thing that means
// absent - the same trick cmd/app uses for enable_openitcockpit_tweaks.
type fileConfig struct {
	MySQLDSN  string `yaml:"mysql_dsn"`
	LogLevel  string `yaml:"log_level"`
	LogFormat string `yaml:"log_format"`

	CleanupBatchSize  int    `yaml:"cleanup_batch_size"`
	CleanupBatchPause string `yaml:"cleanup_batch_pause"`

	AgeHostchecks              *int `yaml:"age_hostchecks"`
	AgeServicechecks           *int `yaml:"age_servicechecks"`
	AgeHostAcknowledgements    *int `yaml:"age_host_acknowledgements"`
	AgeServiceAcknowledgements *int `yaml:"age_service_acknowledgements"`
	AgeHostNotifications       *int `yaml:"age_host_notifications"`
	AgeServiceNotifications    *int `yaml:"age_service_notifications"`
	AgeHostNotificationsLog    *int `yaml:"age_host_notifications_log"`
	AgeServiceNotificationsLog *int `yaml:"age_service_notifications_log"`
	AgeHostStatehistory        *int `yaml:"age_host_statehistory"`
	AgeServiceStatehistory     *int `yaml:"age_service_statehistory"`
	AgeHostDowntimes           *int `yaml:"age_host_downtimes"`
	AgeServiceDowntimes        *int `yaml:"age_service_downtimes"`
	AgeLogentries              *int `yaml:"age_logentries"`
	AgePerfdata                *int `yaml:"age_perfdata"`
}

// retentionTable ties one retention setting to the table and column it
// governs. Keeping the four facts (flag name, table, column, default) in
// one row rather than spreading them across fourteen flag declarations and
// fourteen resolve calls is what makes it possible to check at a glance
// that age_host_downtimes really does clean statusengine_host_downtimehistory.
type retentionTable struct {
	// flagName is the CLI flag without its leading dash. The YAML key and
	// the environment variable are derived from it (see yamlKey/envKey),
	// so the three can never drift apart.
	flagName string
	table    string
	column   string
	def      int

	// file extracts this setting's value from a parsed config file.
	file func(fileConfig) *int

	// days is the resolved value, filled in by loadConfig.
	days int
}

func (rt retentionTable) yamlKey() string {
	return strings.ReplaceAll(rt.flagName, "-", "_")
}

func (rt retentionTable) envKey() string {
	return "STATUSENGINE_" + strings.ToUpper(rt.yamlKey())
}

// retentionTables lists every table this tool cleans, in the order it
// processes them, with the legacy PHP worker's config key names and
// defaults.
//
// Deliberately absent: statusengine_host_scheduleddowntimes and
// statusengine_service_scheduleddowntimes hold currently active downtimes
// rather than history, so deleting old rows there would cancel running
// downtimes; statusengine_hoststatus/statusengine_servicestatus hold one
// row per object, not a growing log; and statusengine_tasks/_users/_nodes/
// _dbversion are not written by this worker at all.
func retentionTables() []retentionTable {
	return []retentionTable{
		{
			flagName: "age-hostchecks", def: 5,
			table: "statusengine_hostchecks", column: "start_time",
			file: func(fc fileConfig) *int { return fc.AgeHostchecks },
		},
		{
			flagName: "age-servicechecks", def: 5,
			table: "statusengine_servicechecks", column: "start_time",
			file: func(fc fileConfig) *int { return fc.AgeServicechecks },
		},
		{
			flagName: "age-host-acknowledgements", def: 60,
			table: "statusengine_host_acknowledgements", column: "entry_time",
			file: func(fc fileConfig) *int { return fc.AgeHostAcknowledgements },
		},
		{
			flagName: "age-service-acknowledgements", def: 60,
			table: "statusengine_service_acknowledgements", column: "entry_time",
			file: func(fc fileConfig) *int { return fc.AgeServiceAcknowledgements },
		},
		{
			flagName: "age-host-notifications", def: 60,
			table: "statusengine_host_notifications", column: "start_time",
			file: func(fc fileConfig) *int { return fc.AgeHostNotifications },
		},
		{
			flagName: "age-service-notifications", def: 60,
			table: "statusengine_service_notifications", column: "start_time",
			file: func(fc fileConfig) *int { return fc.AgeServiceNotifications },
		},
		{
			flagName: "age-host-notifications-log", def: 60,
			table: "statusengine_host_notifications_log", column: "start_time",
			file: func(fc fileConfig) *int { return fc.AgeHostNotificationsLog },
		},
		{
			flagName: "age-service-notifications-log", def: 60,
			table: "statusengine_service_notifications_log", column: "start_time",
			file: func(fc fileConfig) *int { return fc.AgeServiceNotificationsLog },
		},
		{
			flagName: "age-host-statehistory", def: 365,
			table: "statusengine_host_statehistory", column: "state_time",
			file: func(fc fileConfig) *int { return fc.AgeHostStatehistory },
		},
		{
			flagName: "age-service-statehistory", def: 365,
			table: "statusengine_service_statehistory", column: "state_time",
			file: func(fc fileConfig) *int { return fc.AgeServiceStatehistory },
		},
		{
			// Named "downtimes" rather than "downtimehistory" to match
			// the legacy config key; it cleans the history table, not
			// statusengine_host_scheduleddowntimes.
			flagName: "age-host-downtimes", def: 60,
			table: "statusengine_host_downtimehistory", column: "entry_time",
			file: func(fc fileConfig) *int { return fc.AgeHostDowntimes },
		},
		{
			flagName: "age-service-downtimes", def: 60,
			table: "statusengine_service_downtimehistory", column: "entry_time",
			file: func(fc fileConfig) *int { return fc.AgeServiceDowntimes },
		},
		{
			flagName: "age-logentries", def: 5,
			table: "statusengine_logentries", column: "entry_time",
			file: func(fc fileConfig) *int { return fc.AgeLogentries },
		},
		{
			flagName: "age-perfdata", def: 90,
			table: "statusengine_perfdata", column: "timestamp_unix",
			file: func(fc fileConfig) *int { return fc.AgePerfdata },
		},
	}
}

// resolveString applies the flag > env > file > default precedence for one
// string setting, matching cmd/app's helper of the same name. flagVal is
// the flag's value after Parse() - if flagName wasn't passed explicitly
// (checked via explicit, built from flag.Visit), it already holds that
// flag's hardcoded default, so it doubles as the final fallback.
func resolveString(explicit map[string]bool, flagName, flagVal, envKey, fileVal string) string {
	if explicit[flagName] {
		return flagVal
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	if fileVal != "" {
		return fileVal
	}
	return flagVal
}

// resolveInt is resolveString's counterpart for settings where 0 is not a
// meaningful value, so a file value of 0 can be read as "not set".
func resolveInt(explicit map[string]bool, flagName string, flagVal int, envKey string, fileVal int) (int, error) {
	if explicit[flagName] {
		return flagVal, nil
	}
	if v, ok := os.LookupEnv(envKey); ok {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("%s=%q is not a number", envKey, v)
		}
		return parsed, nil
	}
	if fileVal != 0 {
		return fileVal, nil
	}
	return flagVal, nil
}

// resolveOptionalInt is resolveInt for settings where 0 is meaningful
// rather than absent. The retention ages are exactly that: 0 means "never
// clean this table", so a file value of 0 has to beat the built-in
// default instead of being mistaken for a missing key. Hence the *int -
// nil is the only thing that means absent.
//
// Unlike cmd/app's resolveInt, an unparseable environment variable is an
// error rather than a silent fall-through to the default. Silently
// ignoring a typo in STATUSENGINE_AGE_HOSTCHECKS would mean deleting five
// days of check results when the operator meant to keep sixty, and data
// that is gone cannot be recovered from a log line nobody read.
func resolveOptionalInt(explicit map[string]bool, flagName string, flagVal int, envKey string, fileVal *int) (int, error) {
	if explicit[flagName] {
		return flagVal, nil
	}
	if v, ok := os.LookupEnv(envKey); ok {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("%s=%q is not a number", envKey, v)
		}
		return parsed, nil
	}
	if fileVal != nil {
		return *fileVal, nil
	}
	return flagVal, nil
}

// resolveDuration is the same precedence chain for a time.Duration, parsed
// with time.ParseDuration ("50ms", "1s", "0s"). An unparseable value is an
// error rather than a silent fallback, for the same reason as above.
func resolveDuration(explicit map[string]bool, flagName string, flagVal time.Duration, envKey, fileVal string) (time.Duration, error) {
	if explicit[flagName] {
		return flagVal, nil
	}
	if v, ok := os.LookupEnv(envKey); ok {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("%s=%q is not a duration (try \"50ms\")", envKey, v)
		}
		return parsed, nil
	}
	if fileVal != "" {
		parsed, err := time.ParseDuration(fileVal)
		if err != nil {
			return 0, fmt.Errorf("%q is not a duration (try \"50ms\")", fileVal)
		}
		return parsed, nil
	}
	return flagVal, nil
}

// loadFileConfig reads and parses the optional YAML config file. Called
// only when -config/STATUSENGINE_CONFIG names a path, so a missing file
// and invalid YAML are both fatal - falling back to defaults would leave a
// typo'd path looking like it worked, and here that means deleting with
// the built-in retention instead of the configured one.
func loadFileConfig(path string) fileConfig {
	data, err := os.ReadFile(path)
	if err != nil {
		fatal("config: failed to read -config file", "path", path, "error", err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		fatal("config: failed to parse -config file", "path", path, "error", err)
	}
	return fc
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() config {
	cfg := config{}
	tables := retentionTables()

	flag.StringVar(&cfg.configFile, "config", envOrDefault("STATUSENGINE_CONFIG", ""),
		"path to an optional YAML config file (the same file cmd/app reads; see config.example.yaml); "+
			"flags and environment variables still take precedence over anything set here")
	flag.StringVar(&cfg.mysqlDSN, "mysql-dsn", "statusengine-dev:statusengine-dev@tcp(127.0.0.1:3306)/statusengine-dev?parseTime=true",
		"MySQL data source name")
	flag.IntVar(&cfg.batchSize, "cleanup-batch-size", 5000,
		"rows deleted per statement; each batch is its own transaction, so smaller values are gentler "+
			"on locks and replication at the cost of more round-trips")
	flag.DurationVar(&cfg.batchPause, "cleanup-batch-pause", 0,
		`pause between two batches of the same table, e.g. "50ms"; gives a busy database room to breathe`)
	flag.StringVar(&cfg.logLevel, "log-level", "info",
		`minimum log level: "debug", "info", "warn" or "error"`)
	flag.StringVar(&cfg.logFormat, "log-format", "text",
		`structured log output format: "text" or "json"`)

	for i := range tables {
		t := &tables[i]
		flag.IntVar(&t.days, t.flagName, t.def,
			fmt.Sprintf("days of history to keep in %s (%s); 0 disables cleanup of this table",
				t.table, t.column))
	}

	showVersion := flag.Bool("version", false,
		"print the build identity and exit")
	flag.Parse()

	// Worth having on this one too: it runs from cron or a timer, where
	// nobody watches it start, and it is the binary that deletes rows.
	if *showVersion {
		fmt.Println("statusengine-db-cleanup", version.String())
		os.Exit(0)
	}

	explicit := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	var fc fileConfig
	if cfg.configFile != "" {
		fc = loadFileConfig(cfg.configFile)
	}

	cfg.mysqlDSN = resolveString(explicit, "mysql-dsn", cfg.mysqlDSN, "STATUSENGINE_MYSQL_DSN", fc.MySQLDSN)
	cfg.logLevel = resolveString(explicit, "log-level", cfg.logLevel, "STATUSENGINE_LOG_LEVEL", fc.LogLevel)
	cfg.logFormat = resolveString(explicit, "log-format", cfg.logFormat, "STATUSENGINE_LOG_FORMAT", fc.LogFormat)

	var err error
	cfg.batchSize, err = resolveInt(explicit, "cleanup-batch-size", cfg.batchSize,
		"STATUSENGINE_CLEANUP_BATCH_SIZE", fc.CleanupBatchSize)
	if err != nil {
		fatal("invalid cleanup_batch_size", "error", err)
	}
	cfg.batchPause, err = resolveDuration(explicit, "cleanup-batch-pause", cfg.batchPause,
		"STATUSENGINE_CLEANUP_BATCH_PAUSE", fc.CleanupBatchPause)
	if err != nil {
		fatal("invalid cleanup_batch_pause", "error", err)
	}

	if cfg.batchSize < 1 {
		fatal("invalid -cleanup-batch-size", "value", cfg.batchSize, "want", "a positive number")
	}
	if cfg.batchPause < 0 {
		fatal("invalid -cleanup-batch-pause", "value", cfg.batchPause, "want", "zero or a positive duration")
	}

	for i := range tables {
		t := &tables[i]
		t.days, err = resolveOptionalInt(explicit, t.flagName, t.days, t.envKey(), t.file(fc))
		if err != nil {
			fatal("invalid retention setting", "key", t.yamlKey(), "error", err)
		}
		// A negative age would put the cutoff in the future, i.e. delete
		// the whole table. cleanup.Run treats it as disabled, but a
		// value that can only be a mistake is worth stopping for rather
		// than quietly ignoring.
		if t.days < 0 {
			fatal("invalid retention setting", "key", t.yamlKey(), "value", t.days,
				"want", "a positive number of days, or 0 to disable cleanup of this table")
		}

		cfg.tables = append(cfg.tables, cleanup.Table{
			Name:   t.table,
			Column: t.column,
			Days:   t.days,
		})
	}

	return cfg
}

func setupLogger(cfg config) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.logLevel)); err != nil {
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.EqualFold(cfg.logFormat, "json") {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func main() {
	cfg := loadConfig()
	setupLogger(cfg)
	if cfg.configFile != "" {
		slog.Info("config: loaded settings from file", "path", cfg.configFile)
	}

	// SIGTERM is what a systemd timer sends when it wants the unit to
	// stop, and Ctrl-C is what a human sends during a manual run. Both
	// end the pass between two batches rather than tearing down a
	// transaction mid-flight; the next run resumes where this one left
	// off, since every committed batch is permanent.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sqlDB, err := sql.Open("mysql", cfg.mysqlDSN)
	if err != nil {
		fatal("mysql: open failed", "error", err)
	}
	defer sqlDB.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, pingTimeout)
	err = sqlDB.PingContext(pingCtx)
	cancelPing()
	if err != nil {
		fatal("mysql: unreachable", "error", err)
	}

	slog.Info("cleanup: starting",
		"tables", len(cfg.tables),
		"batch_size", cfg.batchSize,
		"batch_pause", cfg.batchPause)

	started := time.Now()
	runErr := cleanup.Run(ctx, sqlDB, cfg.tables, cleanup.Options{
		BatchSize: cfg.batchSize,
		Pause:     cfg.batchPause,
	})

	elapsed := time.Since(started).Round(time.Millisecond)

	if runErr != nil {
		// Individual failures were already logged as they happened by
		// cleanup.Run; this is the summary line plus the non-zero exit
		// the timer reports.
		slog.Error("cleanup: finished with errors",
			"failed_tables", len(splitJoined(runErr)), "duration", elapsed)
		os.Exit(1)
	}

	// An interrupted run is not a failure: it stopped where it was told
	// to, and the remaining rows are the next run's job. Reporting it as
	// one would make every timer stop look like an incident - but it is
	// not "finished" either, so it gets its own closing line rather than
	// both.
	if ctx.Err() != nil {
		slog.Warn("cleanup: interrupted, remaining tables left for the next run",
			"duration", elapsed)
		return
	}

	slog.Info("cleanup: finished", "duration", elapsed)
}

// splitJoined unwraps the errors.Join result from cleanup.Run back into
// the individual table failures, so the summary can say how many tables
// were affected rather than dumping every message again.
func splitJoined(err error) []error {
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return joined.Unwrap()
	}
	return []error{err}
}

// Package types defines the Go representations of the queue payloads consumed
// by the statusengine worker, as documented in /.claude/specs/mysql_schema.sql
// and the JSON dumps in /.claude/specs/.
package types

// Known values of Envelope.Type across the supported queues.
const (
	EventTypeLogEntry                  = 300
	EventTypeServiceCheck              = 701
	EventTypeHostCheck                 = 801
	EventTypeNotification              = 601
	EventTypeContactNotificationMethod = 605
	EventTypeDowntime                  = 1100
	EventTypeHostStatus                = 1201
	EventTypeServiceStatus             = 1202
	EventTypeAcknowledgement           = 1700
	EventTypeStateChange               = 1801
)

// Envelope is the common header present on every queue message.
type Envelope struct {
	Type          int   `json:"type"`
	Flags         int   `json:"flags"`
	Attr          int   `json:"attr"`
	Timestamp     int64 `json:"timestamp"`
	TimestampUsec int   `json:"timestamp_usec"`
}

// HostStatusPayload mirrors the `hoststatus` object and the
// statusengine_hoststatus table.
type HostStatusPayload struct {
	Name                       string  `json:"name"`
	PluginOutput               string  `json:"plugin_output"`
	LongPluginOutput           string  `json:"long_plugin_output"`
	EventHandler               *string `json:"event_handler"`
	PerfData                   string  `json:"perf_data"`
	CheckCommand               string  `json:"check_command"`
	CheckPeriod                string  `json:"check_period"`
	CurrentState               int     `json:"current_state"`
	HasBeenChecked             int     `json:"has_been_checked"`
	ShouldBeScheduled          int     `json:"should_be_scheduled"`
	CurrentAttempt             int     `json:"current_attempt"`
	MaxAttempts                int     `json:"max_attempts"`
	LastCheck                  int64   `json:"last_check"`
	NextCheck                  int64   `json:"next_check"`
	CheckType                  int     `json:"check_type"`
	LastStateChange            int64   `json:"last_state_change"`
	LastHardStateChange        int64   `json:"last_hard_state_change"`
	LastHardState              int     `json:"last_hard_state"`
	LastTimeUp                 int64   `json:"last_time_up"`
	LastTimeDown               int64   `json:"last_time_down"`
	LastTimeUnreachable        int64   `json:"last_time_unreachable"`
	StateType                  int     `json:"state_type"`
	LastNotification           int64   `json:"last_notification"`
	NextNotification           int64   `json:"next_notification"`
	NoMoreNotifications        int     `json:"no_more_notifications"`
	NotificationsEnabled       int     `json:"notifications_enabled"`
	ProblemHasBeenAcknowledged int     `json:"problem_has_been_acknowledged"`
	AcknowledgementType        int     `json:"acknowledgement_type"`
	CurrentNotificationNumber  int     `json:"current_notification_number"`
	AcceptPassiveChecks        int     `json:"accept_passive_checks"`
	EventHandlerEnabled        int     `json:"event_handler_enabled"`
	ChecksEnabled              int     `json:"checks_enabled"`
	FlapDetectionEnabled       int     `json:"flap_detection_enabled"`
	IsFlapping                 int     `json:"is_flapping"`
	PercentStateChange         float64 `json:"percent_state_change"`
	Latency                    float64 `json:"latency"`
	ExecutionTime              float64 `json:"execution_time"`
	ScheduledDowntimeDepth     int     `json:"scheduled_downtime_depth"`
	ProcessPerformanceData     int     `json:"process_performance_data"`
	Obsess                     int     `json:"obsess"`
	ModifiedAttributes         int     `json:"modified_attributes"`
	CheckInterval              float64 `json:"check_interval"`
	RetryInterval              float64 `json:"retry_interval"`
}

// HostStatusMessage is a single entry of the statusngin_hoststatus queue.
type HostStatusMessage struct {
	Envelope
	HostStatus HostStatusPayload `json:"hoststatus"`
}

// HostStatusBulk is the bulk payload delivered by the statusngin_hoststatus queue.
type HostStatusBulk struct {
	Messages []HostStatusMessage `json:"messages"`
	Format   string              `json:"format"`
}

// ServiceStatusPayload mirrors the `servicestatus` object and the
// statusengine_servicestatus table.
type ServiceStatusPayload struct {
	HostName                   string  `json:"host_name"`
	Description                string  `json:"description"`
	PluginOutput               string  `json:"plugin_output"`
	LongPluginOutput           string  `json:"long_plugin_output"`
	EventHandler               *string `json:"event_handler"`
	PerfData                   string  `json:"perf_data"`
	CheckCommand               string  `json:"check_command"`
	CheckPeriod                string  `json:"check_period"`
	CurrentState               int     `json:"current_state"`
	HasBeenChecked             int     `json:"has_been_checked"`
	ShouldBeScheduled          int     `json:"should_be_scheduled"`
	CurrentAttempt             int     `json:"current_attempt"`
	MaxAttempts                int     `json:"max_attempts"`
	LastCheck                  int64   `json:"last_check"`
	NextCheck                  int64   `json:"next_check"`
	CheckType                  int     `json:"check_type"`
	LastStateChange            int64   `json:"last_state_change"`
	LastHardStateChange        int64   `json:"last_hard_state_change"`
	LastHardState              int     `json:"last_hard_state"`
	LastTimeOk                 int64   `json:"last_time_ok"`
	LastTimeWarning            int64   `json:"last_time_warning"`
	LastTimeCritical           int64   `json:"last_time_critical"`
	LastTimeUnknown            int64   `json:"last_time_unknown"`
	StateType                  int     `json:"state_type"`
	LastNotification           int64   `json:"last_notification"`
	NextNotification           int64   `json:"next_notification"`
	NoMoreNotifications        int     `json:"no_more_notifications"`
	NotificationsEnabled       int     `json:"notifications_enabled"`
	ProblemHasBeenAcknowledged int     `json:"problem_has_been_acknowledged"`
	AcknowledgementType        int     `json:"acknowledgement_type"`
	CurrentNotificationNumber  int     `json:"current_notification_number"`
	AcceptPassiveChecks        int     `json:"accept_passive_checks"`
	EventHandlerEnabled        int     `json:"event_handler_enabled"`
	ChecksEnabled              int     `json:"checks_enabled"`
	FlapDetectionEnabled       int     `json:"flap_detection_enabled"`
	IsFlapping                 int     `json:"is_flapping"`
	PercentStateChange         float64 `json:"percent_state_change"`
	Latency                    float64 `json:"latency"`
	ExecutionTime              float64 `json:"execution_time"`
	ScheduledDowntimeDepth     int     `json:"scheduled_downtime_depth"`
	ProcessPerformanceData     int     `json:"process_performance_data"`
	Obsess                     int     `json:"obsess"`
	ModifiedAttributes         int     `json:"modified_attributes"`
	CheckInterval              float64 `json:"check_interval"`
	RetryInterval              float64 `json:"retry_interval"`
}

// ServiceStatusMessage is a single entry of the statusngin_servicestatus queue.
type ServiceStatusMessage struct {
	Envelope
	ServiceStatus ServiceStatusPayload `json:"servicestatus"`
}

// ServiceStatusBulk is the bulk payload delivered by the statusngin_servicestatus queue.
type ServiceStatusBulk struct {
	Messages []ServiceStatusMessage `json:"messages"`
	Format   string                 `json:"format"`
}

// HostCheckPayload mirrors the `hostcheck` object and the
// statusengine_hostchecks table.
type HostCheckPayload struct {
	HostName       string  `json:"host_name"`
	CommandLine    string  `json:"command_line"`
	CommandName    string  `json:"command_name"`
	Output         string  `json:"output"`
	LongOutput     string  `json:"long_output"`
	PerfData       string  `json:"perf_data"`
	CheckType      int     `json:"check_type"`
	CurrentAttempt int     `json:"current_attempt"`
	MaxAttempts    int     `json:"max_attempts"`
	StateType      int     `json:"state_type"`
	State          int     `json:"state"`
	Timeout        int     `json:"timeout"`
	StartTime      int64   `json:"start_time"`
	EndTime        int64   `json:"end_time"`
	EarlyTimeout   int     `json:"early_timeout"`
	ExecutionTime  float64 `json:"execution_time"`
	Latency        float64 `json:"latency"`
	ReturnCode     int     `json:"return_code"`
}

// HostCheckMessage is a single entry of the statusngin_hostchecks queue.
type HostCheckMessage struct {
	Envelope
	HostCheck HostCheckPayload `json:"hostcheck"`
}

// HostCheckBulk is the bulk payload delivered by the statusngin_hostchecks queue.
type HostCheckBulk struct {
	Messages []HostCheckMessage `json:"messages"`
	Format   string             `json:"format"`
}

// ServiceCheckPayload mirrors the `servicecheck` object and the
// statusengine_servicechecks table. The statusngin_service_perfdata queue
// reuses this same payload shape but only populates HostName,
// ServiceDescription, PerfData and StartTime - see CLAUDE.md rule 5
// (Conditional Perfdata Routing).
type ServiceCheckPayload struct {
	HostName           string  `json:"host_name"`
	ServiceDescription string  `json:"service_description"`
	CommandLine        string  `json:"command_line,omitempty"`
	CommandName        string  `json:"command_name,omitempty"`
	Output             string  `json:"output,omitempty"`
	LongOutput         string  `json:"long_output,omitempty"`
	PerfData           string  `json:"perf_data"`
	CheckType          int     `json:"check_type,omitempty"`
	CurrentAttempt     int     `json:"current_attempt,omitempty"`
	MaxAttempts        int     `json:"max_attempts,omitempty"`
	StateType          int     `json:"state_type,omitempty"`
	State              int     `json:"state,omitempty"`
	Timeout            int     `json:"timeout,omitempty"`
	StartTime          int64   `json:"start_time"`
	EndTime            int64   `json:"end_time,omitempty"`
	EarlyTimeout       int     `json:"early_timeout,omitempty"`
	ExecutionTime      float64 `json:"execution_time,omitempty"`
	Latency            float64 `json:"latency,omitempty"`
	ReturnCode         int     `json:"return_code,omitempty"`
}

// ServiceCheckMessage is a single entry of the statusngin_servicechecks and
// statusngin_service_perfdata queues.
type ServiceCheckMessage struct {
	Envelope
	ServiceCheck ServiceCheckPayload `json:"servicecheck"`
}

// ServiceCheckBulk is the bulk payload delivered by the statusngin_servicechecks
// and statusngin_service_perfdata queues.
type ServiceCheckBulk struct {
	Messages []ServiceCheckMessage `json:"messages"`
	Format   string                `json:"format"`
}

// StateChangePayload mirrors the `statechange` object and the
// statusengine_host_statehistory / statusengine_service_statehistory tables.
type StateChangePayload struct {
	HostName           string `json:"host_name"`
	ServiceDescription string `json:"service_description"`
	Output             string `json:"output"`
	LongOutput         string `json:"long_output"`
	StateChangeType    int    `json:"statechange_type"`
	State              int    `json:"state"`
	StateType          int    `json:"state_type"`
	CurrentAttempt     int    `json:"current_attempt"`
	MaxAttempts        int    `json:"max_attempts"`
	LastState          int    `json:"last_state"`
	LastHardState      int    `json:"last_hard_state"`
}

// StateChangeMessage is a single entry of the statusngin_statechanges queue.
type StateChangeMessage struct {
	Envelope
	StateChange StateChangePayload `json:"statechange"`
}

// StateChangeBulk is the bulk payload delivered by the statusngin_statechanges queue.
type StateChangeBulk struct {
	Messages []StateChangeMessage `json:"messages"`
	Format   string               `json:"format"`
}

// LogEntryPayload mirrors the `logentry` object and the
// statusengine_logentries table.
type LogEntryPayload struct {
	EntryTime int64  `json:"entry_time"`
	DataType  int    `json:"data_type"`
	Data      string `json:"data"`
}

// LogEntryMessage is a single entry of the statusngin_logentries queue.
type LogEntryMessage struct {
	Envelope
	LogEntry LogEntryPayload `json:"logentry"`
}

// LogEntryBulk is the bulk payload delivered by the statusngin_logentries queue.
type LogEntryBulk struct {
	Messages []LogEntryMessage `json:"messages"`
	Format   string            `json:"format"`
}

// NotificationPayload mirrors the `notification_data` object and the
// statusengine_host_notifications / statusengine_service_notifications tables.
type NotificationPayload struct {
	HostName           string `json:"host_name"`
	ServiceDescription string `json:"service_description"`
	Output             string `json:"output"`
	LongOutput         string `json:"long_output"`
	AckAuthor          string `json:"ack_author"`
	AckData            string `json:"ack_data"`
	NotificationType   int    `json:"notification_type"`
	StartTime          int64  `json:"start_time"`
	EndTime            int64  `json:"end_time"`
	ReasonType         int    `json:"reason_type"`
	State              int    `json:"state"`
	Escalated          int    `json:"escalated"`
	ContactsNotified   int    `json:"contacts_notified"`
}

// NotificationMessage is a single entry of the statusngin_notifications queue.
type NotificationMessage struct {
	Envelope
	NotificationData NotificationPayload `json:"notification_data"`
}

// NotificationBulk is the bulk payload delivered by the statusngin_notifications queue.
type NotificationBulk struct {
	Messages []NotificationMessage `json:"messages"`
	Format   string                `json:"format"`
}

// ContactNotificationMethodPayload mirrors the `contactnotificationmethod`
// object. There is no dedicated table for it in the MySQL schema - it
// represents a single contact's delivery of a notification already recorded
// via NotificationPayload.
type ContactNotificationMethodPayload struct {
	HostName           string  `json:"host_name"`
	ServiceDescription string  `json:"service_description"`
	Output             string  `json:"output"`
	AckAuthor          string  `json:"ack_author"`
	AckData            string  `json:"ack_data"`
	ContactName        string  `json:"contact_name"`
	CommandName        string  `json:"command_name"`
	CommandArgs        *string `json:"command_args"`
	ReasonType         int     `json:"reason_type"`
	State              int     `json:"state"`
	StartTime          int64   `json:"start_time"`
	EndTime            int64   `json:"end_time"`
}

// ContactNotificationMethodMessage is the (non-bulk) payload of the
// statusngin_contactnotificationmethod queue.
type ContactNotificationMethodMessage struct {
	Envelope
	ContactNotificationMethod ContactNotificationMethodPayload `json:"contactnotificationmethod"`
}

// AcknowledgementPayload mirrors the `acknowledgement` object and the
// statusengine_host_acknowledgements / statusengine_service_acknowledgements
// tables.
type AcknowledgementPayload struct {
	HostName            string `json:"host_name"`
	ServiceDescription  string `json:"service_description"`
	AuthorName          string `json:"author_name"`
	CommentData         string `json:"comment_data"`
	AcknowledgementType int    `json:"acknowledgement_type"`
	State               int    `json:"state"`
	IsSticky            int    `json:"is_sticky"`
	PersistentComment   int    `json:"persistent_comment"`
	NotifyContacts      int    `json:"notify_contacts"`
}

// AcknowledgementMessage is the (non-bulk) payload of the
// statusngin_acknowledgements queue.
type AcknowledgementMessage struct {
	Envelope
	Acknowledgement AcknowledgementPayload `json:"acknowledgement"`
}

// DowntimePayload mirrors the `downtime` object and the
// statusengine_host_scheduleddowntimes / statusengine_host_downtimehistory
// (and service equivalents) tables.
type DowntimePayload struct {
	HostName           string `json:"host_name"`
	ServiceDescription string `json:"service_description"`
	AuthorName         string `json:"author_name"`
	CommentData        string `json:"comment_data"`
	DowntimeType       int    `json:"downtime_type"`
	EntryTime          int64  `json:"entry_time"`
	StartTime          int64  `json:"start_time"`
	EndTime            int64  `json:"end_time"`
	TriggeredBy        int    `json:"triggered_by"`
	DowntimeID         int    `json:"downtime_id"`
	Fixed              int    `json:"fixed"`
	Duration           int    `json:"duration"`
}

// DowntimeMessage is the (non-bulk) payload of the statusngin_downtimes queue.
type DowntimeMessage struct {
	Envelope
	Downtime DowntimePayload `json:"downtime"`
}

// CoreRestartMessage is the (non-bulk, envelope-less) payload of the
// statusngin_core_restart queue.
type CoreRestartMessage struct {
	ObjectType int `json:"object_type"`
}

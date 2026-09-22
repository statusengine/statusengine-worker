-- Statusengine Worker - MySQL schema
--
-- Creates the 22 tables the Go worker writes to.
--
--   mysql -u statusengine -p statusengine < mysql_schema.sql
--
-- Uses CREATE TABLE IF NOT EXISTS throughout, so running it against a database
-- that already has some of them adds the rest and leaves the others alone.
--
-- Translated from the Doctrine DBAL schema the PHP worker built its tables
-- with (statusengine/worker, lib/mysql.php), which is the definition of record
-- for the standard Statusengine schema. Four deliberate differences from what
-- that code produced:
--
--   utf8mb4 instead of utf8. MySQL's "utf8" is the three-byte version and
--   cannot hold anything outside the Basic Multilingual Plane, so an emoji in
--   a plugin output does not survive it.
--
--   No COLLATE clause anywhere. MySQL and MariaDB disagree about which modern
--   utf8mb4 collation exists, and the answer changes by version, so naming one
--   would make this file fail to load somewhere. Without it the server uses
--   its own default for utf8mb4 - utf8mb4_0900_ai_ci on MySQL 8,
--   utf8mb4_uca1400_ai_ci on MariaDB 11.4, utf8mb4_general_ci on MariaDB
--   10.11 - which is always present and always consistent within one server.
--
--   perfdata is VARCHAR(2048), not VARCHAR(8192). Under utf8mb4 a
--   VARCHAR(8192) reserves 32770 bytes of the 65535-byte row limit, and
--   statusengine_hostchecks, _hoststatus, _servicechecks and _servicestatus
--   each carry two of them - perfdata and long_output - so the row no longer
--   fits and MySQL refuses the table with error 1118. 2048 is what the schema
--   running in production uses, so a database created from this file matches
--   one that was migrated rather than created fresh. The measured ceiling,
--   with long_output left at 8192, is between 5120 and 6144.
--
--   Note that an over-long value is not truncated: in strict mode it is
--   error 1406, and the worker drops the whole batch that carried it. If your
--   plugins emit performance data wider than 2048 characters, raise this here
--   and in any existing database, rather than finding out from a dropped
--   batch.
--
--   ack_author is VARCHAR(1024) in all four notification tables. The PHP
--   gives it 255 everywhere; production has 1024 in the two _log tables and
--   255 in the other two. This is the only width that is at least as generous
--   as both, and the failure mode is not truncation - see error 1406 above.
--
--   Every PRIMARY KEY column is NOT NULL. The PHP declares hostname as
--   nullable in four service tables while listing it in setPrimaryKey;
--   Doctrine forces NOT NULL when it emits the DDL. MySQL rejects the table
--   otherwise (error 1171) and MariaDB silently promotes the column, which
--   would leave the two with different schemas from one file.
--
--   statusengine_host_acknowledgements and _service_acknowledgements carry an
--   end_time column that the PHP schema does not have at all. Naemon
--   acknowledgements can expire and the broker module publishes that expiry;
--   the worker does not read it yet, so the column sits at its default of 0
--   until it does. It is here now because ADD COLUMN on a table that has
--   grown for years is the expensive kind of change and these two are small.
--
--   That last one is not derivable from lib/mysql.php. Anyone re-running the
--   translation against that file will produce a schema without it.
--
-- Integer display widths are omitted. MySQL 8 deprecated them and warns about
-- every one it is given, including tinyint(1) - the documented exception in
-- 8.0.19 preserves what tinyint(1) is *stored* as, so connectors can go on
-- treating it as BOOLEAN, but writing the width still raises warning 1681.
--
-- Booleans are therefore declared BOOLEAN. Both MySQL and MariaDB store that
-- as tinyint(1), which keeps the connector assumption, and neither warns,
-- because no display width was written. Note that the exception is
-- signed-only: "TINYINT(1) UNSIGNED" comes back out as "tinyint unsigned",
-- the width gone. The PHP declares these columns unsigned; they hold 0 and 1,
-- and the schema in production is signed.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

--
-- statusengine_dbversion
--

CREATE TABLE IF NOT EXISTS `statusengine_dbversion` (
  `id` INT NOT NULL,
  `dbversion` VARCHAR(255) DEFAULT '4.0.0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_host_acknowledgements
--

CREATE TABLE IF NOT EXISTS `statusengine_host_acknowledgements` (
  `hostname` VARCHAR(255) NOT NULL,
  `entry_time` BIGINT NOT NULL,
  `entry_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `state` SMALLINT unsigned DEFAULT 0,
  `author_name` VARCHAR(255) DEFAULT NULL,
  `comment_data` VARCHAR(1024) DEFAULT NULL,
  `acknowledgement_type` SMALLINT unsigned DEFAULT 0,
  `is_sticky` BOOLEAN DEFAULT 0,
  `persistent_comment` BOOLEAN DEFAULT 0,
  `notify_contacts` BOOLEAN DEFAULT 0,
  `end_time` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`hostname`,`entry_time`,`entry_time_usec`),
  KEY `hostname` (`hostname`),
  KEY `entry_time` (`entry_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_host_downtimehistory
--

CREATE TABLE IF NOT EXISTS `statusengine_host_downtimehistory` (
  `hostname` VARCHAR(255) NOT NULL,
  `internal_downtime_id` INT unsigned NOT NULL,
  `scheduled_start_time` BIGINT NOT NULL,
  `node_name` VARCHAR(255) NOT NULL,
  `entry_time` BIGINT NOT NULL,
  `entry_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `author_name` VARCHAR(255) DEFAULT NULL,
  `comment_data` VARCHAR(1024) DEFAULT NULL,
  `triggered_by_id` INT unsigned DEFAULT NULL,
  `is_fixed` BOOLEAN DEFAULT 0,
  `duration` INT unsigned DEFAULT NULL,
  `scheduled_end_time` BIGINT NOT NULL,
  `was_started` BOOLEAN DEFAULT 0,
  `actual_start_time` BIGINT NOT NULL,
  `actual_end_time` BIGINT NOT NULL,
  `was_cancelled` BOOLEAN DEFAULT 0,
  PRIMARY KEY (`hostname`,`node_name`,`scheduled_start_time`,`internal_downtime_id`),
  KEY `reports` (`hostname`,`entry_time`,`entry_time_usec`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`),
  KEY `list` (`hostname`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_host_notifications
--

CREATE TABLE IF NOT EXISTS `statusengine_host_notifications` (
  `hostname` VARCHAR(255) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `start_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `contact_name` VARCHAR(1024) DEFAULT NULL,
  `command_name` VARCHAR(1024) DEFAULT NULL,
  `command_args` VARCHAR(1024) DEFAULT NULL,
  `state` SMALLINT unsigned DEFAULT 0,
  `end_time` BIGINT NOT NULL,
  `reason_type` SMALLINT unsigned DEFAULT 0,
  `output` VARCHAR(1024) DEFAULT NULL,
  `ack_author` VARCHAR(1024) DEFAULT NULL,
  `ack_data` VARCHAR(1024) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`start_time`,`start_time_usec`),
  KEY `hostname` (`hostname`),
  KEY `start_time` (`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_host_notifications_log
--

CREATE TABLE IF NOT EXISTS `statusengine_host_notifications_log` (
  `hostname` VARCHAR(255) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `start_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `end_time` BIGINT NOT NULL,
  `state` SMALLINT unsigned DEFAULT 0,
  `reason_type` SMALLINT unsigned DEFAULT 0,
  `is_escalated` BOOLEAN DEFAULT 0,
  `contacts_notified_count` SMALLINT unsigned DEFAULT 0,
  `output` VARCHAR(1024) DEFAULT NULL,
  `ack_author` VARCHAR(1024) DEFAULT NULL,
  `ack_data` VARCHAR(1024) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`start_time`,`start_time_usec`),
  KEY `hostname` (`hostname`),
  KEY `start_time` (`start_time`),
  KEY `filter` (`start_time`,`end_time`,`reason_type`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_host_scheduleddowntimes
--

CREATE TABLE IF NOT EXISTS `statusengine_host_scheduleddowntimes` (
  `hostname` VARCHAR(255) NOT NULL,
  `internal_downtime_id` INT unsigned NOT NULL,
  `scheduled_start_time` BIGINT NOT NULL,
  `node_name` VARCHAR(255) NOT NULL,
  `entry_time` BIGINT NOT NULL,
  `author_name` VARCHAR(255) DEFAULT NULL,
  `comment_data` VARCHAR(1024) DEFAULT NULL,
  `triggered_by_id` INT unsigned DEFAULT NULL,
  `is_fixed` BOOLEAN DEFAULT 0,
  `duration` INT unsigned DEFAULT NULL,
  `scheduled_end_time` BIGINT NOT NULL,
  `was_started` BOOLEAN DEFAULT 0,
  `actual_start_time` BIGINT NOT NULL,
  PRIMARY KEY (`hostname`,`node_name`,`scheduled_start_time`,`internal_downtime_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_host_statehistory
--

CREATE TABLE IF NOT EXISTS `statusengine_host_statehistory` (
  `hostname` VARCHAR(255) NOT NULL,
  `state_time` BIGINT NOT NULL,
  `state_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `state_change` BOOLEAN DEFAULT 0,
  `state` SMALLINT unsigned DEFAULT 0,
  `is_hardstate` BOOLEAN DEFAULT 0,
  `current_check_attempt` SMALLINT unsigned DEFAULT 0,
  `max_check_attempts` SMALLINT unsigned DEFAULT 0,
  `last_state` SMALLINT unsigned DEFAULT 0,
  `last_hard_state` SMALLINT unsigned DEFAULT 0,
  `output` VARCHAR(1024) DEFAULT NULL,
  `long_output` VARCHAR(8192) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`state_time`,`state_time_usec`),
  KEY `hostname_time` (`hostname`,`state_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_hostchecks
--

CREATE TABLE IF NOT EXISTS `statusengine_hostchecks` (
  `hostname` VARCHAR(255) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `start_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `state` SMALLINT unsigned DEFAULT 0,
  `is_hardstate` BOOLEAN DEFAULT 0,
  `end_time` BIGINT NOT NULL,
  `output` VARCHAR(1024) DEFAULT NULL,
  `timeout` SMALLINT unsigned DEFAULT 0,
  `early_timeout` BOOLEAN DEFAULT 0,
  `latency` DOUBLE DEFAULT 0,
  `execution_time` DOUBLE DEFAULT 0,
  `perfdata` VARCHAR(2048) DEFAULT NULL,
  `command` VARCHAR(1024) DEFAULT NULL,
  `current_check_attempt` SMALLINT unsigned DEFAULT 0,
  `max_check_attempts` SMALLINT unsigned DEFAULT 0,
  `long_output` VARCHAR(8192) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`start_time`,`start_time_usec`),
  KEY `times` (`start_time`,`end_time`),
  KEY `hostname` (`hostname`,`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_hoststatus
--

CREATE TABLE IF NOT EXISTS `statusengine_hoststatus` (
  `hostname` VARCHAR(255) NOT NULL,
  `status_update_time` BIGINT NOT NULL,
  `output` VARCHAR(1024) DEFAULT NULL,
  `long_output` VARCHAR(8192) DEFAULT NULL,
  `perfdata` VARCHAR(2048) DEFAULT NULL,
  `current_state` SMALLINT unsigned DEFAULT 0,
  `current_check_attempt` SMALLINT unsigned DEFAULT 0,
  `max_check_attempts` SMALLINT unsigned DEFAULT 0,
  `last_check` BIGINT NOT NULL,
  `next_check` BIGINT NOT NULL,
  `is_passive_check` BOOLEAN DEFAULT 0,
  `last_state_change` BIGINT NOT NULL,
  `last_hard_state_change` BIGINT NOT NULL,
  `last_hard_state` SMALLINT unsigned DEFAULT 0,
  `is_hardstate` BOOLEAN DEFAULT 0,
  `last_notification` BIGINT NOT NULL,
  `next_notification` BIGINT NOT NULL,
  `notifications_enabled` BOOLEAN DEFAULT 0,
  `problem_has_been_acknowledged` BOOLEAN DEFAULT 0,
  `acknowledgement_type` SMALLINT unsigned DEFAULT 0,
  `passive_checks_enabled` BOOLEAN DEFAULT 0,
  `active_checks_enabled` BOOLEAN DEFAULT 0,
  `event_handler_enabled` BOOLEAN DEFAULT 0,
  `flap_detection_enabled` BOOLEAN DEFAULT 0,
  `is_flapping` BOOLEAN DEFAULT 0,
  `latency` DOUBLE DEFAULT 0,
  `execution_time` DOUBLE DEFAULT 0,
  `scheduled_downtime_depth` SMALLINT unsigned DEFAULT 0,
  `process_performance_data` BOOLEAN DEFAULT 0,
  `obsess_over_host` BOOLEAN DEFAULT 0,
  `normal_check_interval` INT unsigned DEFAULT 0,
  `retry_check_interval` INT unsigned DEFAULT 0,
  `check_timeperiod` VARCHAR(255) DEFAULT NULL,
  `node_name` VARCHAR(255) DEFAULT NULL,
  `last_time_up` BIGINT NOT NULL,
  `last_time_down` BIGINT NOT NULL,
  `last_time_unreachable` BIGINT NOT NULL,
  `current_notification_number` INT unsigned DEFAULT 0,
  `percent_state_change` DOUBLE DEFAULT 0,
  `event_handler` VARCHAR(255) DEFAULT NULL,
  `check_command` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`hostname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_logentries
--

CREATE TABLE IF NOT EXISTS `statusengine_logentries` (
  `id` BIGINT unsigned NOT NULL AUTO_INCREMENT,
  `entry_time` BIGINT NOT NULL,
  `logentry_type` INT DEFAULT 0,
  `logentry_data` VARCHAR(2048) DEFAULT NULL,
  `node_name` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `logentries_se` (`entry_time`,`node_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_perfdata
--

CREATE TABLE IF NOT EXISTS `statusengine_perfdata` (
  `hostname` VARCHAR(255) DEFAULT NULL,
  `service_description` VARCHAR(255) DEFAULT NULL,
  `label` VARCHAR(255) DEFAULT NULL,
  `timestamp` BIGINT NOT NULL,
  `timestamp_unix` BIGINT NOT NULL,
  `value` DOUBLE DEFAULT NULL,
  `unit` VARCHAR(10) DEFAULT NULL,
  KEY `metric` (`hostname`,`service_description`,`label`,`timestamp_unix`),
  KEY `timestamp_unix` (`timestamp_unix`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_service_acknowledgements
--

CREATE TABLE IF NOT EXISTS `statusengine_service_acknowledgements` (
  `service_description` VARCHAR(255) NOT NULL,
  `entry_time` BIGINT NOT NULL,
  `entry_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `hostname` VARCHAR(255) NOT NULL,
  `state` SMALLINT unsigned DEFAULT 0,
  `author_name` VARCHAR(255) DEFAULT NULL,
  `comment_data` VARCHAR(1024) DEFAULT NULL,
  `acknowledgement_type` SMALLINT unsigned DEFAULT 0,
  `is_sticky` BOOLEAN DEFAULT 0,
  `persistent_comment` BOOLEAN DEFAULT 0,
  `notify_contacts` BOOLEAN DEFAULT 0,
  `end_time` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`hostname`,`service_description`,`entry_time`,`entry_time_usec`),
  KEY `servicename` (`hostname`,`service_description`),
  KEY `entry_time` (`entry_time`),
  KEY `servicedesc_time` (`service_description`,`entry_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_service_downtimehistory
--

CREATE TABLE IF NOT EXISTS `statusengine_service_downtimehistory` (
  `hostname` VARCHAR(255) NOT NULL,
  `service_description` VARCHAR(255) NOT NULL,
  `internal_downtime_id` INT unsigned NOT NULL,
  `scheduled_start_time` BIGINT NOT NULL,
  `node_name` VARCHAR(255) NOT NULL,
  `entry_time` BIGINT NOT NULL,
  `entry_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `author_name` VARCHAR(255) DEFAULT NULL,
  `comment_data` VARCHAR(1024) DEFAULT NULL,
  `triggered_by_id` INT unsigned DEFAULT NULL,
  `is_fixed` BOOLEAN DEFAULT 0,
  `duration` INT unsigned DEFAULT NULL,
  `scheduled_end_time` BIGINT NOT NULL,
  `was_started` BOOLEAN DEFAULT 0,
  `actual_start_time` BIGINT NOT NULL,
  `actual_end_time` BIGINT NOT NULL,
  `was_cancelled` BOOLEAN DEFAULT 0,
  PRIMARY KEY (`hostname`,`service_description`,`node_name`,`scheduled_start_time`,`internal_downtime_id`),
  KEY `reports` (`service_description`,`entry_time`,`entry_time_usec`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`),
  KEY `report` (`service_description`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_service_notifications
--

CREATE TABLE IF NOT EXISTS `statusengine_service_notifications` (
  `service_description` VARCHAR(255) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `start_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `hostname` VARCHAR(255) NOT NULL,
  `contact_name` VARCHAR(1024) DEFAULT NULL,
  `command_name` VARCHAR(1024) DEFAULT NULL,
  `command_args` VARCHAR(1024) DEFAULT NULL,
  `state` SMALLINT unsigned DEFAULT 0,
  `end_time` BIGINT NOT NULL,
  `reason_type` SMALLINT unsigned DEFAULT 0,
  `output` VARCHAR(1024) DEFAULT NULL,
  `ack_author` VARCHAR(1024) DEFAULT NULL,
  `ack_data` VARCHAR(1024) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`service_description`,`start_time`,`start_time_usec`),
  KEY `servicename` (`hostname`,`service_description`),
  KEY `start_time` (`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_service_notifications_log
--

CREATE TABLE IF NOT EXISTS `statusengine_service_notifications_log` (
  `hostname` VARCHAR(255) NOT NULL,
  `service_description` VARCHAR(255) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `start_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `end_time` BIGINT NOT NULL,
  `state` SMALLINT unsigned DEFAULT 0,
  `reason_type` SMALLINT unsigned DEFAULT 0,
  `is_escalated` BOOLEAN DEFAULT 0,
  `contacts_notified_count` SMALLINT unsigned DEFAULT 0,
  `output` VARCHAR(1024) DEFAULT NULL,
  `ack_author` VARCHAR(1024) DEFAULT NULL,
  `ack_data` VARCHAR(1024) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`service_description`,`start_time`,`start_time_usec`),
  KEY `hostname` (`hostname`),
  KEY `servicename` (`hostname`,`service_description`),
  KEY `start_time` (`start_time`),
  KEY `filter` (`start_time`,`end_time`,`reason_type`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_service_scheduleddowntimes
--

CREATE TABLE IF NOT EXISTS `statusengine_service_scheduleddowntimes` (
  `hostname` VARCHAR(255) NOT NULL,
  `service_description` VARCHAR(255) NOT NULL,
  `internal_downtime_id` INT unsigned NOT NULL,
  `scheduled_start_time` BIGINT NOT NULL,
  `node_name` VARCHAR(255) NOT NULL,
  `entry_time` BIGINT NOT NULL,
  `author_name` VARCHAR(255) DEFAULT NULL,
  `comment_data` VARCHAR(1024) DEFAULT NULL,
  `triggered_by_id` INT unsigned DEFAULT NULL,
  `is_fixed` BOOLEAN DEFAULT 0,
  `duration` INT unsigned DEFAULT NULL,
  `scheduled_end_time` BIGINT NOT NULL,
  `was_started` BOOLEAN DEFAULT 0,
  `actual_start_time` BIGINT NOT NULL,
  PRIMARY KEY (`hostname`,`service_description`,`node_name`,`scheduled_start_time`,`internal_downtime_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_service_statehistory
--

CREATE TABLE IF NOT EXISTS `statusengine_service_statehistory` (
  `service_description` VARCHAR(255) NOT NULL,
  `state_time` BIGINT NOT NULL,
  `state_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `hostname` VARCHAR(255) NOT NULL,
  `state_change` BOOLEAN DEFAULT 0,
  `state` SMALLINT unsigned DEFAULT 0,
  `is_hardstate` BOOLEAN DEFAULT 0,
  `current_check_attempt` SMALLINT unsigned DEFAULT 0,
  `max_check_attempts` SMALLINT unsigned DEFAULT 0,
  `last_state` SMALLINT unsigned DEFAULT 0,
  `last_hard_state` SMALLINT unsigned DEFAULT 0,
  `output` VARCHAR(1024) DEFAULT NULL,
  `long_output` VARCHAR(8192) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`service_description`,`state_time`,`state_time_usec`),
  KEY `host_servicename_time` (`hostname`,`service_description`,`state_time`),
  KEY `servicename_time` (`service_description`,`state_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_servicechecks
--

CREATE TABLE IF NOT EXISTS `statusengine_servicechecks` (
  `service_description` VARCHAR(255) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `start_time_usec` INT unsigned NOT NULL DEFAULT 0,
  `hostname` VARCHAR(255) NOT NULL,
  `state` SMALLINT unsigned DEFAULT 0,
  `is_hardstate` BOOLEAN DEFAULT 0,
  `end_time` BIGINT NOT NULL,
  `output` VARCHAR(1024) DEFAULT NULL,
  `timeout` SMALLINT unsigned DEFAULT 0,
  `early_timeout` BOOLEAN DEFAULT 0,
  `latency` DOUBLE DEFAULT 0,
  `execution_time` DOUBLE DEFAULT 0,
  `perfdata` VARCHAR(2048) DEFAULT NULL,
  `command` VARCHAR(1024) DEFAULT NULL,
  `current_check_attempt` SMALLINT unsigned DEFAULT 0,
  `max_check_attempts` SMALLINT unsigned DEFAULT 0,
  `long_output` VARCHAR(8192) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`service_description`,`start_time`,`start_time_usec`),
  KEY `servicename` (`hostname`,`service_description`,`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- statusengine_servicestatus
--

CREATE TABLE IF NOT EXISTS `statusengine_servicestatus` (
  `hostname` VARCHAR(255) NOT NULL,
  `service_description` VARCHAR(255) NOT NULL,
  `status_update_time` BIGINT NOT NULL,
  `output` VARCHAR(1024) DEFAULT NULL,
  `long_output` VARCHAR(8192) DEFAULT NULL,
  `perfdata` VARCHAR(2048) DEFAULT NULL,
  `current_state` SMALLINT unsigned DEFAULT 0,
  `current_check_attempt` SMALLINT unsigned DEFAULT 0,
  `max_check_attempts` SMALLINT unsigned DEFAULT 0,
  `last_check` BIGINT NOT NULL,
  `next_check` BIGINT NOT NULL,
  `is_passive_check` BOOLEAN DEFAULT 0,
  `last_state_change` BIGINT NOT NULL,
  `last_hard_state_change` BIGINT NOT NULL,
  `last_hard_state` SMALLINT unsigned DEFAULT 0,
  `is_hardstate` BOOLEAN DEFAULT 0,
  `last_notification` BIGINT NOT NULL,
  `next_notification` BIGINT NOT NULL,
  `notifications_enabled` BOOLEAN DEFAULT 0,
  `problem_has_been_acknowledged` BOOLEAN DEFAULT 0,
  `acknowledgement_type` SMALLINT unsigned DEFAULT 0,
  `passive_checks_enabled` BOOLEAN DEFAULT 0,
  `active_checks_enabled` BOOLEAN DEFAULT 0,
  `event_handler_enabled` BOOLEAN DEFAULT 0,
  `flap_detection_enabled` BOOLEAN DEFAULT 0,
  `is_flapping` BOOLEAN DEFAULT 0,
  `latency` DOUBLE DEFAULT 0,
  `execution_time` DOUBLE DEFAULT 0,
  `scheduled_downtime_depth` SMALLINT unsigned DEFAULT 0,
  `process_performance_data` BOOLEAN DEFAULT 0,
  `obsess_over_service` BOOLEAN DEFAULT 0,
  `normal_check_interval` INT unsigned DEFAULT 0,
  `retry_check_interval` INT unsigned DEFAULT 0,
  `check_timeperiod` VARCHAR(255) DEFAULT NULL,
  `node_name` VARCHAR(255) DEFAULT NULL,
  `last_time_ok` BIGINT NOT NULL,
  `last_time_warning` BIGINT NOT NULL,
  `last_time_critical` BIGINT NOT NULL,
  `last_time_unknown` BIGINT NOT NULL,
  `current_notification_number` INT unsigned DEFAULT 0,
  `percent_state_change` DOUBLE DEFAULT 0,
  `event_handler` VARCHAR(255) DEFAULT NULL,
  `check_command` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`service_description`),
  KEY `service_description` (`service_description`),
  KEY `current_state_node` (`current_state`,`node_name`),
  KEY `issues` (`problem_has_been_acknowledged`,`scheduled_downtime_depth`,`current_state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


SET FOREIGN_KEY_CHECKS = 1;

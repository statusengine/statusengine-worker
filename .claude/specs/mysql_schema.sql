/*M!999999\- enable the sandbox mode */
-- MariaDB dump 10.19  Distrib 10.11.14-MariaDB, for debian-linux-gnu (aarch64)
--
-- Host: localhost    Database: openitcockpit
-- ------------------------------------------------------
-- Server version       10.11.14-MariaDB-0+deb12u2

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `statusengine_dbversion`
--

DROP TABLE IF EXISTS `statusengine_dbversion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_dbversion` (
  `id` int(11) NOT NULL,
  `dbversion` varchar(255) DEFAULT '3.0.0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_host_acknowledgements`
--

DROP TABLE IF EXISTS `statusengine_host_acknowledgements`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_host_acknowledgements` (
  `hostname` varchar(255) NOT NULL,
  `entry_time` bigint(20) NOT NULL,
  `entry_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `state` smallint(5) unsigned DEFAULT 0,
  `author_name` varchar(255) DEFAULT NULL,
  `comment_data` varchar(1024) DEFAULT NULL,
  `acknowledgement_type` smallint(5) unsigned DEFAULT 0,
  `is_sticky` tinyint(1) DEFAULT 0,
  `persistent_comment` tinyint(1) DEFAULT 0,
  `notify_contacts` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`hostname`,`entry_time`,`entry_time_usec`),
  KEY `hostname` (`hostname`),
  KEY `entry_time` (`entry_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_host_downtimehistory`
--

DROP TABLE IF EXISTS `statusengine_host_downtimehistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_host_downtimehistory` (
  `hostname` varchar(255) NOT NULL,
  `internal_downtime_id` int(10) unsigned NOT NULL,
  `scheduled_start_time` bigint(20) NOT NULL,
  `node_name` varchar(255) NOT NULL,
  `entry_time` bigint(20) NOT NULL,
  `entry_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `author_name` varchar(255) DEFAULT NULL,
  `comment_data` varchar(1024) DEFAULT NULL,
  `triggered_by_id` int(10) unsigned DEFAULT NULL,
  `is_fixed` tinyint(1) DEFAULT 0,
  `duration` int(10) unsigned DEFAULT NULL,
  `scheduled_end_time` bigint(20) NOT NULL,
  `was_started` tinyint(1) DEFAULT 0,
  `actual_start_time` bigint(20) NOT NULL,
  `actual_end_time` bigint(20) NOT NULL,
  `was_cancelled` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`hostname`,`node_name`,`scheduled_start_time`,`internal_downtime_id`),
  KEY `reports` (`hostname`,`entry_time`,`entry_time_usec`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`),
  KEY `list` (`hostname`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_host_notifications`
--

DROP TABLE IF EXISTS `statusengine_host_notifications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_host_notifications` (
  `hostname` varchar(255) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `start_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `contact_name` varchar(1024) DEFAULT NULL,
  `command_name` varchar(1024) DEFAULT NULL,
  `command_args` varchar(1024) DEFAULT NULL,
  `state` smallint(2) DEFAULT 0,
  `end_time` bigint(20) NOT NULL,
  `reason_type` smallint(3) DEFAULT 0,
  `output` varchar(1024) DEFAULT NULL,
  `ack_author` varchar(255) DEFAULT NULL,
  `ack_data` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`start_time`,`start_time_usec`),
  KEY `hostname` (`hostname`),
  KEY `start_time` (`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`start_time` DIV 86400)
(PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_host_notifications_log`
--

DROP TABLE IF EXISTS `statusengine_host_notifications_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_host_notifications_log` (
  `hostname` varchar(255) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `start_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `end_time` bigint(20) NOT NULL,
  `state` smallint(6) unsigned DEFAULT 0,
  `reason_type` smallint(6) unsigned DEFAULT 0,
  `is_escalated` tinyint(1) NOT NULL DEFAULT 0,
  `contacts_notified_count` smallint(6) unsigned NOT NULL DEFAULT 0,
  `output` varchar(1024) DEFAULT NULL,
  `ack_author` varchar(1024) DEFAULT NULL,
  `ack_data` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`start_time`,`start_time_usec`),
  KEY `hostname` (`hostname`),
  KEY `filter` (`start_time`,`end_time`,`reason_type`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`start_time` DIV 86400)
(PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_host_scheduleddowntimes`
--

DROP TABLE IF EXISTS `statusengine_host_scheduleddowntimes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_host_scheduleddowntimes` (
  `hostname` varchar(255) NOT NULL,
  `internal_downtime_id` int(10) unsigned NOT NULL,
  `scheduled_start_time` bigint(20) NOT NULL,
  `node_name` varchar(255) NOT NULL,
  `entry_time` bigint(20) NOT NULL,
  `author_name` varchar(255) DEFAULT NULL,
  `comment_data` varchar(1024) DEFAULT NULL,
  `triggered_by_id` int(10) unsigned DEFAULT NULL,
  `is_fixed` tinyint(1) DEFAULT 0,
  `duration` int(10) unsigned DEFAULT NULL,
  `scheduled_end_time` bigint(20) NOT NULL,
  `was_started` tinyint(1) DEFAULT 0,
  `actual_start_time` bigint(20) NOT NULL,
  PRIMARY KEY (`hostname`,`node_name`,`scheduled_start_time`,`internal_downtime_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_host_statehistory`
--

DROP TABLE IF EXISTS `statusengine_host_statehistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_host_statehistory` (
  `hostname` varchar(255) NOT NULL,
  `state_time` bigint(20) NOT NULL,
  `state_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `state_change` tinyint(1) DEFAULT 0,
  `state` smallint(2) DEFAULT 0,
  `is_hardstate` tinyint(1) DEFAULT 0,
  `current_check_attempt` smallint(3) DEFAULT 0,
  `max_check_attempts` smallint(3) DEFAULT 0,
  `last_state` smallint(2) DEFAULT 0,
  `last_hard_state` smallint(2) DEFAULT 0,
  `output` varchar(1024) DEFAULT NULL,
  `long_output` varchar(8192) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`state_time`,`state_time_usec`),
  KEY `hostname_time` (`hostname`,`state_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`state_time` DIV 86400)
(PARTITION `p_2025_30` VALUES LESS THAN (20297) ENGINE = InnoDB,
 PARTITION `p_2025_31` VALUES LESS THAN (20304) ENGINE = InnoDB,
 PARTITION `p_2025_32` VALUES LESS THAN (20311) ENGINE = InnoDB,
 PARTITION `p_2025_33` VALUES LESS THAN (20318) ENGINE = InnoDB,
 PARTITION `p_2025_34` VALUES LESS THAN (20325) ENGINE = InnoDB,
 PARTITION `p_2025_35` VALUES LESS THAN (20332) ENGINE = InnoDB,
 PARTITION `p_2025_36` VALUES LESS THAN (20339) ENGINE = InnoDB,
 PARTITION `p_2025_37` VALUES LESS THAN (20346) ENGINE = InnoDB,
 PARTITION `p_2025_38` VALUES LESS THAN (20353) ENGINE = InnoDB,
 PARTITION `p_2025_39` VALUES LESS THAN (20360) ENGINE = InnoDB,
 PARTITION `p_2025_40` VALUES LESS THAN (20367) ENGINE = InnoDB,
 PARTITION `p_2025_41` VALUES LESS THAN (20374) ENGINE = InnoDB,
 PARTITION `p_2025_42` VALUES LESS THAN (20381) ENGINE = InnoDB,
 PARTITION `p_2025_43` VALUES LESS THAN (20388) ENGINE = InnoDB,
 PARTITION `p_2025_44` VALUES LESS THAN (20395) ENGINE = InnoDB,
 PARTITION `p_2025_45` VALUES LESS THAN (20402) ENGINE = InnoDB,
 PARTITION `p_2025_46` VALUES LESS THAN (20409) ENGINE = InnoDB,
 PARTITION `p_2025_47` VALUES LESS THAN (20416) ENGINE = InnoDB,
 PARTITION `p_2025_48` VALUES LESS THAN (20423) ENGINE = InnoDB,
 PARTITION `p_2025_49` VALUES LESS THAN (20430) ENGINE = InnoDB,
 PARTITION `p_2025_50` VALUES LESS THAN (20437) ENGINE = InnoDB,
 PARTITION `p_2025_51` VALUES LESS THAN (20444) ENGINE = InnoDB,
 PARTITION `p_2025_52` VALUES LESS THAN (20451) ENGINE = InnoDB,
 PARTITION `p_2026_01` VALUES LESS THAN (20458) ENGINE = InnoDB,
 PARTITION `p_2026_02` VALUES LESS THAN (20465) ENGINE = InnoDB,
 PARTITION `p_2026_03` VALUES LESS THAN (20472) ENGINE = InnoDB,
 PARTITION `p_2026_04` VALUES LESS THAN (20479) ENGINE = InnoDB,
 PARTITION `p_2026_05` VALUES LESS THAN (20486) ENGINE = InnoDB,
 PARTITION `p_2026_06` VALUES LESS THAN (20493) ENGINE = InnoDB,
 PARTITION `p_2026_07` VALUES LESS THAN (20500) ENGINE = InnoDB,
 PARTITION `p_2026_08` VALUES LESS THAN (20507) ENGINE = InnoDB,
 PARTITION `p_2026_09` VALUES LESS THAN (20514) ENGINE = InnoDB,
 PARTITION `p_2026_10` VALUES LESS THAN (20521) ENGINE = InnoDB,
 PARTITION `p_2026_11` VALUES LESS THAN (20528) ENGINE = InnoDB,
 PARTITION `p_2026_12` VALUES LESS THAN (20535) ENGINE = InnoDB,
 PARTITION `p_2026_13` VALUES LESS THAN (20542) ENGINE = InnoDB,
 PARTITION `p_2026_14` VALUES LESS THAN (20549) ENGINE = InnoDB,
 PARTITION `p_2026_15` VALUES LESS THAN (20556) ENGINE = InnoDB,
 PARTITION `p_2026_16` VALUES LESS THAN (20563) ENGINE = InnoDB,
 PARTITION `p_2026_17` VALUES LESS THAN (20570) ENGINE = InnoDB,
 PARTITION `p_2026_18` VALUES LESS THAN (20577) ENGINE = InnoDB,
 PARTITION `p_2026_19` VALUES LESS THAN (20584) ENGINE = InnoDB,
 PARTITION `p_2026_20` VALUES LESS THAN (20591) ENGINE = InnoDB,
 PARTITION `p_2026_21` VALUES LESS THAN (20598) ENGINE = InnoDB,
 PARTITION `p_2026_22` VALUES LESS THAN (20605) ENGINE = InnoDB,
 PARTITION `p_2026_23` VALUES LESS THAN (20612) ENGINE = InnoDB,
 PARTITION `p_2026_24` VALUES LESS THAN (20619) ENGINE = InnoDB,
 PARTITION `p_2026_25` VALUES LESS THAN (20626) ENGINE = InnoDB,
 PARTITION `p_2026_26` VALUES LESS THAN (20633) ENGINE = InnoDB,
 PARTITION `p_2026_27` VALUES LESS THAN (20640) ENGINE = InnoDB,
 PARTITION `p_2026_28` VALUES LESS THAN (20647) ENGINE = InnoDB,
 PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_hostchecks`
--

DROP TABLE IF EXISTS `statusengine_hostchecks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_hostchecks` (
  `hostname` varchar(255) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `start_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `state` smallint(2) DEFAULT 0,
  `is_hardstate` tinyint(1) DEFAULT 0,
  `end_time` bigint(20) NOT NULL,
  `output` varchar(1024) DEFAULT NULL,
  `timeout` smallint(3) DEFAULT 0,
  `early_timeout` tinyint(1) DEFAULT 0,
  `latency` double DEFAULT 0,
  `execution_time` double DEFAULT 0,
  `perfdata` varchar(2048) DEFAULT NULL,
  `command` varchar(1024) DEFAULT NULL,
  `current_check_attempt` smallint(3) DEFAULT 0,
  `max_check_attempts` smallint(3) DEFAULT 0,
  `long_output` varchar(8192) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`start_time`,`start_time_usec`),
  KEY `hostname` (`hostname`,`start_time`),
  KEY `times` (`start_time`,`end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`start_time` DIV 86400)
(PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_hoststatus`
--

DROP TABLE IF EXISTS `statusengine_hoststatus`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_hoststatus` (
  `hostname` varchar(255) NOT NULL,
  `status_update_time` bigint(20) NOT NULL,
  `output` varchar(1024) DEFAULT NULL,
  `long_output` varchar(8192) DEFAULT NULL,
  `perfdata` varchar(2048) DEFAULT NULL,
  `current_state` smallint(5) unsigned DEFAULT 0,
  `current_check_attempt` smallint(5) unsigned DEFAULT 0,
  `max_check_attempts` smallint(5) unsigned DEFAULT 0,
  `last_check` bigint(20) NOT NULL,
  `next_check` bigint(20) NOT NULL,
  `is_passive_check` tinyint(1) DEFAULT 0,
  `last_state_change` bigint(20) NOT NULL,
  `last_hard_state_change` bigint(20) NOT NULL,
  `last_hard_state` smallint(5) unsigned DEFAULT 0,
  `is_hardstate` tinyint(1) DEFAULT 0,
  `last_notification` bigint(20) NOT NULL,
  `next_notification` bigint(20) NOT NULL,
  `notifications_enabled` tinyint(1) DEFAULT 0,
  `problem_has_been_acknowledged` tinyint(1) DEFAULT 0,
  `acknowledgement_type` smallint(5) unsigned DEFAULT 0,
  `passive_checks_enabled` tinyint(1) DEFAULT 0,
  `active_checks_enabled` tinyint(1) DEFAULT 0,
  `event_handler_enabled` tinyint(1) DEFAULT 0,
  `flap_detection_enabled` tinyint(1) DEFAULT 0,
  `is_flapping` tinyint(1) DEFAULT 0,
  `latency` double DEFAULT 0,
  `execution_time` double DEFAULT 0,
  `scheduled_downtime_depth` smallint(5) unsigned DEFAULT 0,
  `process_performance_data` tinyint(1) DEFAULT 0,
  `obsess_over_host` tinyint(1) DEFAULT 0,
  `normal_check_interval` int(10) unsigned DEFAULT 0,
  `retry_check_interval` int(10) unsigned DEFAULT 0,
  `check_timeperiod` varchar(255) DEFAULT NULL,
  `node_name` varchar(255) DEFAULT NULL,
  `last_time_up` bigint(20) NOT NULL,
  `last_time_down` bigint(20) NOT NULL,
  `last_time_unreachable` bigint(20) NOT NULL,
  `current_notification_number` int(10) unsigned DEFAULT 0,
  `percent_state_change` double DEFAULT 0,
  `event_handler` varchar(255) DEFAULT NULL,
  `check_command` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`hostname`),
  KEY `current_state_node` (`current_state`,`node_name`),
  KEY `issues` (`problem_has_been_acknowledged`,`scheduled_downtime_depth`,`current_state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_logentries`
--

DROP TABLE IF EXISTS `statusengine_logentries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_logentries` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `entry_time` bigint(20) NOT NULL,
  `logentry_type` int(11) DEFAULT 0,
  `logentry_data` varchar(2048) DEFAULT NULL,
  `node_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`,`entry_time`),
  KEY `logentries_se` (`entry_time`,`node_name`)
) ENGINE=InnoDB AUTO_INCREMENT=47737 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`entry_time` DIV 86400)
(PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_nodes`
--

DROP TABLE IF EXISTS `statusengine_nodes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_nodes` (
  `node_name` varchar(255) NOT NULL,
  `node_version` varchar(255) DEFAULT NULL,
  `node_start_time` bigint(20) NOT NULL,
  PRIMARY KEY (`node_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_perfdata`
--

DROP TABLE IF EXISTS `statusengine_perfdata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_perfdata` (
  `hostname` varchar(255) DEFAULT NULL,
  `service_description` varchar(255) DEFAULT NULL,
  `label` varchar(255) DEFAULT NULL,
  `timestamp` bigint(20) NOT NULL,
  `timestamp_unix` bigint(20) NOT NULL,
  `value` double DEFAULT NULL,
  `unit` varchar(10) DEFAULT NULL,
  KEY `metric` (`hostname`,`service_description`,`label`,`timestamp_unix`),
  KEY `timestamp_unix` (`timestamp_unix`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_service_acknowledgements`
--

DROP TABLE IF EXISTS `statusengine_service_acknowledgements`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_service_acknowledgements` (
  `service_description` varchar(255) NOT NULL,
  `entry_time` bigint(20) NOT NULL,
  `entry_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `hostname` varchar(255) DEFAULT NULL,
  `state` smallint(5) unsigned DEFAULT 0,
  `author_name` varchar(255) DEFAULT NULL,
  `comment_data` varchar(1024) DEFAULT NULL,
  `acknowledgement_type` smallint(5) unsigned DEFAULT 0,
  `is_sticky` tinyint(1) DEFAULT 0,
  `persistent_comment` tinyint(1) DEFAULT 0,
  `notify_contacts` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`service_description`,`entry_time`,`entry_time_usec`),
  KEY `servicename` (`hostname`,`service_description`),
  KEY `entry_time` (`entry_time`),
  KEY `servicedesc_time` (`service_description`,`entry_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_service_downtimehistory`
--

DROP TABLE IF EXISTS `statusengine_service_downtimehistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_service_downtimehistory` (
  `hostname` varchar(255) NOT NULL,
  `service_description` varchar(255) NOT NULL,
  `internal_downtime_id` int(10) unsigned NOT NULL,
  `scheduled_start_time` bigint(20) NOT NULL,
  `node_name` varchar(255) NOT NULL,
  `entry_time` bigint(20) NOT NULL,
  `entry_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `author_name` varchar(255) DEFAULT NULL,
  `comment_data` varchar(1024) DEFAULT NULL,
  `triggered_by_id` int(10) unsigned DEFAULT NULL,
  `is_fixed` tinyint(1) DEFAULT 0,
  `duration` int(10) unsigned DEFAULT NULL,
  `scheduled_end_time` bigint(20) NOT NULL,
  `was_started` tinyint(1) DEFAULT 0,
  `actual_start_time` bigint(20) NOT NULL,
  `actual_end_time` bigint(20) NOT NULL,
  `was_cancelled` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`hostname`,`service_description`,`node_name`,`scheduled_start_time`,`internal_downtime_id`),
  KEY `reports` (`service_description`,`entry_time`,`entry_time_usec`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`),
  KEY `report` (`service_description`,`scheduled_start_time`,`scheduled_end_time`,`was_cancelled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_service_notifications`
--

DROP TABLE IF EXISTS `statusengine_service_notifications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_service_notifications` (
  `service_description` varchar(255) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `start_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `hostname` varchar(255) DEFAULT NULL,
  `contact_name` varchar(1024) DEFAULT NULL,
  `command_name` varchar(1024) DEFAULT NULL,
  `command_args` varchar(1024) DEFAULT NULL,
  `state` smallint(2) DEFAULT 0,
  `end_time` bigint(20) NOT NULL,
  `reason_type` smallint(3) DEFAULT 0,
  `output` varchar(1024) DEFAULT NULL,
  `ack_author` varchar(255) DEFAULT NULL,
  `ack_data` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`service_description`,`start_time`,`start_time_usec`),
  KEY `start_time` (`start_time`),
  KEY `servicename` (`hostname`,`service_description`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`start_time` DIV 86400)
(PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_service_notifications_log`
--

DROP TABLE IF EXISTS `statusengine_service_notifications_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_service_notifications_log` (
  `hostname` varchar(255) NOT NULL,
  `service_description` varchar(255) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `start_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `end_time` bigint(20) NOT NULL,
  `state` smallint(6) unsigned DEFAULT 0,
  `reason_type` smallint(6) unsigned DEFAULT 0,
  `is_escalated` tinyint(1) NOT NULL DEFAULT 0,
  `contacts_notified_count` smallint(6) unsigned NOT NULL DEFAULT 0,
  `output` varchar(1024) DEFAULT NULL,
  `ack_author` varchar(1024) DEFAULT NULL,
  `ack_data` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`service_description`,`start_time`,`start_time_usec`),
  KEY `servicename` (`hostname`,`service_description`),
  KEY `filter` (`start_time`,`end_time`,`reason_type`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`start_time` DIV 86400)
(PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_service_scheduleddowntimes`
--

DROP TABLE IF EXISTS `statusengine_service_scheduleddowntimes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_service_scheduleddowntimes` (
  `hostname` varchar(255) NOT NULL,
  `service_description` varchar(255) NOT NULL,
  `internal_downtime_id` int(10) unsigned NOT NULL,
  `scheduled_start_time` bigint(20) NOT NULL,
  `node_name` varchar(255) NOT NULL,
  `entry_time` bigint(20) NOT NULL,
  `author_name` varchar(255) DEFAULT NULL,
  `comment_data` varchar(1024) DEFAULT NULL,
  `triggered_by_id` int(10) unsigned DEFAULT NULL,
  `is_fixed` tinyint(1) DEFAULT 0,
  `duration` int(10) unsigned DEFAULT NULL,
  `scheduled_end_time` bigint(20) NOT NULL,
  `was_started` tinyint(1) DEFAULT 0,
  `actual_start_time` bigint(20) NOT NULL,
  PRIMARY KEY (`hostname`,`service_description`,`node_name`,`scheduled_start_time`,`internal_downtime_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_service_statehistory`
--

DROP TABLE IF EXISTS `statusengine_service_statehistory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_service_statehistory` (
  `service_description` varchar(255) NOT NULL,
  `state_time` bigint(20) NOT NULL,
  `state_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `hostname` varchar(255) DEFAULT NULL,
  `state_change` tinyint(1) DEFAULT 0,
  `state` smallint(2) DEFAULT 0,
  `is_hardstate` tinyint(1) DEFAULT 0,
  `current_check_attempt` smallint(3) DEFAULT 0,
  `max_check_attempts` smallint(3) DEFAULT 0,
  `last_state` smallint(2) DEFAULT 0,
  `last_hard_state` smallint(2) DEFAULT 0,
  `output` varchar(1024) DEFAULT NULL,
  `long_output` varchar(8192) DEFAULT NULL,
  PRIMARY KEY (`service_description`,`state_time`,`state_time_usec`),
  KEY `host_servicename_time` (`hostname`,`service_description`,`state_time`),
  KEY `servicename_time` (`service_description`,`state_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`state_time` DIV 86400)
(PARTITION `p_2025_30` VALUES LESS THAN (20297) ENGINE = InnoDB,
 PARTITION `p_2025_31` VALUES LESS THAN (20304) ENGINE = InnoDB,
 PARTITION `p_2025_32` VALUES LESS THAN (20311) ENGINE = InnoDB,
 PARTITION `p_2025_33` VALUES LESS THAN (20318) ENGINE = InnoDB,
 PARTITION `p_2025_34` VALUES LESS THAN (20325) ENGINE = InnoDB,
 PARTITION `p_2025_35` VALUES LESS THAN (20332) ENGINE = InnoDB,
 PARTITION `p_2025_36` VALUES LESS THAN (20339) ENGINE = InnoDB,
 PARTITION `p_2025_37` VALUES LESS THAN (20346) ENGINE = InnoDB,
 PARTITION `p_2025_38` VALUES LESS THAN (20353) ENGINE = InnoDB,
 PARTITION `p_2025_39` VALUES LESS THAN (20360) ENGINE = InnoDB,
 PARTITION `p_2025_40` VALUES LESS THAN (20367) ENGINE = InnoDB,
 PARTITION `p_2025_41` VALUES LESS THAN (20374) ENGINE = InnoDB,
 PARTITION `p_2025_42` VALUES LESS THAN (20381) ENGINE = InnoDB,
 PARTITION `p_2025_43` VALUES LESS THAN (20388) ENGINE = InnoDB,
 PARTITION `p_2025_44` VALUES LESS THAN (20395) ENGINE = InnoDB,
 PARTITION `p_2025_45` VALUES LESS THAN (20402) ENGINE = InnoDB,
 PARTITION `p_2025_46` VALUES LESS THAN (20409) ENGINE = InnoDB,
 PARTITION `p_2025_47` VALUES LESS THAN (20416) ENGINE = InnoDB,
 PARTITION `p_2025_48` VALUES LESS THAN (20423) ENGINE = InnoDB,
 PARTITION `p_2025_49` VALUES LESS THAN (20430) ENGINE = InnoDB,
 PARTITION `p_2025_50` VALUES LESS THAN (20437) ENGINE = InnoDB,
 PARTITION `p_2025_51` VALUES LESS THAN (20444) ENGINE = InnoDB,
 PARTITION `p_2025_52` VALUES LESS THAN (20451) ENGINE = InnoDB,
 PARTITION `p_2026_01` VALUES LESS THAN (20458) ENGINE = InnoDB,
 PARTITION `p_2026_02` VALUES LESS THAN (20465) ENGINE = InnoDB,
 PARTITION `p_2026_03` VALUES LESS THAN (20472) ENGINE = InnoDB,
 PARTITION `p_2026_04` VALUES LESS THAN (20479) ENGINE = InnoDB,
 PARTITION `p_2026_05` VALUES LESS THAN (20486) ENGINE = InnoDB,
 PARTITION `p_2026_06` VALUES LESS THAN (20493) ENGINE = InnoDB,
 PARTITION `p_2026_07` VALUES LESS THAN (20500) ENGINE = InnoDB,
 PARTITION `p_2026_08` VALUES LESS THAN (20507) ENGINE = InnoDB,
 PARTITION `p_2026_09` VALUES LESS THAN (20514) ENGINE = InnoDB,
 PARTITION `p_2026_10` VALUES LESS THAN (20521) ENGINE = InnoDB,
 PARTITION `p_2026_11` VALUES LESS THAN (20528) ENGINE = InnoDB,
 PARTITION `p_2026_12` VALUES LESS THAN (20535) ENGINE = InnoDB,
 PARTITION `p_2026_13` VALUES LESS THAN (20542) ENGINE = InnoDB,
 PARTITION `p_2026_14` VALUES LESS THAN (20549) ENGINE = InnoDB,
 PARTITION `p_2026_15` VALUES LESS THAN (20556) ENGINE = InnoDB,
 PARTITION `p_2026_16` VALUES LESS THAN (20563) ENGINE = InnoDB,
 PARTITION `p_2026_17` VALUES LESS THAN (20570) ENGINE = InnoDB,
 PARTITION `p_2026_18` VALUES LESS THAN (20577) ENGINE = InnoDB,
 PARTITION `p_2026_19` VALUES LESS THAN (20584) ENGINE = InnoDB,
 PARTITION `p_2026_20` VALUES LESS THAN (20591) ENGINE = InnoDB,
 PARTITION `p_2026_21` VALUES LESS THAN (20598) ENGINE = InnoDB,
 PARTITION `p_2026_22` VALUES LESS THAN (20605) ENGINE = InnoDB,
 PARTITION `p_2026_23` VALUES LESS THAN (20612) ENGINE = InnoDB,
 PARTITION `p_2026_24` VALUES LESS THAN (20619) ENGINE = InnoDB,
 PARTITION `p_2026_25` VALUES LESS THAN (20626) ENGINE = InnoDB,
 PARTITION `p_2026_26` VALUES LESS THAN (20633) ENGINE = InnoDB,
 PARTITION `p_2026_27` VALUES LESS THAN (20640) ENGINE = InnoDB,
 PARTITION `p_2026_28` VALUES LESS THAN (20647) ENGINE = InnoDB,
 PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_servicechecks`
--

DROP TABLE IF EXISTS `statusengine_servicechecks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_servicechecks` (
  `service_description` varchar(255) NOT NULL,
  `start_time` bigint(20) NOT NULL,
  `start_time_usec` int(10) unsigned NOT NULL DEFAULT 0,
  `hostname` varchar(255) DEFAULT NULL,
  `state` smallint(2) DEFAULT 0,
  `is_hardstate` tinyint(1) DEFAULT 0,
  `end_time` bigint(20) NOT NULL,
  `output` varchar(1024) DEFAULT NULL,
  `timeout` smallint(3) DEFAULT 0,
  `early_timeout` tinyint(1) DEFAULT 0,
  `latency` double DEFAULT 0,
  `execution_time` double DEFAULT 0,
  `perfdata` varchar(2048) DEFAULT NULL,
  `command` varchar(1024) DEFAULT NULL,
  `current_check_attempt` smallint(3) DEFAULT 0,
  `max_check_attempts` smallint(3) DEFAULT 0,
  `long_output` varchar(8192) DEFAULT NULL,
  PRIMARY KEY (`service_description`,`start_time`,`start_time_usec`),
  KEY `servicename` (`hostname`,`service_description`,`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
 PARTITION BY RANGE (`start_time` DIV 86400)
(PARTITION `p_2026_29` VALUES LESS THAN (20654) ENGINE = InnoDB,
 PARTITION `p_2026_30` VALUES LESS THAN (20661) ENGINE = InnoDB,
 PARTITION `p_2026_31` VALUES LESS THAN (20668) ENGINE = InnoDB,
 PARTITION `p_2026_32` VALUES LESS THAN (20675) ENGINE = InnoDB,
 PARTITION `p_max` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_servicestatus`
--

DROP TABLE IF EXISTS `statusengine_servicestatus`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_servicestatus` (
  `hostname` varchar(255) NOT NULL,
  `service_description` varchar(255) NOT NULL,
  `status_update_time` bigint(20) NOT NULL,
  `output` varchar(1024) DEFAULT NULL,
  `long_output` varchar(8192) DEFAULT NULL,
  `perfdata` varchar(2048) DEFAULT NULL,
  `current_state` smallint(5) unsigned DEFAULT 0,
  `current_check_attempt` smallint(5) unsigned DEFAULT 0,
  `max_check_attempts` smallint(5) unsigned DEFAULT 0,
  `last_check` bigint(20) NOT NULL,
  `next_check` bigint(20) NOT NULL,
  `is_passive_check` tinyint(1) DEFAULT 0,
  `last_state_change` bigint(20) NOT NULL,
  `last_hard_state_change` bigint(20) NOT NULL,
  `last_hard_state` smallint(5) unsigned DEFAULT 0,
  `is_hardstate` tinyint(1) DEFAULT 0,
  `last_notification` bigint(20) NOT NULL,
  `next_notification` bigint(20) NOT NULL,
  `notifications_enabled` tinyint(1) DEFAULT 0,
  `problem_has_been_acknowledged` tinyint(1) DEFAULT 0,
  `acknowledgement_type` smallint(5) unsigned DEFAULT 0,
  `passive_checks_enabled` tinyint(1) DEFAULT 0,
  `active_checks_enabled` tinyint(1) DEFAULT 0,
  `event_handler_enabled` tinyint(1) DEFAULT 0,
  `flap_detection_enabled` tinyint(1) DEFAULT 0,
  `is_flapping` tinyint(1) DEFAULT 0,
  `latency` double DEFAULT 0,
  `execution_time` double DEFAULT 0,
  `scheduled_downtime_depth` smallint(5) unsigned DEFAULT 0,
  `process_performance_data` tinyint(1) DEFAULT 0,
  `obsess_over_service` tinyint(1) DEFAULT 0,
  `normal_check_interval` int(10) unsigned DEFAULT 0,
  `retry_check_interval` int(10) unsigned DEFAULT 0,
  `check_timeperiod` varchar(255) DEFAULT NULL,
  `node_name` varchar(255) DEFAULT NULL,
  `last_time_ok` bigint(20) NOT NULL,
  `last_time_warning` bigint(20) NOT NULL,
  `last_time_critical` bigint(20) NOT NULL,
  `last_time_unknown` bigint(20) NOT NULL,
  `current_notification_number` int(10) unsigned DEFAULT 0,
  `percent_state_change` double DEFAULT 0,
  `event_handler` varchar(255) DEFAULT NULL,
  `check_command` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`hostname`,`service_description`),
  KEY `service_description` (`service_description`),
  KEY `current_state_node` (`current_state`,`node_name`),
  KEY `issues` (`problem_has_been_acknowledged`,`scheduled_downtime_depth`,`current_state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_tasks`
--

DROP TABLE IF EXISTS `statusengine_tasks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_tasks` (
  `uuid` varchar(255) DEFAULT NULL,
  `node_name` varchar(255) DEFAULT NULL,
  `entry_time` bigint(20) NOT NULL,
  `type` varchar(255) DEFAULT NULL,
  `payload` varchar(8192) DEFAULT NULL,
  KEY `uuid` (`uuid`),
  KEY `node_name` (`node_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statusengine_users`
--

DROP TABLE IF EXISTS `statusengine_users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statusengine_users` (
  `username` varchar(255) DEFAULT NULL,
  `password` varchar(255) DEFAULT NULL,
  KEY `username` (`username`,`password`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspagegroup_categories`
--

DROP TABLE IF EXISTS `statuspagegroup_categories`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspagegroup_categories` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statuspagegroup_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `modified` datetime NOT NULL,
  `created` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `statuspagegroup_id` (`statuspagegroup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspagegroup_collections`
--

DROP TABLE IF EXISTS `statuspagegroup_collections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspagegroup_collections` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statuspagegroup_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `modified` datetime NOT NULL,
  `created` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `statuspagegroup_id` (`statuspagegroup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspagegroups`
--

DROP TABLE IF EXISTS `statuspagegroups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspagegroups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `container_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `description` varchar(1000) DEFAULT NULL,
  `additional_information` varchar(2048) NOT NULL DEFAULT '',
  `further_information` varchar(2048) NOT NULL DEFAULT '',
  `show_ticker` tinyint(1) NOT NULL DEFAULT 1,
  `modified` datetime NOT NULL,
  `created` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspages`
--

DROP TABLE IF EXISTS `statuspages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspages` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `uuid` varchar(37) DEFAULT NULL,
  `container_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `description` varchar(1000) DEFAULT NULL,
  `public_title` varchar(255) DEFAULT NULL,
  `public_identifier` varchar(255) DEFAULT NULL,
  `public` tinyint(1) NOT NULL DEFAULT 0,
  `show_downtimes` tinyint(1) NOT NULL DEFAULT 0,
  `show_downtime_comments` tinyint(1) NOT NULL DEFAULT 0,
  `show_acknowledgements` tinyint(1) NOT NULL DEFAULT 0,
  `show_acknowledgement_comments` tinyint(1) NOT NULL DEFAULT 0,
  `created` datetime NOT NULL,
  `modified` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `public_identifier` (`public_identifier`),
  KEY `uuid` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspages_to_hostgroups`
--

DROP TABLE IF EXISTS `statuspages_to_hostgroups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspages_to_hostgroups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statuspage_id` int(11) NOT NULL,
  `hostgroup_id` int(11) NOT NULL,
  `display_alias` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `statuspage_id` (`statuspage_id`),
  KEY `hostgroup_id` (`hostgroup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspages_to_hosts`
--

DROP TABLE IF EXISTS `statuspages_to_hosts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspages_to_hosts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statuspage_id` int(11) NOT NULL,
  `host_id` int(11) NOT NULL,
  `display_alias` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `statuspage_id` (`statuspage_id`),
  KEY `host_id` (`host_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspages_to_servicegroups`
--

DROP TABLE IF EXISTS `statuspages_to_servicegroups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspages_to_servicegroups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statuspage_id` int(11) NOT NULL,
  `servicegroup_id` int(11) NOT NULL,
  `display_alias` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `statuspage_id` (`statuspage_id`),
  KEY `servicegroup_id` (`servicegroup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspages_to_services`
--

DROP TABLE IF EXISTS `statuspages_to_services`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspages_to_services` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statuspage_id` int(11) NOT NULL,
  `service_id` int(11) NOT NULL,
  `display_alias` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `statuspage_id` (`statuspage_id`),
  KEY `service_id` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `statuspages_to_statuspagegroups`
--

DROP TABLE IF EXISTS `statuspages_to_statuspagegroups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statuspages_to_statuspagegroups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statuspagegroup_id` int(11) NOT NULL,
  `collection_id` int(11) NOT NULL,
  `category_id` int(11) NOT NULL,
  `statuspage_id` int(11) NOT NULL,
  `modified` datetime NOT NULL,
  `created` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `statuspagegroup_id` (`statuspagegroup_id`),
  KEY `collection_id` (`collection_id`),
  KEY `category_id` (`category_id`),
  KEY `statuspage_id` (`statuspage_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-07-31 19:09:13
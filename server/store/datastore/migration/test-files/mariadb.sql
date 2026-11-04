-- MariaDB fixture for TestMigrate: the rows of postgres.sql at the schema of
-- Woodpecker v3.9.0. Regenerate with scripts/gen-mariadb-fixture.sh.
/*M!999999\- enable the sandbox mode */ 

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*M!100616 SET @OLD_NOTE_VERBOSITY=@@NOTE_VERBOSITY, NOTE_VERBOSITY=0 */;
DROP TABLE IF EXISTS `agents`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `agents` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `created` bigint(20) DEFAULT NULL,
  `updated` bigint(20) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `owner_id` bigint(20) DEFAULT NULL,
  `token` varchar(255) DEFAULT NULL,
  `last_contact` bigint(20) DEFAULT NULL,
  `last_work` bigint(20) DEFAULT NULL,
  `platform` varchar(100) DEFAULT NULL,
  `backend` varchar(100) DEFAULT NULL,
  `capacity` int(11) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `no_schedule` tinyint(1) DEFAULT NULL,
  `custom_labels` text DEFAULT NULL,
  `org_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `IDX_agents_org_id` (`org_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `agents` WRITE;
/*!40000 ALTER TABLE `agents` DISABLE KEYS */;
INSERT INTO `agents` VALUES (1,1641630000,1641630000,'agent-1',1,'agent_token_abc123xyz',1641630000,NULL,'linux','docker',2,'1.0.0',0,NULL,-1);
INSERT INTO `agents` VALUES (2,1641630100,1641630100,'agent-2',1,'agent_token_def456uvw',1641630100,NULL,'linux','docker',4,'1.0.0',0,NULL,-1);
INSERT INTO `agents` VALUES (3,1641630200,1641630200,'agent-3',2,'agent_token_ghi789rst',1641630200,NULL,'linux','kubernetes',8,'1.0.1',0,NULL,-1);
/*!40000 ALTER TABLE `agents` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `configs` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `repo_id` bigint(20) DEFAULT NULL,
  `hash` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `data` longblob DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_configs_s` (`repo_id`,`hash`,`name`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `configs` WRITE;
/*!40000 ALTER TABLE `configs` DISABLE KEYS */;
INSERT INTO `configs` VALUES (1,105,'ec8ca9529d6081e631aec26175b26ac91699395b96b9c5fc1f3af6d3aef5d3a8','woodpecker','clone:\n  git:\n    image: woodpeckerci/plugin-git:test\n\nsteps:\n  Print:\n    image: print/env\n    secrets: [ AAAAAAAAAAAAAAAAAAAAAAAAAA ]');
/*!40000 ALTER TABLE `configs` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `crons`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `crons` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `repo_id` bigint(20) DEFAULT NULL,
  `creator_id` bigint(20) DEFAULT NULL,
  `next_exec` bigint(20) DEFAULT NULL,
  `schedule` varchar(255) NOT NULL,
  `created` bigint(20) NOT NULL DEFAULT 0,
  `branch` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_crons_s` (`name`,`repo_id`),
  KEY `IDX_crons_name` (`name`),
  KEY `IDX_crons_repo_id` (`repo_id`),
  KEY `IDX_crons_creator_id` (`creator_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `crons` WRITE;
/*!40000 ALTER TABLE `crons` DISABLE KEYS */;
INSERT INTO `crons` VALUES (1,'nightly-build',105,1,1641686400,'0 0 * * *',1641630600,'master');
/*!40000 ALTER TABLE `crons` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `forges`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `forges` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `type` varchar(250) DEFAULT NULL,
  `url` varchar(500) DEFAULT NULL,
  `o_auth_client_i_d` varchar(250) DEFAULT NULL,
  `o_auth_client_secret` varchar(250) DEFAULT NULL,
  `skip_verify` tinyint(1) DEFAULT NULL,
  `oauth_host` varchar(250) DEFAULT NULL,
  `additional_options` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `forges` WRITE;
/*!40000 ALTER TABLE `forges` DISABLE KEYS */;
INSERT INTO `forges` VALUES (1,'gitea','http://100.114.106.50:3000','6e9119df-a86d-4fe0-b392-fe125d7a265f','gto_bagkxxp5yio7npmj7uzrf5neyyalfbqykfmri3ryqfpgvlylqwsa',0,'','{}');
/*!40000 ALTER TABLE `forges` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `log_entries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `log_entries` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `step_id` bigint(20) DEFAULT NULL,
  `time` bigint(20) DEFAULT NULL,
  `line` int(11) DEFAULT NULL,
  `data` longblob DEFAULT NULL,
  `created` bigint(20) DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `IDX_log_entries_step_id` (`step_id`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `log_entries` WRITE;
/*!40000 ALTER TABLE `log_entries` DISABLE KEYS */;
INSERT INTO `log_entries` VALUES (1,2,0,0,'StepName: clone',1641630525,0);
INSERT INTO `log_entries` VALUES (2,2,0,1,'StepType: clone',1641630525,0);
INSERT INTO `log_entries` VALUES (3,2,0,2,'StepUUID: 01J1Q4ND22YKSJ1FZD6TS2443W',1641630525,0);
INSERT INTO `log_entries` VALUES (4,2,0,3,'StepCommands:',1641630525,0);
INSERT INTO `log_entries` VALUES (5,2,0,4,'------------------',1641630525,0);
INSERT INTO `log_entries` VALUES (6,2,0,5,'',1641630525,0);
INSERT INTO `log_entries` VALUES (7,2,0,6,'------------------',1641630525,0);
INSERT INTO `log_entries` VALUES (8,2,0,7,'',1641630525,0);
INSERT INTO `log_entries` VALUES (9,3,0,0,'StepName: Print',1641630526,0);
INSERT INTO `log_entries` VALUES (10,3,0,1,'StepType: commands',1641630526,0);
INSERT INTO `log_entries` VALUES (11,3,0,2,'StepUUID: 01J1Q4ND22YKSJ1FZD6W98ZW0G',1641630526,0);
INSERT INTO `log_entries` VALUES (12,3,0,3,'StepCommands:',1641630526,0);
INSERT INTO `log_entries` VALUES (13,3,0,4,'------------------',1641630526,0);
INSERT INTO `log_entries` VALUES (14,3,0,5,'print env command',1641630526,0);
INSERT INTO `log_entries` VALUES (15,3,0,6,'------------------',1641630526,0);
INSERT INTO `log_entries` VALUES (16,3,0,7,'',1641630526,0);
/*!40000 ALTER TABLE `log_entries` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `migration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `migration` (
  `id` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `migration` WRITE;
/*!40000 ALTER TABLE `migration` DISABLE KEYS */;
INSERT INTO `migration` VALUES ('SCHEMA_INIT','');
INSERT INTO `migration` VALUES ('legacy-to-xormigrate','');
INSERT INTO `migration` VALUES ('add-org-id','');
INSERT INTO `migration` VALUES ('alter-table-tasks-update-type-of-task-data','');
INSERT INTO `migration` VALUES ('alter-table-config-update-type-of-config-data','');
INSERT INTO `migration` VALUES ('remove-plugin-only-option-from-secrets-table','');
INSERT INTO `migration` VALUES ('convert-to-new-pipeline-error-format','');
INSERT INTO `migration` VALUES ('rename-link-to-url','');
INSERT INTO `migration` VALUES ('clean-registry-pipeline','');
INSERT INTO `migration` VALUES ('set-forge-id','');
INSERT INTO `migration` VALUES ('unify-columns-tables','');
INSERT INTO `migration` VALUES ('alter-table-registries-fix-required-fields','');
INSERT INTO `migration` VALUES ('cron-without-sec','');
INSERT INTO `migration` VALUES ('rename-start-end-time','');
INSERT INTO `migration` VALUES ('fix-v31-registries','');
INSERT INTO `migration` VALUES ('remove-old-migrations-of-v1','');
INSERT INTO `migration` VALUES ('add-org-agents','');
INSERT INTO `migration` VALUES ('add-custom-labels-to-agent','');
INSERT INTO `migration` VALUES ('split-trusted','');
INSERT INTO `migration` VALUES ('correct-potential-corrupt-orgs-users-relation','');
INSERT INTO `migration` VALUES ('gated-to-require-approval','');
INSERT INTO `migration` VALUES ('remove-repo-netrc-only-trusted','');
INSERT INTO `migration` VALUES ('rename-token-fields','');
INSERT INTO `migration` VALUES ('set-new-defaults-for-require-approval','');
INSERT INTO `migration` VALUES ('remove-repo-scm','');
INSERT INTO `migration` VALUES ('unsanitize-org-and-user-names','');
INSERT INTO `migration` VALUES ('replace-zero-forge-ids-in-orgs','');
/*!40000 ALTER TABLE `migration` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `orgs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `orgs` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `forge_id` bigint(20) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `is_user` tinyint(1) DEFAULT NULL,
  `private` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_orgs_s` (`forge_id`,`name`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `orgs` WRITE;
/*!40000 ALTER TABLE `orgs` DISABLE KEYS */;
INSERT INTO `orgs` VALUES (1,1,'2',0,0);
INSERT INTO `orgs` VALUES (2,1,'test',1,0);
/*!40000 ALTER TABLE `orgs` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `perms`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `perms` (
  `user_id` bigint(20) NOT NULL,
  `repo_id` bigint(20) NOT NULL,
  `pull` tinyint(1) DEFAULT NULL,
  `push` tinyint(1) DEFAULT NULL,
  `admin` tinyint(1) DEFAULT NULL,
  `synced` bigint(20) DEFAULT NULL,
  `created` bigint(20) DEFAULT NULL,
  `updated` bigint(20) DEFAULT NULL,
  UNIQUE KEY `UQE_perms_s` (`user_id`,`repo_id`),
  KEY `IDX_perms_user_id` (`user_id`),
  KEY `IDX_perms_repo_id` (`repo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `perms` WRITE;
/*!40000 ALTER TABLE `perms` DISABLE KEYS */;
INSERT INTO `perms` VALUES (1,1,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,2,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,3,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,4,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,5,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,6,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,7,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,8,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,9,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,10,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,11,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,12,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,13,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,14,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,15,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,16,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,17,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,18,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,19,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,20,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,21,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,22,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,23,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,24,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,25,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,26,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,27,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,28,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,29,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,30,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,31,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,32,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,33,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,34,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,35,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,36,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,37,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,38,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,39,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,40,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,41,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,42,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,43,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,44,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,45,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,46,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,47,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,48,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,49,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,50,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,51,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,52,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,53,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,54,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,55,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,56,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,57,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,58,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,59,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,60,1,1,1,1641626844,NULL,NULL);
INSERT INTO `perms` VALUES (1,105,1,1,1,1641630452,NULL,NULL);
INSERT INTO `perms` VALUES (1,115,1,1,1,1641630451,NULL,NULL);
/*!40000 ALTER TABLE `perms` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `pipeline_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `pipeline_configs` (
  `config_id` bigint(20) NOT NULL,
  `pipeline_id` bigint(20) NOT NULL,
  UNIQUE KEY `UQE_pipeline_configs_s` (`config_id`,`pipeline_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `pipeline_configs` WRITE;
/*!40000 ALTER TABLE `pipeline_configs` DISABLE KEYS */;
INSERT INTO `pipeline_configs` VALUES (1,1);
/*!40000 ALTER TABLE `pipeline_configs` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `pipelines`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `pipelines` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `repo_id` bigint(20) DEFAULT NULL,
  `number` bigint(20) DEFAULT NULL,
  `author` varchar(255) DEFAULT NULL,
  `parent` bigint(20) DEFAULT NULL,
  `event` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `errors` text DEFAULT NULL,
  `created` bigint(20) NOT NULL DEFAULT 0,
  `updated` bigint(20) NOT NULL DEFAULT 0,
  `started` bigint(20) DEFAULT NULL,
  `finished` bigint(20) DEFAULT NULL,
  `deploy` varchar(255) DEFAULT NULL,
  `deploy_task` varchar(255) DEFAULT NULL,
  `commit` varchar(255) DEFAULT NULL,
  `branch` varchar(255) DEFAULT NULL,
  `ref` varchar(255) DEFAULT NULL,
  `refspec` varchar(255) DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL,
  `message` text DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `sender` varchar(255) DEFAULT NULL,
  `avatar` varchar(500) DEFAULT NULL,
  `email` varchar(500) DEFAULT NULL,
  `forge_url` varchar(255) DEFAULT NULL,
  `reviewer` varchar(255) DEFAULT NULL,
  `reviewed` bigint(20) DEFAULT NULL,
  `changed_files` longtext DEFAULT NULL,
  `additional_variables` text DEFAULT NULL,
  `pr_labels` text DEFAULT NULL,
  `is_prerelease` tinyint(1) DEFAULT NULL,
  `from_fork` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_pipelines_s` (`repo_id`,`number`),
  KEY `IDX_pipelines_author` (`author`),
  KEY `IDX_pipelines_status` (`status`),
  KEY `IDX_pipelines_repo_id` (`repo_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `pipelines` WRITE;
/*!40000 ALTER TABLE `pipelines` DISABLE KEYS */;
INSERT INTO `pipelines` VALUES (1,105,1,'test',0,'push','failure',NULL,1641630525,0,1641630525,1641630527,'',NULL,'24bf205107cea48b92bc6444e18e40d21733a594','master','refs/heads/master','','','„.woodpecker.yml“ hinzufügen\\n',1641630525,'test','http://10.40.8.5:3000/avatars/d6c72f5d7e2a070b52e1194969df2cfe','test@test.test','http://10.40.8.5:3000/2/settings/compare/3fee083df05667d525878b5fcbd4eaf2a121c559...24bf205107cea48b92bc6444e18e40d21733a594','',0,'[\".woodpecker.yml\"]\\n',NULL,NULL,NULL,NULL);
/*!40000 ALTER TABLE `pipelines` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `redirections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `redirections` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `repo_id` bigint(20) DEFAULT NULL,
  `repo_full_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_redirections_repo_full_name` (`repo_full_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `redirections` WRITE;
/*!40000 ALTER TABLE `redirections` DISABLE KEYS */;
/*!40000 ALTER TABLE `redirections` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `registries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `registries` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `org_id` bigint(20) NOT NULL DEFAULT 0,
  `repo_id` bigint(20) NOT NULL DEFAULT 0,
  `address` varchar(255) NOT NULL,
  `username` varchar(2000) DEFAULT NULL,
  `password` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_registries_s` (`org_id`,`repo_id`,`address`),
  KEY `IDX_registries_org_id` (`org_id`),
  KEY `IDX_registries_repo_id` (`repo_id`),
  KEY `IDX_registries_address` (`address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `registries` WRITE;
/*!40000 ALTER TABLE `registries` DISABLE KEYS */;
/*!40000 ALTER TABLE `registries` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `repos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `repos` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) DEFAULT NULL,
  `forge_id` bigint(20) DEFAULT NULL,
  `forge_remote_id` varchar(255) DEFAULT NULL,
  `org_id` bigint(20) DEFAULT NULL,
  `owner` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `full_name` varchar(255) DEFAULT NULL,
  `avatar` varchar(500) DEFAULT NULL,
  `forge_url` varchar(1000) DEFAULT NULL,
  `clone` varchar(1000) DEFAULT NULL,
  `clone_ssh` varchar(1000) DEFAULT NULL,
  `branch` varchar(500) DEFAULT NULL,
  `pr_enabled` tinyint(1) DEFAULT 1,
  `timeout` bigint(20) DEFAULT NULL,
  `visibility` varchar(10) DEFAULT NULL,
  `private` tinyint(1) DEFAULT NULL,
  `trusted` text DEFAULT NULL,
  `require_approval` varchar(50) DEFAULT NULL,
  `approval_allowed_users` text DEFAULT NULL,
  `active` tinyint(1) DEFAULT NULL,
  `allow_pr` tinyint(1) DEFAULT NULL,
  `allow_deploy` tinyint(1) DEFAULT NULL,
  `config_path` varchar(500) DEFAULT NULL,
  `hash` varchar(500) DEFAULT NULL,
  `cancel_previous_pipeline_events` text DEFAULT NULL,
  `netrc_trusted` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_repos_name` (`owner`,`name`),
  UNIQUE KEY `UQE_repos_full_name` (`full_name`),
  KEY `IDX_repos_user_id` (`user_id`),
  KEY `IDX_repos_org_id` (`org_id`)
) ENGINE=InnoDB AUTO_INCREMENT=116 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `repos` WRITE;
/*!40000 ALTER TABLE `repos` DISABLE KEYS */;
INSERT INTO `repos` VALUES (105,1,1,NULL,1,'2','settings','2/settings','http://10.40.8.5:3000/avatars/c81e728d9d4c2f636f067f89cc14862c','http://10.40.8.5:3000/2/settings','http://10.40.8.5:3000/2/settings.git',NULL,'master',1,60,'public',0,'{\"network\":false,\"volumes\":false,\"security\":false}','forks',NULL,1,1,NULL,'.woodpecker.yml','3OQA7X5CNGPTILDYLQSJFDML6U2W7UUFBPPP2G2LRBG3WETAYZLA====',NULL,NULL);
INSERT INTO `repos` VALUES (115,1,1,NULL,1,'2','testCIservices','2/testCIservices','http://10.40.8.5:3000/avatars/c81e728d9d4c2f636f067f89cc14862c','http://10.40.8.5:3000/2/testCIservices','http://10.40.8.5:3000/2/testCIservices.git',NULL,'master',1,60,'public',0,'{\"network\":false,\"volumes\":false,\"security\":false}','forks',NULL,1,1,NULL,'.woodpecker.yml','FOUXTSNL2GXK7JP2SQQJVWVAS6J4E4SGIQYPAHEJBIFPVR46LLDA====',NULL,NULL);
/*!40000 ALTER TABLE `repos` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `secrets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `secrets` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `org_id` bigint(20) NOT NULL DEFAULT 0,
  `repo_id` bigint(20) NOT NULL DEFAULT 0,
  `name` varchar(255) NOT NULL,
  `value` text DEFAULT NULL,
  `images` text DEFAULT NULL,
  `events` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_secrets_s` (`org_id`,`repo_id`,`name`),
  KEY `IDX_secrets_name` (`name`),
  KEY `IDX_secrets_org_id` (`org_id`),
  KEY `IDX_secrets_repo_id` (`repo_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `secrets` WRITE;
/*!40000 ALTER TABLE `secrets` DISABLE KEYS */;
INSERT INTO `secrets` VALUES (1,0,105,'wow','test','null\\n','[\"push\",\"tag\",\"deployment\",\"pull_request\"]\\n');
INSERT INTO `secrets` VALUES (2,0,105,'n','n','null\\n','[\"deployment\"]\\n');
INSERT INTO `secrets` VALUES (3,0,105,'abc','edf','null\\n','[\"push\"]\\n');
INSERT INTO `secrets` VALUES (4,0,105,'quak','fdsa','null\\n','[\"pull_request\"]\\n');
/*!40000 ALTER TABLE `secrets` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `server_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `server_configs` (
  `key` varchar(255) NOT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `server_configs` WRITE;
/*!40000 ALTER TABLE `server_configs` DISABLE KEYS */;
INSERT INTO `server_configs` VALUES ('jwt-secret','GKQDHRJXNN5ONIMOHJUMYDBR4IYIH46M6E5HOXX3Q2KEVZ35GM5Q====');
INSERT INTO `server_configs` VALUES ('signature-private-key','1fe3b71c87d7f89fa878306028cf08d66020ef6cafc2af90d05c40ebd03eee3c93189d2a3c46fe5292afc33e9237615ed595ee3d588dce431d5f6848b6a9bf77');
/*!40000 ALTER TABLE `server_configs` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `steps`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `steps` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `uuid` varchar(255) DEFAULT NULL,
  `pipeline_id` bigint(20) DEFAULT NULL,
  `pid` int(11) DEFAULT NULL,
  `ppid` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `state` varchar(255) DEFAULT NULL,
  `error` text DEFAULT NULL,
  `failure` varchar(255) DEFAULT NULL,
  `exit_code` int(11) DEFAULT NULL,
  `started` bigint(20) DEFAULT NULL,
  `finished` bigint(20) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_steps_s` (`pipeline_id`,`pid`),
  KEY `IDX_steps_uuid` (`uuid`),
  KEY `IDX_steps_pipeline_id` (`pipeline_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `steps` WRITE;
/*!40000 ALTER TABLE `steps` DISABLE KEYS */;
INSERT INTO `steps` VALUES (2,NULL,1,2,1,'git','success','',NULL,0,1641630525,1641630527,NULL);
INSERT INTO `steps` VALUES (3,NULL,1,3,1,'Print','skipped','',NULL,0,0,0,NULL);
/*!40000 ALTER TABLE `steps` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `tasks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `tasks` (
  `id` varchar(255) NOT NULL,
  `data` longblob DEFAULT NULL,
  `labels` text DEFAULT NULL,
  `dependencies` text DEFAULT NULL,
  `run_on` text DEFAULT NULL,
  `dependencies_status` text DEFAULT NULL,
  `agent_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_tasks_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `tasks` WRITE;
/*!40000 ALTER TABLE `tasks` DISABLE KEYS */;
/*!40000 ALTER TABLE `tasks` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `forge_id` bigint(20) DEFAULT NULL,
  `forge_remote_id` varchar(255) DEFAULT NULL,
  `login` varchar(255) DEFAULT NULL,
  `access_token` text DEFAULT NULL,
  `refresh_token` text DEFAULT NULL,
  `expiry` bigint(20) DEFAULT NULL,
  `email` varchar(500) DEFAULT NULL,
  `avatar` varchar(500) DEFAULT NULL,
  `admin` tinyint(1) DEFAULT NULL,
  `hash` varchar(500) DEFAULT NULL,
  `org_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_users_login` (`login`),
  UNIQUE KEY `UQE_users_hash` (`hash`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` VALUES (1,1,NULL,'test','eyJhbGciOiJSUzI1NiIsImtpZCI6IldmbUJ1c2Q0RndUVWRmMjc2NHowUWlEYlJ3TnRBcU5pNVlXS1U1c2k0eEEiLCJ0eXAiOiJKV1QifQ.eyJnbnQiOjEsInR0IjowLCJleHAiOjE2NDE2MzQxMjcsImlhdCI6MTY0MTYzMDUyN30.Fu0wUP-08NpPjq737y6HOeyKN_-_SE4iOZr5yrH7S8Jrz8nIuNKfU7AvlypeMSJ7wo8e3cSTadbSH1polZuFv-Nb1AqWDDXeuXudm61BkF96sTslbSHd0nF7cOy6hqCfIAfQLQpqZTJZ4E26oOSSJxPfOOntOWhlEejRl5F-flXAoYAQLegHxdn9IfYJeM1eanZqF4k6dT9hthFp9v4fmUjODPPfHip_iS7ckPonP1E4-8KeNkU3O-lIS1fgrsbCDA8531FXIGB0U7cSur7H0picKGL6WSzAErPGntlNlQWYB5JedDtLN9Ionxy1Y9LKQON76XYL4gM1Ji98RCEXggVqd7TW0B1fGV-Jve2hU3fKaDyQywsCJp36mpnVaqb5eiTssncHixAwZE0C4yh_XsTd-WoVhsbqlEuDfPTjrtAK94mSzHJTcO3fbtE9L-MoPevQIPM7Yog0i2Xn1oPUCDXVXsV2yJriBiI_r2xbG0nz5Bwn8KAFZ0dNGJ7T9urqKaKMh9guE4jgYLIpRpod_Fd13_GAK0ebgF2CZJdjJT7eEGhzzcg4uFpFdIXL2kNgVN1D6YLMPw3HhVg7_MIfASbJgpcppFhYa4Fk-OpchL5-e_mMyeWogvaJA2wSpyY1f5zJlBnFuIyk_OdV0TwQ3b_TjutehsiibT9WRpOK8h8','eyJhbGciOiJSUzI1NiIsImtpZCI6IldmbUJ1c2Q0RndUVWRmMjc2NHowUWlEYlJ3TnRBcU5pNVlXS1U1c2k0eEEiLCJ0eXAiOiJKV1QifQ.eyJnbnQiOjEsInR0IjoxLCJleHAiOjE2NDQyNTg1MjcsImlhdCI6MTY0MTYzMDUyN30.iVtIGQ6VTgRI8L3xFD_YNvVBGZ6kdFb3ERdyOCIHC_CHhOEpZxVGawMGnNNooqbNdmOqJQ0RLJyiAirEKdxSVrtWvqub6uVMjjpeBylE1sAFymCGNJQf77dKvgPHW3QY5FvOSoOoNcRU2g99Bx8sbZhiI12GnNOB-abazrzICpOUikiTdb2ri3w_TNF2Ibrn-itSa1yuhmTrVpqXt_CT4MEfteiDmgjyqonmk-J_BqbcriF3DKAvrXNK1VKVU7xODcFSIRizlgA2kDmnpMT3Oo-Z1I37TFIGAuDOTgcceOPa7rXg_Mfd_jhL7bSH1BI4RsK0rgde3NaCQlU2n7yVOYGbJCSsSWwSAi-gCjjuTTPnQWe3ep3IWrB73_7tKG2_x7YxZ1nQCSFKouA5rZH4g6yoV8wdJh8_bX2Z64-MJBUl8E7JGM2urA5GY1abo0GZ6ZuQi2JS5WnG1iTL9pFlmOoTpN1DKtNE2PUE90GJwi0qGeACif9uJBXQPDAgKk7fbUxKYQobc6ko2CJ1isoRjbi8-GsJ9lhw7tXno5zfAvN3eps9SYgmIRNh0t_vx-LMBezSTSEcTJpv-7Ap6F10GD3E9KmGcYrOMvdtaYgkWFXO6rh49uElUVid-C1tNVpKjnj7ewUosQo9MHSn-d5l1df0rJSueXcaUMSqRSrEzqQ',1641634127,'test@test.test','http://10.40.8.5:3000/avatars/d6c72f5d7e2a070b52e1194969df2cfe',1,'OBW2OF5QH3NMCYJ44VU5B5YEQ5LHZLTFW2FDSAJ4R4JVZ4HWSNVQ====',2);
INSERT INTO `users` VALUES (2,1,NULL,'user2','eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMiIsImlhdCI6MTY0MTYzMDUyNywiZXhwIjoxNjQxNjM0MTI3fQ.example_token_user2','eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMiIsImlhdCI6MTY0MTYzMDUyNywiZXhwIjoxNjQ0MjU4NTI3fQ.example_secret_user2',1641634127,'user2@test.test','http://10.40.8.5:3000/avatars/default2',0,'HASH2EXAMPLEHASH2EXAMPLEHASH2EXAMPLEHASH2EXAMPLE====',2);
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
DROP TABLE IF EXISTS `workflows`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `workflows` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `pipeline_id` bigint(20) DEFAULT NULL,
  `pid` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `state` varchar(255) DEFAULT NULL,
  `error` text DEFAULT NULL,
  `started` bigint(20) DEFAULT NULL,
  `finished` bigint(20) DEFAULT NULL,
  `agent_id` bigint(20) DEFAULT NULL,
  `platform` varchar(255) DEFAULT NULL,
  `environ` text DEFAULT NULL,
  `axis_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UQE_workflows_s` (`pipeline_id`,`pid`),
  KEY `IDX_workflows_pipeline_id` (`pipeline_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `workflows` WRITE;
/*!40000 ALTER TABLE `workflows` DISABLE KEYS */;
INSERT INTO `workflows` VALUES (1,1,1,'woodpecker','failure','Error response from daemon: manifest for woodpeckerci/plugin-git:test not found: manifest unknown: manifest unknown',1641630525,1641630527,0,'','{}',NULL);
/*!40000 ALTER TABLE `workflows` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;


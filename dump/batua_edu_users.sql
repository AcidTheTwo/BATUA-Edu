CREATE DATABASE  IF NOT EXISTS `batua_edu` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `batua_edu`;
-- MySQL dump 10.13  Distrib 8.0.46, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: batua_edu
-- ------------------------------------------------------
-- Server version	8.0.46

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `user_id` varchar(10) NOT NULL,
  `username` varchar(50) NOT NULL,
  `first_name` varchar(50) NOT NULL,
  `middle_name` varchar(50) DEFAULT NULL,
  `last_name` varchar(50) NOT NULL,
  `email` varchar(100) NOT NULL,
  `password_hash` varchar(255) NOT NULL,
  `role` enum('USER','ADMIN') DEFAULT 'USER',
  `account_status` enum('ACTIVE','BLOCKED','PENDING') DEFAULT 'ACTIVE',
  `created_on` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `status` enum('ACTIVE','LOCKED') NOT NULL DEFAULT 'ACTIVE',
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `username` (`username`),
  UNIQUE KEY `email` (`email`),
  CONSTRAINT `chk_user_id_fmt` CHECK (regexp_like(`user_id`,_utf8mb4'^USR[0-9]{7}$'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` VALUES ('USR0000001','acidthesecond','Kushagra','K.','Singh','251210056@nitdelhi.ac.in','$2b$10$k6j0wqAZ/j7R9lyH5BeSreeiP1fJ4cBKmmgtamemI3xmhqTayicW2','USER','ACTIVE','2026-11-15 14:57:25','2026-11-15 14:57:25','ACTIVE'),('USR0000002','kuga_tae','kuga',NULL,'tse','251210055@nitdelhi.ac.in','$2b$10$3EU7Xgc3VEFYyXxUl0DGde.kMZVC/senUCRsV6UCpa6476vg3Z/l6','USER','ACTIVE','2026-11-15 15:35:23','2026-11-16 15:12:24','ACTIVE'),('USR0000003','hari','Krishna',NULL,'Gautam','251210054@nitdelhi.ac.in','$2b$10$yog6upA1WxKavGwdOk/MneGH/zw9FD1cYqM4q2wzRQpvWP1fJW4Xa','USER','ACTIVE','2026-11-15 17:44:19','2026-11-23 07:14:56','ACTIVE'),('USR0000004','root','batua',NULL,'admin','acidmc69@gmail.com','$2b$10$HW0K7Jtsgv0wfjw60SNoUOOKPkFzw3YuaMv5pAP7sFmVy4mOVU5C2','ADMIN','ACTIVE','2026-11-16 14:55:46','2026-11-16 14:56:32','ACTIVE'),('USR0000005','Kaustubh','Kaustubh',NULL,'Gavandgave','251210052@nitdelhi.ac.in','$2b$10$trwmCj6xBPOXZk6UzVss/OniNx7psf2Vst76/xuG0t1mSPRTxh6qq','USER','BLOCKED','2026-11-16 15:16:01','2026-11-23 07:14:16','ACTIVE');
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-11-23 13:46:42

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
-- Table structure for table `loans`
--

DROP TABLE IF EXISTS `loans`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `loans` (
  `loan_id` varchar(10) NOT NULL,
  `borrower_id` varchar(10) NOT NULL,
  `lender_id` varchar(10) DEFAULT NULL,
  `lending_id` varchar(10) DEFAULT NULL,
  `principal_amount` decimal(18,2) NOT NULL,
  `interest_rate` decimal(7,3) NOT NULL,
  `term_days` int NOT NULL,
  `total_repayment` decimal(18,2) NOT NULL,
  `amount_paid` decimal(18,2) DEFAULT '0.00',
  `loan_status` enum('PENDING','APPROVED','ACTIVE','REPAID','REJECTED','DEFAULTED') DEFAULT 'PENDING',
  `start_date` date DEFAULT NULL,
  `due_date` date DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`loan_id`),
  KEY `fk_loan_lending` (`lending_id`),
  KEY `idx_loan_borrower` (`borrower_id`),
  KEY `idx_loan_lender` (`lender_id`),
  KEY `idx_loan_status` (`loan_status`),
  CONSTRAINT `fk_loan_borrower` FOREIGN KEY (`borrower_id`) REFERENCES `users` (`user_id`) ON DELETE RESTRICT,
  CONSTRAINT `fk_loan_lender` FOREIGN KEY (`lender_id`) REFERENCES `users` (`user_id`) ON DELETE RESTRICT,
  CONSTRAINT `fk_loan_lending` FOREIGN KEY (`lending_id`) REFERENCES `lending` (`lending_id`) ON DELETE RESTRICT,
  CONSTRAINT `chk_loan_amount` CHECK (((`principal_amount` > 0) and (`total_repayment` >= `principal_amount`) and (`amount_paid` >= 0) and (`amount_paid` <= `total_repayment`))),
  CONSTRAINT `chk_loan_id_fmt` CHECK (regexp_like(`loan_id`,_utf8mb4'^LON[0-9]{7}$')),
  CONSTRAINT `chk_loan_interest` CHECK ((`interest_rate` >= 0)),
  CONSTRAINT `chk_loan_term` CHECK ((`term_days` > 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `loans`
--

LOCK TABLES `loans` WRITE;
/*!40000 ALTER TABLE `loans` DISABLE KEYS */;
/*!40000 ALTER TABLE `loans` ENABLE KEYS */;
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

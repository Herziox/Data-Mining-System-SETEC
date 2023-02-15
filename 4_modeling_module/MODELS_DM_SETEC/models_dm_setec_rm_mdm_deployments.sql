-- MySQL dump 10.13  Distrib 8.0.30, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: models_dm_setec
-- ------------------------------------------------------
-- Server version	8.0.30

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
-- Table structure for table `rm_mdm_deployments`
--

DROP TABLE IF EXISTS `rm_mdm_deployments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rm_mdm_deployments` (
  `location_id` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `id` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rm_mdm_deployments`
--

LOCK TABLES `rm_mdm_deployments` WRITE;
/*!40000 ALTER TABLE `rm_mdm_deployments` DISABLE KEYS */;
INSERT INTO `rm_mdm_deployments` VALUES ('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-total_horas','REGRESSION','06992bf2-521e-4fda-9ccf-fa6443916048'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-volumen_ganancias','REGRESSION','1f7e7431-b013-4ccd-9a81-44631b69fe19'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-volumen_horas','REGRESSION','226f78d9-f2d4-4428-9e0c-88cf31088761'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-volumen_porcentaje_asistencia','REGRESSION','278aa4fa-be42-4b22-9c19-7237ac7cfb8f'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-total_cursos','REGRESSION','284f18c7-4c77-4bca-8fb7-0cc1e659d93d'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_ci-num_cap','REGRESSION','2d0b74ac-0f9a-4583-b93c-274bbc2d4bb9'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_ci-volumen_ganancias','REGRESSION','4813a7fb-0f66-465f-b619-2bf0aa8af338'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-total_ganancias','REGRESSION','4aabb863-6617-4666-a19b-c5ca156e0b18'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-num_cer','REGRESSION','55057bd6-a77e-4574-86ce-a43cbebbefe9'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-volumen_cer','REGRESSION','6b8824fa-6c63-49c9-97de-adde196b8627'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-porcentaje_asistencia','REGRESSION','7bf4fff0-6c3a-43ac-b81e-cf860f44446b'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-volumen_horas','REGRESSION','826b0f79-3963-47ae-b547-6c59d3236370'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-num_cap_cer','REGRESSION','84dc4e98-480c-49da-941e-f1eabf3e4986'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-volumen_porcentaje_asistencia','REGRESSION','8c6eee98-afe9-4e35-8eb7-40cccb0ffcb7'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_ci-total_cursos','REGRESSION','9cb09941-c644-40ea-a48a-1452ed3a0432'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-total_ganancias','REGRESSION','a006774a-61ee-48bf-a4fc-e77721e73cee'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-porcentaje_asistencia','REGRESSION','a6abce32-0cbe-4068-97e2-d97408bb92fc'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-volumen_cap_cer','REGRESSION','b4c650ff-2156-4b64-8da0-f783c4bdf286'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_ci-total_horas','REGRESSION','bc70677f-9b49-4454-be07-41cba8c411eb'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_ci-volumen_horas','REGRESSION','c429d988-1920-4db7-aa68-1498518b9f24'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_ci-volumen_cap','REGRESSION','c656b6eb-369e-4f8c-ad7e-30fb81ac239b'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_ci-total_ganancias','REGRESSION','caf2e664-7e9e-4732-bfbf-7ba3a2e5aceb'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-total_horas','REGRESSION','dfd43306-a89f-4594-865c-5e452a571334'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oc-volumen_ganancias','REGRESSION','f25b712d-bb22-48d7-aa0f-ad5452bb489d'),('301a903f-467e-4de2-878e-f9d0f5aef894','deployment_oec-total_perfiles','REGRESSION','fb30492f-9d0f-4f54-ace3-47f5f59a2b3d');
/*!40000 ALTER TABLE `rm_mdm_deployments` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-02-14 22:14:43

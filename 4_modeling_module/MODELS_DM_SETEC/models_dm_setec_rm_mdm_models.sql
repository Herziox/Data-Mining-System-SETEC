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
-- Table structure for table `rm_mdm_models`
--

DROP TABLE IF EXISTS `rm_mdm_models`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rm_mdm_models` (
  `deployment_id` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `id` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rm_mdm_models`
--

LOCK TABLES `rm_mdm_models` WRITE;
/*!40000 ALTER TABLE `rm_mdm_models` DISABLE KEYS */;
INSERT INTO `rm_mdm_models` VALUES ('bc70677f-9b49-4454-be07-41cba8c411eb','model_ci-total_horas_v1','1a5f9661-ec91-47ec-8c5d-3c7d5e8d9692'),('7bf4fff0-6c3a-43ac-b81e-cf860f44446b','model_oc-porcentaje_asistencia_v1','1b2f0e0c-e079-4c08-aad8-9cd596024ab7'),('c656b6eb-369e-4f8c-ad7e-30fb81ac239b','model_ci-volumen_cap_v2','1cfeb381-204a-4bef-83c2-194d2df8f9da'),('c429d988-1920-4db7-aa68-1498518b9f24','model_ci-volumen_horas_v1','29b55c5f-aa06-4961-b62f-93a990992020'),('bc70677f-9b49-4454-be07-41cba8c411eb','model_ci-total_horas_v2','2a18e0b0-4dd6-489a-b384-a91610dad336'),('b4c650ff-2156-4b64-8da0-f783c4bdf286','model_oc-volumen_cap_cer','3c1935db-7572-45a6-bbf4-9b5eb71d184c'),('4aabb863-6617-4666-a19b-c5ca156e0b18','model_oec-total_ganancias_v1','43d423d5-8496-4fe2-93f9-2cd77ecc32d8'),('06992bf2-521e-4fda-9ccf-fa6443916048','model_oc-total_horas_v1','45bc730f-ddcb-46cc-a56f-72dd427f5777'),('a6abce32-0cbe-4068-97e2-d97408bb92fc','model_oec-porcentaje_asistencia_v1','4a20d888-b637-49e2-91c1-a5c538cf3268'),('6b8824fa-6c63-49c9-97de-adde196b8627','model_oec-volumen_cer_v1','4b82cb6c-4d41-41c3-b52c-e7697fd8e907'),('c656b6eb-369e-4f8c-ad7e-30fb81ac239b','model_ci-volumen_cap_v1','4c4f6a89-2fd9-4848-affe-088bdc287cd9'),('c429d988-1920-4db7-aa68-1498518b9f24','model_ci-volumen_horas_v2','4f833682-45f8-425d-a874-52ea3e3a8177'),('55057bd6-a77e-4574-86ce-a43cbebbefe9','model_oec-num_cer_v1','501ff61a-de21-4af3-99f6-f9bcd1d1260d'),('1f7e7431-b013-4ccd-9a81-44631b69fe19','model_oec-volumen_ganancias_v1','513dbf64-f7b4-4df0-8523-cb47bcc7f4d6'),('caf2e664-7e9e-4732-bfbf-7ba3a2e5aceb','model_ci-total_ganancias_v1','5b9c8235-9820-4494-8828-32b19c2d42f3'),('826b0f79-3963-47ae-b547-6c59d3236370','model_oec-volumen_horas_v1','5e96ad78-d9c0-44fb-9cbe-9f755a4ea5a3'),('2d0b74ac-0f9a-4583-b93c-274bbc2d4bb9','model_ci-num_cap_v2','5ef6ab2a-5d6a-4d96-97f8-1a59387f04b9'),('278aa4fa-be42-4b22-9c19-7237ac7cfb8f','model_oc-volumen_porcentaje_asistencia_v1','82686634-0928-4df7-8d26-54c9efaaede0'),('2d0b74ac-0f9a-4583-b93c-274bbc2d4bb9','model_ci-num_cap_v1','8b2c6a1e-3f7d-418e-ae68-513dec43dea4'),('8c6eee98-afe9-4e35-8eb7-40cccb0ffcb7','model_oec-volumen_porcentaje_asistencia','8f560d2a-cbc7-4626-b709-bc0376a5c90f'),('84dc4e98-480c-49da-941e-f1eabf3e4986','model_oc-num_cap_cer_v1','910ef61b-b459-48f2-8de2-5dc156305fd5'),('226f78d9-f2d4-4428-9e0c-88cf31088761','model_oc-volumen_horas_v1','a6fb1376-175b-4937-9386-2ee2588255c1'),('4813a7fb-0f66-465f-b619-2bf0aa8af338','model_ci-volumen_ganancias_v2','af4bdf86-f89f-4d4d-af5c-1af4fc261962'),('fb30492f-9d0f-4f54-ace3-47f5f59a2b3d','model_oec-total_perfiles_v1','b2c21e02-1df9-4040-a8a4-6dc5c3183345'),('9cb09941-c644-40ea-a48a-1452ed3a0432','model_ci-total_cursos_v2','c37e9a27-1df0-4c2d-8c13-da3099076464'),('caf2e664-7e9e-4732-bfbf-7ba3a2e5aceb','model_ci-total_ganancias_v2','c56867f7-ce5a-4b43-a445-cca56f16aa9b'),('9cb09941-c644-40ea-a48a-1452ed3a0432','model_ci-total_cursos_v1','d2a8418f-b757-44c8-a937-21887a3cede2'),('4813a7fb-0f66-465f-b619-2bf0aa8af338','model_ci-volumen_ganancias_v1','d3db2377-5a56-4553-b3f9-59d0cf314293'),('a006774a-61ee-48bf-a4fc-e77721e73cee','model_oc-total_ganancias_v1','debc4b89-a6bb-42a8-8e2a-7998265d793f'),('284f18c7-4c77-4bca-8fb7-0cc1e659d93d','model_oc-total_cursos','e369e80c-e909-4bc0-a281-d64ed2119b12'),('dfd43306-a89f-4594-865c-5e452a571334','model_oec-total_horas_v1','e67f88fa-7c74-4013-8701-e84d99f46631'),('f25b712d-bb22-48d7-aa0f-ad5452bb489d','model_oc-volumen_ganancias_v1','eff77d11-3cc4-4e2e-9088-db392e8866aa');
/*!40000 ALTER TABLE `rm_mdm_models` ENABLE KEYS */;
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

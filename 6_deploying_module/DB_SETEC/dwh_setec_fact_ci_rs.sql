-- MySQL dump 10.13  Distrib 8.0.30, for Win64 (x86_64)
--
-- Host: localhost    Database: dwh_setec
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
-- Table structure for table `fact_ci_rs`
--

DROP TABLE IF EXISTS `fact_ci_rs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_ci_rs` (
  `id_fact_ci_rs` int NOT NULL,
  `id_razon_social` int NOT NULL,
  `id_fecha` int NOT NULL,
  `volumen_cap` int NOT NULL,
  `prediction_volumen_cap` int NOT NULL,
  `volumen_horas` int NOT NULL,
  `prediction_volumen_horas` int NOT NULL,
  `volumen_ganancias` float NOT NULL,
  `prediction_volumen_ganancias` float NOT NULL,
  PRIMARY KEY (`id_fact_ci_rs`),
  KEY `id_fecha` (`id_fecha`),
  CONSTRAINT `fact_ci_rs_ibfk_1` FOREIGN KEY (`id_fecha`) REFERENCES `dim_fecha` (`id_fecha`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fact_ci_rs`
--

LOCK TABLES `fact_ci_rs` WRITE;
/*!40000 ALTER TABLE `fact_ci_rs` DISABLE KEYS */;
INSERT INTO `fact_ci_rs` VALUES (0,1000,7,191,191,1696,3044,23671.7,20777),(1,1001,6,263,263,6784,6784,31272,33762.7),(2,1002,5,144,144,4632,4632,11960.7,15579),(3,1004,7,22,22,704,704,1936,1775.92),(4,1006,6,1,1,32,32,32,309.06),(5,1009,6,4,4,160,160,673.6,568.796),(6,1011,6,1,1,40,40,20.8,325.373),(7,1018,5,2,2,80,75,419.2,382.262),(8,1023,7,16,16,352,352,2506.33,2199.87),(9,1024,6,5,5,160,160,843.2,685.003),(10,1025,7,175,175,7000,7000,9208,10138.7),(11,1027,6,23,23,920,920,1692.8,3055.32),(12,1030,7,4,4,128,128,1442.56,751.527),(13,1031,7,1,1,16,16,129.92,1447.84),(14,1037,6,11,11,440,440,1980,1415),(15,1043,6,61,61,488,1037,10277.3,7827.94),(16,108,6,41,41,1640,1640,8805.52,6041.33),(17,111,6,1,1,16,16,4.48,309.06),(18,117,6,9,9,144,127,623.52,997.825),(19,120,6,2,2,80,80,41.6,354.323),(20,1238,6,54,54,2144,2144,3187.2,2977.14),(21,1239,6,8,8,320,320,560,1652.74),(22,1240,6,16,16,640,640,1120,2606.29),(23,1246,6,99,99,1584,1584,38808,23607.1),(24,1247,6,106,106,4240,4240,7420,13795.3),(25,125,6,160,160,6400,6400,6923.2,12706),(26,1250,6,47,47,1712,1712,3309.44,4710.82),(27,1251,7,10,10,400,352,448,1450.83),(28,1253,6,1,1,40,40,320,376.269),(29,1257,6,465,465,5280,5280,51984,54398.3),(30,1258,5,235,235,9400,9400,39574,40160.8),(31,1263,6,24,24,408,292,2683.92,3107),(32,1265,5,26,26,1040,1040,1955.2,3425.48),(33,1266,6,86,86,3440,2933,10403.6,10797.3),(34,1267,6,2,2,32,32,85.44,321.808),(35,1268,6,69,69,1656,1656,24873.1,21280.2),(36,1269,6,79,79,1264,1408,13309.9,9501.43),(37,127,6,2,2,80,80,89.6,338.01),(38,1270,6,34,34,544,544,3740.47,3760.82),(39,1272,6,1,1,40,40,70,376.269),(40,1274,7,61,61,488,488,3355.43,3625.59),(41,1275,6,11,11,440,440,794.4,972.151),(42,1276,5,542,542,21536,21536,107689,110868),(43,128,6,1,1,40,35,168.4,325.373),(44,1281,6,4,4,160,160,240,667.562),(45,1282,6,1,1,16,16,140,309.06),(46,1294,6,13,13,520,520,582.4,1009.47),(47,1298,6,124,124,4960,4960,22320,29577.2),(48,1299,7,5,8,80,80,550.07,629.482),(49,1301,6,1019,1019,32608,32608,224343,189822),(50,1313,6,1,1,32,32,168.64,325.373),(51,1315,6,24,24,960,597,4041.6,3770.84),(52,1316,6,3,3,96,104,335.04,500.948),(53,1333,6,2,2,32,37,99.84,338.121),(54,1334,7,3,3,120,120,886.8,531.046),(55,1337,6,100,100,3448,3448,20004.6,15779.2),(56,1338,7,21,21,600,600,1326.4,2382.31),(57,1339,6,37,37,856,856,13336.9,5000.9),(58,1343,6,13,13,520,520,1450.8,1958.46),(59,1345,6,8,8,256,209,802.56,1155.61),(60,1348,6,9,9,88,127,700,980.054),(61,1349,6,13,13,368,368,4240.2,8578.87),(62,1350,6,5,5,200,200,350,783.769),(63,1353,6,161,161,6440,4688,18804.8,24281.6),(64,1355,7,16,16,512,512,5770.24,5220.83),(65,1358,6,883,853,23008,23008,97234.6,97949.8),(66,1359,6,45,45,1800,1800,2700,5223.79),(67,1361,7,13,13,520,520,3575.45,2940.38),(68,1362,6,49,53,784,784,11179,7239.03),(69,1363,7,100,100,4000,4000,10480,11738.1),(70,1364,6,41,41,656,656,4510.57,3309.79),(71,1365,7,7,7,280,280,945.6,1092.02),(72,1367,6,10,10,400,293,1684,1223.2),(73,1368,5,2,2,32,32,220.028,826.134),(74,1370,6,1,1,40,40,173.2,309.06),(75,1371,6,43,43,1432,1432,1303.36,6041.33),(76,1378,7,1,1,40,40,44.8,314.401),(77,1379,6,42,42,1680,1680,9377.2,8747.94),(78,1380,6,260,260,10400,10400,46800,46435.2),(79,1384,5,84,84,3360,3536,3763.2,8829.01),(80,1385,6,4,4,160,160,1100.14,680.597),(81,1386,6,38,38,1520,1520,1702.4,2935.25),(82,1390,6,59,59,1024,1024,3795.82,7327.73),(83,1392,7,169,169,6760,6760,60907.6,63763.3),(84,1393,6,15,15,584,584,900.32,2050.4),(85,1395,6,257,257,4576,4576,46710.1,39415.2),(86,1397,6,12,12,384,384,1340.16,1597.18),(87,1398,6,6,5,96,96,533.76,692.514),(88,1401,6,15,15,600,600,1050,2213.6),(89,1402,6,1,1,16,16,129.92,424.139),(90,1406,6,212,212,4128,4128,27623.3,26497.4),(91,1408,6,6,6,80,164,1279.68,676.201),(92,1413,6,17,17,480,480,2078.4,2602),(93,1415,7,8,8,320,320,1080,1291.72),(94,1416,6,54,54,2160,2160,18522.6,12268.7),(95,1420,6,2,2,32,32,220.028,389.35),(96,1430,6,94,94,1880,1880,7099.54,5651.46),(97,1436,5,1216,1216,29264,29264,204911,227503),(98,1438,6,110,110,1088,1088,7480.95,9226.97),(99,1439,6,270,270,3088,3088,15620,23078.6),(100,1441,6,36,36,888,888,2288.5,4872.56),(101,1453,6,26,26,208,597,4380.48,3145.34),(102,1455,6,15,15,384,384,2190.21,2186.82),(103,1457,6,58,58,2320,2320,16227,13673.4),(104,1464,6,22,22,880,880,3960,2480.01),(105,1465,6,21,21,840,840,1470,2781.51),(106,1468,6,36,36,1104,1104,7074.6,5200.91),(107,1469,6,16,16,640,640,4400.56,2606.29),(108,1470,6,2,2,32,21,336.96,338.121),(109,1472,6,9,9,72,127,540,980.054),(110,1474,6,94,94,1072,1072,2128,2762.8),(111,1475,6,81,81,1544,1544,10340.5,12601.9),(112,1476,6,13,13,312,312,1026.48,1753.89),(113,1478,5,495,486,19800,19800,103752,98800.2),(114,1479,6,26,26,624,597,4380.48,3979.91),(115,1481,6,90,90,3120,3120,20250.8,20707.2),(116,1482,6,1,1,40,40,275.035,325.373),(117,1484,7,7,7,112,154,1179.36,958.616),(118,1485,7,125,125,1000,1000,6875.87,10647.5),(119,1487,5,158,158,2408,2408,16898.6,18847.6),(120,1488,5,10,10,248,248,181.28,1730.69),(121,1490,5,149,149,5960,5980,53997.6,39161.1),(122,1496,6,1,1,16,16,360.48,325.373),(123,1502,6,373,373,14576,14576,199590,185992),(124,1505,6,17,17,544,520,1898.56,2520.72),(125,1506,6,5,5,200,200,1048,685.003),(126,1510,5,2,2,64,75,419.2,382.262),(127,1511,7,57,57,2280,2280,2553.6,2579.38),(128,1514,6,28,28,1120,1120,1960,1825.86),(129,1515,6,30,30,688,688,10808.4,5653.27),(130,1520,7,86,86,3440,3440,23653,20462.9),(131,1521,6,5,5,120,160,445.2,685.003),(132,1523,6,16,16,256,256,1410.56,1830.14),(133,1525,6,39,39,1248,1229,4355.52,5347.19),(134,1526,6,15,15,600,600,1050,1382.75),(135,1527,6,95,95,1600,1600,1668.48,9683.25),(136,1532,6,21,21,840,648,3536.4,2960.42),(137,1539,6,8,8,256,256,2560,1568.12),(138,1541,6,94,94,1504,1504,10341.3,13978.2),(139,1544,6,4,4,32,32,673.92,484.747),(140,1545,6,234,234,9328,9328,10910.6,18451.2),(141,1546,7,38,38,1520,1540,4240.8,5417.17),(142,1548,7,2,2,80,80,4,316.573),(143,1549,6,174,174,6960,6960,38210.8,30802.6),(144,155,7,92,92,2208,2200,10289.3,12433.2),(145,1550,6,16,17,536,536,3332.48,2407.93),(146,1551,6,2,2,80,80,640,338.01),(147,1552,6,47,47,1880,1880,2105.6,8391.26),(148,1558,6,4,5,160,160,446.4,568.796),(149,156,6,19,19,760,760,3280.8,2967.67),(150,1568,6,10,10,80,293,1117.6,996.367),(151,1570,7,9,9,144,144,990.126,963.359),(152,1571,6,55,55,2200,2200,3300,9639.58),(153,1572,6,2,2,80,80,640,338.01),(154,1576,6,74,74,2960,2960,20352.6,15467),(155,1577,6,22,22,176,292,3706.56,2464.35),(156,1588,6,4,4,160,136,240,552.483),(157,1593,6,28,28,1120,1120,1254.4,1352.52),(158,1594,7,19,19,152,152,1045.13,2046.03),(159,1597,6,115,115,2752,2752,19813,20572.2),(160,1603,6,2,2,32,32,720.96,489.931),(161,1604,5,4,4,160,160,673.6,596.735),(162,1609,6,673,673,26920,26920,188213,191297),(163,1612,6,41,41,1640,1640,2460,6894.28),(164,1617,6,208,208,8320,8320,14560,17971.1),(165,1619,6,66,66,2640,2640,8515.66,13153.3),(166,1620,6,8,8,320,288,4400,1445.49),(167,1622,6,1,1,40,40,550,309.06),(168,1623,7,2,2,64,64,440.056,365.256),(169,1632,6,16,16,640,520,2694.4,2609.12),(170,1634,7,41,36,592,592,4727.42,4912.06),(171,1635,5,2,2,80,80,233.6,382.262),(172,1636,6,19,19,608,608,1143.04,2247.68),(173,1638,6,9,9,72,209,1516.32,996.367),(174,1639,6,14,14,560,560,627.2,1009.47),(175,1640,6,289,289,10792,10792,20472,17936.9),(176,1641,7,804,804,22120,22120,80398.6,69025.2),(177,1642,7,31,31,1240,1240,1860,4956.42),(178,1643,7,12,12,480,480,3840,1982.18),(179,1645,6,63,63,2520,2399,11108,8636.71),(180,1646,5,6,6,240,240,720,704.979),(181,1648,7,1,1,16,16,168.48,287.623),(182,165,6,170,170,3736,3736,13651.2,17330.8),(183,1650,6,177,177,6888,7259,29812.6,27112.8),(184,1653,6,65,65,2600,2600,11700,8699.29),(185,1655,6,11,11,88,88,1229.36,1030.8),(186,1657,5,2,2,32,32,220.08,802.773),(187,1664,6,3,2,56,56,115.2,484.747),(188,1668,6,88,88,3520,3600,10278.4,11920.5),(189,1672,6,6,6,240,240,550,914.147),(190,1677,6,93,93,2024,2024,8757.12,9579.8),(191,1678,7,7,7,280,280,1925.24,1581.06),(192,168,6,30,30,720,720,4802.4,5509.46),(193,1680,6,1,1,40,40,275.035,433.126),(194,1684,7,9,9,144,154,1006.56,976.387),(195,1685,6,6,6,168,168,773.04,710.286),(196,1688,6,16,16,528,528,1397.76,1085.27),(197,1689,6,72,72,2856,2856,3231.76,3360.41),(198,1690,6,126,126,5040,5040,20229.6,23268.4),(199,1691,6,10,10,160,160,440,683.099),(200,1698,6,49,49,1960,1960,3430,5788.97),(201,1699,6,14,14,448,520,2360.96,1911.26),(202,1700,6,19,19,760,760,5225.66,3970.57),(203,1701,7,19,19,760,760,3199.6,2929.92),(204,1702,6,68,68,1312,1312,6285.83,8865.89),(205,1704,7,20,20,320,320,2200.28,2186.66),(206,1706,6,32,32,1280,1280,10240,6373.73),(207,1709,6,65,65,2600,2933,3282.8,8199.91),(208,171,6,23,23,920,920,1030.4,3055.32),(209,1710,6,2,2,48,48,320.16,112.631),(210,1717,5,1524,1524,17376,17376,142207,156210),(211,1721,6,31,31,1240,1240,11172.4,6564.88),(212,1722,7,19,19,760,760,1140,1882.27),(213,1723,5,4,4,128,128,224,2376.36),(214,1725,5,1,1,40,40,60,672.651),(215,1729,6,119,119,4104,4104,46271.6,31033.8),(216,173,6,704,704,16896,16896,31764.5,32257.7),(217,1731,6,8,8,320,288,480,1445.49),(218,174,6,1,1,24,24,88.08,325.373),(219,1740,7,92,92,1048,1189,15500.2,11511.6),(220,1743,6,231,231,8472,8472,38903,29552.2),(221,1744,5,56,56,1112,1112,7366.24,7386.47),(222,1748,6,67,67,2584,2584,5177.66,4979.29),(223,175,6,6,6,240,240,420,719.692),(224,1750,6,160,160,5168,5168,31235.2,30581.6),(225,1753,6,9,9,360,360,403.2,1251.5),(226,1754,5,6,6,240,240,451.2,-2178.82),(227,1755,6,56,56,2032,2032,7524.16,9411.08),(228,1758,6,28,28,448,448,3080.39,3577.71),(229,176,5,46,46,736,736,7750.08,5117.97),(230,1760,6,373,373,8952,8952,104470,106457),(231,1761,7,3,3,120,108,420,557.824),(232,1762,6,4,4,160,160,280,568.796),(233,1763,6,35,35,1400,1400,7336,5401.15),(234,1765,7,1,1,40,40,180,336.639),(235,1767,6,99,99,3960,3960,27228.5,24042.7),(236,1769,6,5,5,200,200,350,668.69),(237,177,6,5,5,200,200,900,685.003),(238,1772,6,171,171,4216,4688,20545.3,22685.2),(239,1775,7,61,61,2440,2200,10272.4,7840),(240,1778,6,15,15,360,360,2401.2,1976.18),(241,1779,6,74,74,2696,2399,8000.56,9733.03),(242,1789,7,10,8,80,154,550.07,958.616),(243,1790,6,11,11,440,440,1852.4,1415),(244,1793,6,34,34,1104,1104,1609.92,2256.31),(245,1794,6,8,8,320,209,1440,1223.2),(246,1795,7,24,24,400,400,3510.29,3175.67),(247,1798,6,5,5,136,136,1405.12,668.69),(248,1800,6,22,22,880,880,6050.77,4009.48),(249,1801,6,21,21,504,504,3465.44,2839.52),(250,1806,6,21,21,504,504,7570.08,4233),(251,1807,5,1,1,40,40,176.4,292.871),(252,1810,6,14,14,448,448,842.24,1055.65),(253,1813,6,13,13,208,208,1156.48,1536.22),(254,1815,6,21,21,744,744,5115.65,3723.76),(255,182,7,23,23,920,920,1729.6,3410.73),(256,1822,6,299,299,4840,4840,43178.3,43181.5),(257,1826,7,1,1,40,40,320,287.623),(258,1828,6,1,1,40,40,275.035,376.269),(259,1829,7,4,4,160,160,300.8,580.062),(260,1830,6,17,17,680,680,1190,2691.21),(261,1834,5,279,279,6648,6648,40265.5,33815),(262,1836,6,42,42,1680,1680,8803.2,5529.5),(263,1838,6,16,16,640,640,342.4,2717.34),(264,184,6,9,8,216,127,2430,1053.35),(265,1841,6,36,36,560,560,3850.49,4292.51),(266,1842,5,6,6,48,48,1010.88,704.251),(267,1845,7,62,62,1952,2200,8520,8617.59),(268,1847,7,95,95,2056,2056,14645.4,11065.1),(269,1854,6,674,853,21536,21536,116406,116033),(270,1855,6,125,125,4736,4736,21057.9,22964.7),(271,1857,6,1,1,32,32,60.16,-47.4091),(272,1861,6,3,3,120,120,1087.2,568.796),(273,1863,6,5,5,80,80,550.07,718.128),(274,1865,7,8,8,168,168,2443.55,1647.24),(275,1867,7,2,2,16,21,336.96,300.371),(276,1868,7,1,1,40,40,180,287.623),(277,187,7,35,35,1152,1152,2332.16,4822.81),(278,1873,6,94,94,2256,1408,15047.5,12139),(279,1878,7,2,2,80,80,360,413.127),(280,1882,6,163,163,4680,4680,1892.8,9146.49),(281,1883,6,15,15,600,600,2700,2762.59),(282,1884,7,16,16,640,640,716.8,1377.66),(283,1885,5,389,389,14256,14256,57175.7,57926.3),(284,1886,6,426,426,8232,8232,56919.9,63639.8),(285,1887,7,39,39,1560,1560,6754.8,6319.94),(286,1888,6,29,29,696,696,2554.32,4014.85),(287,189,6,472,472,15728,15728,28508.8,34656.8),(288,1892,6,4,4,128,128,240.64,568.796),(289,1897,5,34,34,1360,1400,5725.6,5325.13),(290,1899,7,2,2,80,80,89.6,343.351),(291,19,6,69,69,2760,2032,7700.4,9309.22),(292,190,6,97,97,776,2032,5335.68,8838.75),(293,1900,7,12,12,216,216,1270.15,1379.35),(294,1903,6,35,35,1400,1400,5894,5401.15),(295,1904,6,2,3,48,37,223.68,338.121),(296,191,5,222,222,7488,7488,17375.4,-4206.07),(297,1910,6,1,1,40,40,150,325.373),(298,1911,6,589,589,11352,11352,51271.4,47347.3),(299,1912,6,50,50,1856,1856,7512.96,7658.12),(300,1913,6,239,239,9560,9560,12394.4,12158.6),(301,1914,7,3,3,120,108,529.2,531.046),(302,1916,6,307,307,12280,12280,55260,52457.1),(303,1917,5,24,24,960,960,2080,-3603.99),(304,1921,7,2,2,32,56,186.527,300.371),(305,1922,5,190,190,7552,7416,30121.8,39186.6),(306,1927,7,40,40,640,640,4473.6,4973.74),(307,1931,6,1,1,40,40,117.6,325.373),(308,1953,6,50,50,400,400,8424,6167.54),(309,1955,6,1,1,24,24,89.04,325.373),(310,1956,6,26,26,624,624,9372.48,6614.2),(311,1957,5,17,17,168,168,2580.04,3414.88),(312,1958,6,31,31,592,592,2004.32,4080.97),(313,1960,6,240,240,9552,9552,40416.2,33632.3),(314,1961,6,20,20,800,800,2284,3120.17),(315,197,6,76,76,3040,3040,20902.7,20291.6),(316,1980,5,164,164,6248,6248,14156,13792.9),(317,1984,5,28,28,1120,1120,5600,5856.99),(318,1989,6,94,94,3664,2399,15829.9,12177.1),(319,1995,7,23,23,920,920,2585.2,3017.57),(320,1999,5,9,9,360,380,1080,1321.83),(321,20,5,94,94,3712,3536,10750.1,13245.9),(322,2000,6,5,5,200,160,350,685.003),(323,2003,5,614,640,19472,19472,108445,107919),(324,2004,6,65,65,2600,2600,10946,8699.29),(325,2005,5,294,294,4512,4512,49533.1,49655.1),(326,2013,6,7,7,280,209,280,1191.48),(327,2018,6,24,24,384,384,2640.34,2890.77),(328,2019,6,328,328,13120,13120,37921.6,37926.2),(329,2023,6,13,13,496,496,950.014,2262.78),(330,2024,6,189,189,7448,7259,50679.6,40575.8),(331,2027,5,12,12,288,288,1920.96,1664.08),(332,2031,6,167,167,6680,6680,11422.8,13329.6),(333,2034,6,90,90,1440,1037,10065.6,11819.9),(334,2035,6,96,96,3840,3840,4300.8,5708.06),(335,2036,7,135,135,5400,5400,37129.7,39059.2),(336,2038,7,9,9,360,360,676.8,1355.18),(337,204,7,1,1,16,16,140,314.401),(338,2040,6,405,405,16200,16200,72900,70373.9),(339,2041,6,10,10,160,160,1272.19,1058.85),(340,2042,6,3,3,112,104,340.028,500.948),(341,2045,6,1,1,40,40,44.8,309.06),(342,2047,6,7,7,56,209,1179.36,996.367),(343,2049,5,4433,4433,141472,141472,746631,684382),(344,2051,6,123,123,984,984,6765.86,7485.53),(345,2052,6,31,31,992,992,5227.84,4722.46),(346,2053,6,55,55,1896,1896,1081.28,6127.75),(347,2054,6,124,124,4664,4664,19433.7,15681.9),(348,2056,6,46,46,976,976,4905.19,4232.83),(349,2058,6,32,32,768,768,2818.56,4722.46),(350,2066,6,42,42,336,336,1136.96,4409.87),(351,2069,6,186,186,7440,7259,31322.4,37159.3),(352,2073,6,57,53,488,488,3347.36,6097.2),(353,2075,6,35,35,1192,1229,5058.88,5611.02),(354,2100,6,126,126,2352,2352,7778.99,12211.4),(355,2102,6,858,853,31560,31560,146329,141859),(356,2103,6,4912,4912,112896,112896,746656,684382),(357,2104,5,1,1,8,8,60,950.287),(358,2108,7,24,24,960,787,4041.6,3658.51),(359,211,7,3,3,96,108,415.68,489.976),(360,2110,5,28,28,448,572,4717.44,3571.67),(361,2113,7,60,60,2400,2400,10966.2,8050.87),(362,2115,6,4,4,160,160,300.8,568.796),(363,2116,6,69,69,2760,2933,5014,8809.83),(364,2118,6,4,4,160,160,240,667.562),(365,2119,6,39,39,800,800,8391.6,6920.57),(366,2121,6,450,450,7296,7296,129724,120962),(367,2124,7,3,3,72,108,787.12,463.198),(368,2125,7,345,345,13760,13760,20760,22241.8),(369,2126,6,4,4,160,160,280,667.562),(370,2127,6,542,542,13008,13008,89441.4,85496.6),(371,2128,5,182,182,7280,7416,20311.2,23386.5),(372,2129,7,5,5,192,192,340.16,647.253),(373,2131,5,1054,1054,42160,42160,103290,105106),(374,2133,6,110,110,4400,4400,15020,15761.4),(375,2135,7,7,7,280,154,420,1153.73),(376,2137,6,2,2,32,32,220.028,112.631),(377,2139,6,41,41,328,328,6907.68,4472.45),(378,2140,7,183,183,4392,3044,20466.7,21558.2),(379,2141,7,32,32,256,256,5391.36,4219.79),(380,2148,7,24,24,960,960,7680,4670.12),(381,2150,6,1,1,40,35,168.4,325.373),(382,2151,6,2,2,80,80,640,481.386),(383,2152,7,1,1,8,8,55.007,287.623),(384,2153,6,58,58,2320,2320,4440.28,4869.95),(385,2156,6,40,40,1600,1600,7056,5529.5),(386,2160,6,17,17,680,680,5440,3759.77),(387,2166,7,12,12,288,288,1499.5,1699.27),(388,217,6,4,4,32,32,220.028,3959.45),(389,2171,7,2,2,80,80,46.4,316.573),(390,2175,7,1146,1146,21384,21384,180280,164961),(391,2180,5,41,41,1312,1312,14786.2,10798.9),(392,2181,6,88,88,3520,3520,6160,5072.35),(393,2185,6,20,20,800,800,2232,3011.95),(394,2187,7,1,1,40,40,70,40.1762),(395,2192,6,785,853,28216,28216,105059,111180),(396,2195,5,150,150,6000,5980,25735.6,28661.6),(397,2196,6,112,112,4480,4480,30803.9,28438.4),(398,2198,6,1,1,16,16,392,424.139),(399,2201,6,10,10,384,384,920.056,1331.69),(400,2206,6,39,39,504,504,2175.23,2825.58),(401,2210,5,665,640,25944,25944,127130,123006),(402,2213,5,166,166,3984,3984,3306.72,2010.25),(403,2214,7,19,19,760,760,1180,2364.38),(404,2215,6,21,21,672,672,188.16,3866.75),(405,2216,6,2,3,80,80,356.4,354.323),(406,2217,5,5,5,80,80,1449.6,1352.59),(407,222,7,543,543,18240,18240,80473.8,79366.9),(408,2221,6,194,194,4032,4032,15973.1,7070.03),(409,2223,6,5,5,88,88,988.24,715.27),(410,2224,5,39,39,1560,1540,6567.6,5661),(411,2227,7,2,2,80,80,120,69.1261),(412,2228,7,3,3,48,48,330.042,732.796),(413,2230,6,11,11,280,280,1100.63,-1135.01),(414,2231,6,140,140,4480,4480,8960,14532.8),(415,2232,5,29,29,696,572,4885.92,4146.35),(416,2233,6,31,31,1240,1240,2760,2786.53),(417,2235,6,45,45,816,816,269.28,5046.67),(418,2240,6,21,21,576,648,3653.28,2711.48),(419,2241,7,34,34,1360,1360,2380,3341.8),(420,2242,6,152,152,6080,6080,27360,29768.4),(421,2243,7,14,14,112,112,770.098,1422.96),(422,2244,6,4,3,64,64,199.68,500.948),(423,2246,6,11,11,312,312,2145.27,1257.63),(424,2248,7,77,77,616,1189,12973,10900.2),(425,2250,7,3,3,48,48,82.08,207.057),(426,2251,6,61,61,2280,2280,3223.2,6131.49),(427,2254,6,11,11,88,88,605.077,554.48),(428,226,5,908,908,9160,9160,46773.7,42631.8),(429,228,5,25,25,1000,1000,8000,5775.06),(430,233,6,4,4,160,160,1100.14,2074.78),(431,239,6,88,88,704,1408,14826.2,10920.9),(432,24,6,126,126,2016,2016,14012.3,13063.5),(433,240,6,61,61,2440,2440,4587.2,5349.96),(434,241,6,274,274,10960,10960,47456.8,49112.4),(435,245,5,2,2,32,32,220.028,815.289),(436,249,7,17,17,680,680,9350,2989.79),(437,25,7,3,3,120,108,334.8,531.046),(438,250,7,26,26,832,832,5724.16,4783.33),(439,251,6,57,57,1368,1368,5075.28,6106.44),(440,252,6,6,6,240,240,1080,809.052),(441,259,7,93,93,1904,1189,15668.6,12433.2),(442,260,6,1,1,40,40,173.2,309.06),(443,261,6,5,5,160,160,52.8,668.69),(444,264,6,1,1,32,32,56,424.139),(445,273,6,21,21,528,648,2791.84,2711.48),(446,275,5,336,336,8064,8064,37578.2,35899.1),(447,283,5,285,285,4560,4560,3784.8,6387.8),(448,286,6,274,274,9216,9216,24852.5,27372.8),(449,287,6,27,27,1080,1080,7425.94,6745.76),(450,288,6,5,5,200,200,558,685.003),(451,295,6,49,49,984,984,8255.52,6441.06),(452,296,6,55,55,2200,2200,19822,14705.3),(453,297,6,38,38,1520,1520,10451.3,5529.5),(454,298,5,455,486,16968,16968,44421.4,47232.4),(455,3,6,14,14,336,336,1841.28,1855.89),(456,30,5,1288,1288,51520,51520,90150,175509),(457,301,6,151,151,6040,6040,41530.3,41116.6),(458,302,6,112,112,4480,4480,7005.6,6712.67),(459,305,7,13,13,256,256,2076.48,1648.55),(460,31,6,43,43,1416,1416,8289.12,7359.47),(461,317,6,238,238,7384,7384,19851.9,24060.2),(462,319,6,334,334,10128,10128,37312.3,37284),(463,32,5,6,6,240,240,420,-2178.82),(464,320,7,3,3,72,72,395.087,511.882),(465,322,6,5,5,200,200,1375.17,1099.14),(466,329,6,252,252,4544,4544,25318.7,26030.2),(467,34,7,165,165,4520,4520,21201.4,20172.1),(468,351,6,168,168,2600,2600,11291.2,6886.45),(469,354,5,507,486,19384,19384,80167.5,83631.6),(470,355,6,46,46,1840,1840,5372.8,5223.79),(471,358,7,247,247,2808,2808,33810.2,38896.7),(472,360,7,30,36,608,608,5743.36,4867.45),(473,361,7,12,12,272,272,1342.08,1527.7),(474,362,6,225,225,1848,1848,33510.5,33157.3),(475,364,6,3,3,120,120,210,680.597),(476,366,7,25,25,1000,1000,1120,3119.02),(477,370,6,20,20,480,480,1780.8,2597.53),(478,373,6,8,8,320,320,1440,1516.38),(479,375,6,110,110,4400,4400,15440,15761.4),(480,376,6,62,62,2320,2320,9842,4479.81),(481,381,6,1046,853,41168,41168,88104.2,125232),(482,382,6,28,28,544,544,3740.47,3415.25),(483,385,6,1,1,24,35,165.021,325.373),(484,387,7,53,53,848,848,5830.74,6191.38),(485,394,6,1,1,8,8,111.76,325.373),(486,413,6,39,39,896,896,3869.76,5000.9),(487,43,5,17,17,680,680,512,3074.13),(488,44,6,30,30,752,752,8724.64,5653.27),(489,456,7,11,11,440,440,770,1377.25),(490,458,6,7,8,248,164,1728.16,1053.35),(491,459,7,3,3,48,48,330.042,446.996),(492,46,6,113,113,4520,4520,26482.6,22687.5),(493,463,6,45,45,816,816,2715.84,3760.81),(494,466,6,36,36,1440,1400,6350.4,5401.15),(495,468,5,32,32,1280,1280,3979.6,5262.55),(496,469,6,4,4,160,160,779.2,568.796),(497,473,6,203,203,4872,4872,73177.4,72458.6),(498,475,6,1,1,40,40,362.4,325.373),(499,481,7,661,661,7008,7008,2312.64,5509),(500,485,7,108,108,1728,1728,11881.5,11706),(501,486,6,73,73,2920,2920,23360,20890.2),(502,487,6,74,74,1184,1037,8276.16,10103.5),(503,488,6,147,147,4760,4760,25065.6,24884.7),(504,489,6,10,10,80,127,550.07,980.054),(505,490,7,14,14,560,520,3850.49,1920.71),(506,493,6,16,16,640,640,2880,3765),(507,494,6,1,1,24,24,165.021,452.436),(508,496,6,4,4,112,136,195.36,484.636),(509,498,6,312,312,12480,12480,82800,84788.4),(510,499,5,1455,1455,51728,51728,519495,531495),(511,503,6,10,10,400,293,700,1223.2),(512,505,6,3,5,48,72,505.44,484.747),(513,515,6,7,7,280,209,1467.2,1191.48),(514,52,6,2,2,80,80,89.6,71.2927),(515,523,6,77,77,3016,3016,4310.56,4749.83),(516,526,6,872,853,34832,34832,114412,134688),(517,527,6,64,64,1432,1432,25155,15927.4),(518,54,6,5,3,80,80,249.6,667.232),(519,544,6,44,44,1184,1184,7510.91,6535.49),(520,547,6,3,3,120,120,825.105,620.024),(521,577,6,12,12,312,312,973.2,1597.18),(522,578,6,170,170,5440,5440,61308.8,59769.6),(523,581,7,27,27,1080,1080,1890,3771.73),(524,583,6,10,10,160,127,44.8,1053.35),(525,585,7,27,27,664,787,4548.88,4010.74),(526,586,6,64,64,2560,2032,4812.8,8699.29),(527,588,6,15,15,480,480,2078.4,2133.55),(528,590,6,13,13,312,312,4421.04,1753.89),(529,599,6,134,134,4040,4040,27778.5,23159.9),(530,6,7,107,107,1712,1712,14072.6,14127.8),(531,60,7,50,50,1600,1600,3008,6098.71),(532,606,5,102,102,2736,2736,14440.8,16035.9),(533,607,6,45,45,1800,1800,2700,2898.46),(534,609,6,28,28,1120,1120,1960,2182.25),(535,610,5,168,168,3568,3568,24533.1,22784.7),(536,612,6,6,6,216,216,364,442.974),(537,625,6,1,1,8,8,20,42.3428),(538,627,5,3,3,104,104,305.021,834.54),(539,628,6,16,16,128,128,880.112,1619.04),(540,63,7,2,3,80,75,419.2,316.573),(541,634,5,2,2,32,37,223.68,305.62),(542,635,6,84,84,3360,3360,14173.6,11388.7),(543,637,6,16,16,256,256,1760.22,1562.29),(544,64,6,2,2,80,80,150.4,453.089),(545,641,6,24,24,960,960,4320,3979.18),(546,642,6,19,19,304,304,948.48,2202.78),(547,643,6,5,5,200,200,700,668.69),(548,645,6,32,32,1280,1280,1433.6,5193.63),(549,65,7,22,22,736,787,5637.76,3383.67),(550,651,6,17,17,272,272,6128.16,2444.63),(551,652,6,4,4,128,128,674.56,568.796),(552,656,6,211,211,2680,2680,27850,32582.2),(553,657,5,1162,1162,16760,16760,164899,170460),(554,659,7,16,16,384,384,960,1037.57),(555,661,6,20,20,640,640,7212.8,5676.91),(556,67,6,22,22,880,880,1540,3740.55),(557,69,6,43,43,1032,1032,340.56,5100.63),(558,70,7,5,5,160,160,1100.14,647.253),(559,705,6,38,38,1424,1424,6793.92,5466.91),(560,706,7,1,1,40,40,200,287.623),(561,710,7,25,25,1000,1000,1120,1258.95),(562,716,6,582,582,14128,14128,4930.24,11346.3),(563,73,6,3,3,120,120,1081.2,619.692),(564,746,6,96,96,2712,2712,11743,9592.55),(565,748,6,89,89,3560,3560,13350,13431.2),(566,752,6,556,556,22096,22096,171243,171282),(567,753,6,18,18,288,288,2013.12,2287.66),(568,759,6,85,85,2536,2399,5755.76,9242.92),(569,763,5,226,226,5424,5424,81468.5,73996.9),(570,769,6,5,5,120,120,657.6,668.69),(571,77,5,459,459,18360,18360,93444.8,91290.3),(572,775,6,7,8,224,288,969.92,1053.35),(573,776,6,7,7,224,224,2524.48,1774.86),(574,779,6,92,92,3680,3600,11040,11355.9),(575,780,7,32,32,1280,1280,5388.8,5081.3),(576,79,6,1,1,40,40,275.035,376.602),(577,794,6,130,130,5200,5200,35754.5,35808.4),(578,804,6,102,102,4080,4080,6976.8,7507.29),(579,810,7,3,3,72,72,267.12,511.882),(580,811,7,46,46,1408,1408,6472.96,5935.08),(581,812,7,3,3,104,104,715.091,512.214),(582,823,7,20,14,320,320,2560,2490.2),(583,825,5,14,14,224,224,1540.2,1878.53),(584,83,6,50,50,1104,1104,7590.96,7776.96),(585,86,6,28,28,896,896,8117.76,5837.13),(586,877,7,8,14,320,352,2560,1450.83),(587,881,6,14,14,336,336,1246.56,997.178),(588,883,6,68,68,1280,2399,8057.6,8354.16),(589,884,6,2,2,80,80,89.6,663.362),(590,885,6,15,15,352,352,2420.31,1862.38),(591,891,6,1,1,16,16,26.72,42.3428),(592,902,6,977,977,25400,25400,17708.4,24537.5),(593,906,6,17,17,408,520,2864.16,2316.15),(594,910,6,4,4,160,160,673.6,568.796),(595,92,5,10,10,400,380,934,1161.57),(596,922,7,403,403,16120,16120,16120,24641),(597,925,6,38,38,608,608,200.64,4110.54),(598,926,7,16,16,256,256,640,986.55),(599,929,6,3,3,96,72,628.8,500.948),(600,930,6,12,12,416,416,1275.52,1597.18),(601,934,7,107,107,4040,4040,12510,16266.2),(602,937,6,412,412,6592,6592,45325.8,46082.2),(603,941,7,3,3,120,108,540,531.046),(604,942,6,412,412,16480,16480,226221,220817),(605,945,6,532,532,20320,20320,139718,141464),(606,946,6,8,8,200,209,1140.8,1069.66),(607,950,5,170,170,5840,5840,40155.1,41427.2),(608,952,6,2,2,16,21,336.96,338.121),(609,961,6,21,21,840,840,1244.8,1437.76),(610,965,6,355,355,14200,14200,84105.1,89313.9),(611,967,5,101,101,2840,2840,15711.2,16790),(612,97,6,10,10,160,160,3604.8,1610.67),(613,970,6,84,84,3360,3360,5040,4611.66),(614,971,6,153,153,3408,4688,25959.8,26476.1),(615,972,6,98,98,3920,3920,16503.2,18124.9),(616,974,6,36,36,1248,1229,4700.16,5218.84),(617,977,6,27,27,216,216,540,2991.46),(618,979,6,19,19,304,304,2090.27,3255.54),(619,98,7,1,1,40,40,275.035,437.22),(620,980,6,87,87,1696,2399,14660.8,11757.3),(621,982,6,401,401,8720,8720,59957.6,60749.6),(622,985,6,1,1,16,16,110.014,325.373),(623,986,6,2,2,80,56,140,354.323),(624,989,7,9,10,336,352,427.68,1450.83),(625,99,6,67,67,536,536,3685.47,7311.67),(626,995,7,376,376,10472,10472,42352.3,39909.1);
/*!40000 ALTER TABLE `fact_ci_rs` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-02-10  2:08:22

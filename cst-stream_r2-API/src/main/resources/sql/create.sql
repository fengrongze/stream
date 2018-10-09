REATE TABLE `app_oil_price` (
  `id` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
  `province` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `b90` double DEFAULT NULL,
  `b93` double DEFAULT NULL,
  `b97` double DEFAULT NULL,
  `b0` double DEFAULT NULL,
  `insert_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `remark` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_oil_price_province` (`province`) USING HASH,
  KEY `idx_province_inserttimestamp` (`province`,`insert_timestamp`) USING BTREE
)
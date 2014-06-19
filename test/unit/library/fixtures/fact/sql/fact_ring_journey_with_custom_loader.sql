CREATE TABLE `fact_ring_journey_with_custom_loader` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dim_ring` int(11) NOT NULL,
  `dim_location` int(11) NOT NULL,
  `fellowship_count` int(11) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `fact_ring_journey_uk` (`dim_ring`,`dim_location`),
  KEY `fact_ring_journey_ibfk_2` (`dim_location`),
  CONSTRAINT `fact_ring_journey_ibfk_1` FOREIGN KEY (`dim_ring`) REFERENCES `dim_ring` (`id`),
  CONSTRAINT `fact_ring_journey_ibfk_2` FOREIGN KEY (`dim_location`) REFERENCES `dim_location` (`id`)
) DEFAULT CHARSET=utf8

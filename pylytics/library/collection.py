CREATE_STAGING_TABLE = """\
CREATE TABLE `staging` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `collector_type` varchar(127) NOT NULL,
  `fact_table` varchar(255) NOT NULL,
  `value_map` text NOT NULL,
  `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `collector_type` (`collector_type`),
  KEY `fact_table` (`fact_table`),
  KEY `created` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

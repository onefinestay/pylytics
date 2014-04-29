CREATE TABLE `dim_timezone` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `country_code` VARCHAR(5) NOT NULL,
  `timezone` VARCHAR(30) NOT NULL,
  `utc_offset_in_hours` DECIMAL(4,2),
  `utc_offset_in_minutes` INT(4),
  `utc_offset_in_seconds` INT(5),
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY UQ_home_shortcode (`timezone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

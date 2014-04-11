CREATE TABLE `dim_timezone` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  # Insert your custom fields here.
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

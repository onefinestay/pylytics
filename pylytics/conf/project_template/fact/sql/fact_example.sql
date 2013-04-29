CREATE TABLE `fact_example` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  # Insert your custom fields here.
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8


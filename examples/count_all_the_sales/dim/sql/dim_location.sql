CREATE TABLE `dim_location` (
  `id` int(11) NOT NULL AUTO_INCREMENT,  # Required
  `location` CHAR(255) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  # Required
  PRIMARY KEY (`id`),
  CONSTRAINT UQ_location UNIQUE (`location`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
CREATE TABLE `sales` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `location_name` CHAR(20) NOT NULL,
  `sale_value` decimal(12,2) NOT NULL,
  `sale_date` date NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `sales` VALUES 
(null, 'London', 523.12, '2013-01-01', CURDATE()),
(null, 'San Francisco', 1231.12, '2013-01-01', CURDATE()),
(null, 'Sao Paulo', 36.73, '2013-01-01', CURDATE()),
(null, 'London', 497.12, '2013-01-01', CURDATE()),
(null, 'London', 897.12, '2013-01-01', CURDATE()),
(null, 'San Francisco', 1231.52, '2013-01-01', CURDATE()),
(null, 'Sao Paulo', 31.98, '2013-01-02', CURDATE()),
(null, 'San Francisco', 231.12, '2013-01-02', CURDATE()),
(null, 'Sao Paulo', 439.09, '2013-01-02', CURDATE()),
(null, 'London', 865.18, '2013-01-03', CURDATE()),
(null, 'San Francisco', 984.90, '2013-01-03', CURDATE()),
(null, 'Sao Paulo', 112.89, '2013-01-03', CURDATE()),
(null, 'London', 834.90, '2013-01-03', CURDATE()),
(null, 'London', 156.23, '2013-01-03', CURDATE()),
(null, 'San Francisco', 121.24, '2013-01-04', CURDATE()),
(null, 'Sao Paulo', 32.87, '2013-01-04', CURDATE()),
(null, 'San Francisco', 21.12, '2013-01-04', CURDATE()),
(null, 'Sao Paulo', 43.98, '2013-01-04', CURDATE());

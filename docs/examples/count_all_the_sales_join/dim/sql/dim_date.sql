CREATE TABLE `dim_date` (
  `id` int(11) NOT NULL AUTO_INCREMENT,  # Required
  `date` date NOT NULL,
  `day` int(11) NOT NULL,
  `day_name` enum('Mon','Tue','Wed','Thu','Fri','Sat','Sun') NOT NULL,
  `day_of_week` int(11) NOT NULL,
  `week` int(11) NOT NULL,
  `full_week` varchar(15) NOT NULL,
  `month` int(11) NOT NULL,
  `month_name` enum('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec') NOT NULL,
  `full_month` varchar(15) NOT NULL,
  `quarter` int(11) NOT NULL,
  `quarter_name` enum('Q1','Q2','Q3','Q4') NOT NULL,
  `full_quarter` varchar(15) NOT NULL,
  `year` int(11) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  # Required
  PRIMARY KEY (`id`),
  CONSTRAINT UQ_location UNIQUE (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
CREATE TABLE `fact_count_all_the_sales` (
  `id` int(11) NOT NULL AUTO_INCREMENT,  # Required
  `dim_date` int(11) NOT NULL,
  `dim_location` int(11) NOT NULL,
  `fact_count` int(11) NOT NULL DEFAULT '0',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  # Required
  PRIMARY KEY (`id`),
  INDEX `dim_date` (`dim_date`),
  INDEX `dim_location` (`dim_location`),
  CONSTRAINT `fact_count_all_the_sales_ibfk_1` FOREIGN KEY (`dim_date`) REFERENCES `dim_date` (`id`),
  CONSTRAINT `fact_count_all_the_sales_ibfk_2` FOREIGN KEY (`dim_location`) REFERENCES `dim_location` (`id`),
  CONSTRAINT UQ_date_location UNIQUE (`dim_date`, `dim_location`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# Creating a view is optional.
CREATE VIEW `view_fact_count_all_the_sales` AS (
  SELECT
    dd.date AS `date`,
    f.fact_count AS `count`,
    l.location AS location
  FROM
    fact_count_all_the_sales f
  JOIN
    dim_date dd ON f.dim_date = dd.id
  JOIN
    dim_location l ON f.dim_location = l.id
);

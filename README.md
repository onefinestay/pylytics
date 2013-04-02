pylytics
========

This is a set of python libraries that allow you to build and manage a star schema using data from a diverse set of inputs.

See http://en.wikipedia.org/wiki/Star_schema

The star schema is a simple approach to data warehousing.  

In contrast to big data tools, the star schema is suited to mid-size data problems.

Its biggest benefit is the strict generating and management of facts.

facts and dimensions
--------------------
The pylitics project creates a set of *facts* based on *dimensions*

The simplest definition of dimensions are the finite data sets you wish to measure against.

Common dimensions include dates, lists of customers and sizes/colors.

Facts are the measurements you take.  They are most often calculations.  

Example facts include gross sales by color per day, total clicks per customer and customers added per week.

building dimensions
-------------------
1) add the dimension table, e.g.
    CREATE TABLE `dim_date` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
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
            `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            CONSTRAINT UQ_location UNIQUE (`date`)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8


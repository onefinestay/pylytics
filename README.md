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
1) add the dimension table sql, e.g. blob/master/dim/sql/dim_date.sql
2) write a script to update the dimension, e.g. blob/master/dim/dim_date.py

building facts
--------------
3) add the fact table sql, e.g. 
4) write a script to update the dimension, e.g.

note that the facts include a list of dimensions

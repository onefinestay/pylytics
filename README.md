pylytics
========

This is a set of python libraries that allow you to build and manage a star schema using data from a diverse set of inputs.

See http://en.wikipedia.org/wiki/Star_schema

The star schema is a simple approach to data warehousing.  In contrast to big data tools, the star schema is suited to mid-size data problems.

Its biggest benefit is the strict generaton and management of facts.

facts and dimensions
--------------------
The pylitics project creates a set of *facts* based on *dimensions*

Dimensions are the finite data sets you wish to measure your facts against.  Common dimensions include dates, lists of customers and sizes/colors.

Facts are the measurements you take.  They are most often calculations or summations.  Example facts include gross sales by color per day, total clicks per customer and customers added per week.

building dimensions
-------------------
1) add the dimension table sql, e.g. [dim/sql/dim_date.sql](dim/sql/dim_date.sql)

2) write a script to update the dimension, e.g. [dim/dim_date.py](dim/dim_date.py)

building facts
--------------
3) add the fact table sql, e.g. [fact/sql/fact_count_allthesales.sql](fact/sql/fact_count_allthesales.sql)

4) write a script to update the dimension, e.g. [dimension/sql/dim_date.sql](dimension/sql/dim_date.sql)

**note that the fact script includes a list of dimensions (dim_names) and a query ... that's it.

run pylytics
------------
Run your scripts using *manage.py* (from the command line or cron)
e.g.
``` python
./manage.py all update
```

Which updates all Fact and Dimension tables.

tests
-----
The [base fact](library/fact.py) and [base dimension](library/dim.py) classes contain a test method.  This method must be overriden by each fact and dimension script.

Run tests as
```python
py manage.py tests
```

pylytics
========

Introduction
************
This is a set of python libraries that allow you to build and manage a `star schema <http://en.wikipedia.org/wiki/Star_schema>`_.

The star schema is a simple approach to data warehousing.  In contrast to big data tools, the star schema is suited to mid-size data problems.

Its biggest benefit is the strict generation and management of facts.


facts and dimensions
--------------------
The pylitics project creates a set of *facts* based on *dimensions*.

Dimensions are the finite data sets you wish to measure your facts against.  Common dimensions include dates, lists of customers and sizes/colours.

Facts are the measurements you take.  They are most often calculations or summations.  Example facts include gross sales by colour per day, total clicks per customer and customers added per week.


Documentation
*************
The full documentation is available at `readthedocs.org <https://pylytics.readthedocs.org/en/latest/index.html>`_

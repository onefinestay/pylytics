pylytics
========

Introduction
************
This is a set of Python libraries that allow you to build and manage a `star schema <http://en.wikipedia.org/wiki/Star_schema>`_.

The star schema is a simple approach to data warehousing. In contrast to big data tools, the star schema is suited to mid-size data problems.

Its biggest benefit is the strict generation and management of facts.


facts and dimensions
--------------------
The pylitics project creates a set of *facts* based on *dimensions*.

Dimensions are the finite data sets you wish to measure your facts against. Common dimensions include dates, lists of customers and sizes/colours.

Facts are the measurements you take.  They are most often calculations or summations. Example facts include gross sales by colour per day, total clicks per customer and customers added per week.


Documentation
*************
The full documentation is available at `readthedocs.org <https://pylytics.readthedocs.org/en/latest/index.html>`_


Version 1
*********

Version 1 of pylytics is a considerable departure from the previous stable version (v.0.7.0).

Projects created using pylytics versions < 1.0 are not compatible with pylytics >= 1.0.

We recommend you version pin your old pylytics projects to v.0.7.0.


Installing the MySQL connector
******************************

Pylytics v1 uses Oracle's Python MySQL adapter. It can be installed using pip, but isn't hosted on PyPi.

Depending on your version of pip, you may need to install the requirements as follows::

    pip install -r requirements.txt --allow-external mysql-connector-python


Supported MySQL version
***********************

Pylytics has been tested with MySQL versions 5.5.37 and 5.6.5.

The recommended version is 5.6.5.

Getting Started
===============

Creating a new pylytics project
*******************************
Create a virtual environment and activate it. Then install pylytics::

    pip install git+ssh://git@github.com/onefinestay/pylytics.git

This installation adds pylytics-admin.py to the path. Use this to create a new pylytics project::

    pylytics-admin.py my_project_name

This creates the my_project_name folder in the current directory, with a skeleton project inside.



Project settings
****************
Make sure that the DATABASES dictionary in settings.py contains the details for all databases you need.

At the moment, only MySQL databases are supported.



Writing fact and dimensions
***************************
See the examples folder in this repository for examples.

The naming convention is strict - all dim files must start with `dim_`, likewise all fact files much start with `fact_`.


building dimensions
-------------------
1) add the sql for creating the dimension table, e.g. [dim/sql/dim_date.sql]

2) write a script to update the dimension, e.g. [dim/dim_date.py]


building facts
--------------
1) add the sql for creating the fact table, e.g. [fact/sql/fact_count_allthesales.sql]

2) write a script to update the dimension, e.g. [fact/fact_count_allthesales.py]



Running scripts
***************
The manage.py file in the project directory is used for building fact and dimension tables, and also for updating them.

A CRON job should run the following command periodically to keep your tables up to date::

    ./manage.py all update



Tests
*****
The base table class contain a test method. This method must be overriden by each fact and dimension script to enable tests.

Run tests by:

    ./manage.py all tests

Note - test functionality is still under active development.

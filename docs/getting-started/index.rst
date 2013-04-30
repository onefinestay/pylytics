Getting Started
===============

Creating a new pylytics project
*******************************
Create a virtual environment and activate it. Then install pylytics::

    pip install pylytics

This installation adds pylytics-admin.py to the path. Use this to create a new pylytics project::

    pylytics-admin.py my_project_name

This creates the my_project_name folder in the current directory, with a skeleton project inside.



Project settings
****************
Make sure that the DATABASES dictionary in settings.py contains the details for all databases you need.

At the moment, only MySQL databases are supported.



Writing facts and dimensions
****************************
The project folder contains *dim* and *fact* folders, which contain your dimension and fact scripts.

See the examples folder in the git repository for examples.

The naming convention is strict - all dim files must start with `dim_`, likewise all fact files much start with `fact_`.


building dimensions
-------------------
1) add the sql for creating the dimension table, e.g. [dim/sql/dim_example.sql]

2) write a script to update the dimension, e.g. [dim/dim_example.py]


building facts
--------------
1) add the sql for creating the fact table, e.g. [fact/sql/fact_count_example.sql]

2) write a script to update the dimension, e.g. [fact/fact_count_example.py]



Running scripts
***************
The manage.py file in the project directory is used for building and updating the star schema.

You can specify the facts to run. For example::

    ./manage.py fact_count_example_1 [fact_count_example_2] {update,build,test,historical}

Or run the command for all facts::

    ./manage.py all {update,build,test,historical}


build
-----
This will make sure that the relevant tables have been created for the facts specified.


update
------
This command automatically calls `build` before executing. It updates your fact and dimension tables.

A CRON job should run the command periodically to keep your tables up to date.


historical
----------
Facts are usually built each day by running *update*. However, in some cases it's useful to be able to rebuild the tables for the last X days (for example, if the project is just starting off, or data loss has occurred).


test
----
The base table class contain a test method. This method must be overridden by each fact and dimension script to enable tests.

Note - test functionality is still under active development.

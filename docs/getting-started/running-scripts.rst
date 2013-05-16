Running scripts
===============
The manage.py file in the project directory is used for building and updating the star schema.

You can specify the facts to run. For example::

    ./manage.py fact_count_example_1 [fact_count_example_2] {update,build,test,historical}

Or run the command for all facts::

    ./manage.py all {update,build,test,historical}


build
*****
This will make sure that the relevant tables have been created for the facts specified, by executing the corresponding .sql files.


update
******
This command automatically calls `build` before executing. It updates your fact and dimension tables.

A CRON job should run the command periodically to keep your tables up to date.


historical
**********
Facts are usually built each day by running *update*. However, in some cases it's useful to be able to rebuild the tables (for example, if the project is just starting off, or data loss has occurred).

If no `historical_source_query` is defined for the fact, then it raises a warning.


test
****
The base table class contain a test method. This method must be overridden by each fact and dimension script to enable tests.

Note - test functionality is still under active development.


Specifying the settings file location
*************************************
When a new pylytics project is created using pylytics-admin.py, a settings.py file is automatically added to the project directory.

However, when several pylytics projects are on a single server it sometimes makes sense to have a single settings.py file in a shared location, e.g. in /etc/pylytics/settings.py.

In this case, use::

    ./manage.py all {update,build,test,historical} --settings='/etc/pylytics'

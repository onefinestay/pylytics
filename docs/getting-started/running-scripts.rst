Running scripts
===============
The manage.py file in the project directory is used for building and updating the star schema.

You can specify the facts to run. For example::

    ./manage.py fact_count_example_1 [fact_count_example_2] {update,build,test,historical}

Or run the command for all facts::

    ./manage.py all {update,build,test,historical}


build
*****
This will make sure that the relevant tables have been created for the facts specified.


update
******
This command automatically calls `build` before executing. It updates your fact and dimension tables.

A CRON job should run the command periodically to keep your tables up to date.


historical
**********
Facts are usually built each day by running *update*. However, in some cases it's useful to be able to rebuild the tables for the last X days (for example, if the project is just starting off, or data loss has occurred).


test
****
The base table class contain a test method. This method must be overridden by each fact and dimension script to enable tests.

Note - test functionality is still under active development.

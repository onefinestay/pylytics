Running scripts
===============

The manage.py file in the root of the project directory is used for building and updating the star schema.

You can specify the facts to run. For example::

    ./manage.py {update,build,test,historical} fact_1 [fact_2]

Or run the command for all facts::

    ./manage.py {update,build,test,historical} all


Commands
********

build
~~~~~

This will make sure that the relevant tables have been created for the facts specified, as well as any dimensions that the fact requires.


update
~~~~~~

This command automatically calls `build` before executing. It updates your fact and dimension tables.

You can just make the scheduled facts run as follows::

    ./manage.py {update,build,test,historical} scheduled

A common way of running pylytics in production is to setup a CRON job which calls `manage.py update scheduled` every 10 minutes.


historical
~~~~~~~~~~

Facts are usually built each day by running *update*. However, in some cases it's useful to be able to rebuild the tables (for example, if the project is just starting off, or data loss has occurred).

When the `update` command is run, it gets data from the `__source__` property of the Fact and Dimension classes.

With the `historical` command, it first looks for a `__historical_source__` property of the Fact and Dimension classes. If it exists then it is used instead of `__source__`. Here is an example::

    class Sales(Fact):

        __source__ = DatabaseSource.define(
            database="sales",
            query="SELECT * FROM sales_table WHERE created > NOW() - INTERVAL 1 DAY"
        )

        __historical_source__ = DatabaseSource.define(
            database="sales",
            query="SELECT * FROM sales_table"
        )


Specifying the settings file location
*************************************

When a new pylytics project is created using pylytics-admin.py, a settings.py file is automatically added to the project directory.

However, when several pylytics projects are on a single server it sometimes makes sense to have a single settings.py file in a shared location, e.g. in /etc/pylytics/settings.py.

In this case, use::

    ./manage.py --settings='/etc/pylytics' {update,build,test,historical} fact_1 [fact_2]

Source Types
============

Each fact and dimension has to specify a source for extracting data from.

DatabaseSource
**************

`DatabaseSource` makes use of connections defined in `DATABASES` in `settings.py` to make queries to a MySQL database.

Declaring
~~~~~~~~~

Here are some examples of how a `DatabaseSource` can be defined::

    # load.py

    class Manager(Dimension):

        __source__ = DatabaseSource.define(
            database="sales",
            query="SELECT name AS manager FROM managers"
        )

        manager = NaturalKey('manager', basestring)


CallableSource
**************

`CallableSource` is the most common source used. The source data is any callable Python object.

This callable could generate the data programatically, pull from an API, make a query to a database, parse a flat file etc.

The only constraint is that the callable must return the data in a certain format - either as a sequence of tuples, or a sequence of dictionaries.

For example::

    # extract.py

    def my_simple_datasource():
        return ({'size': 'large'}, {'size': 'medium'}, {'size': 'small'})

    # Or alternatively:

    def my_simple_datasource():
        return (('size', 'large'), ('size', 'medium'), ('size': 'small'))

Declaring
~~~~~~~~~

Here are some examples of how a `CallableSource` can be defined::

    # load.py

    from extract import my_simple_datasource


    class StoreSize(Dimension):

        __source__ = CallableSource.define(
            _callable=staticmethod(my_simple_datasource)
            )

        size = NaturalKey('size', basestring)


    # For very simple callables, you can specify then as lambdas:

    class StoreOpenWeekends(Dimension):

        __source__ = CallableSource.define(
            _callable=staticmethod(
                lambda: [{'open_weekends': True}, {'open_weekends': False}]
                )
            )

        open_weekends = NaturalKey('open_weekends', bool)

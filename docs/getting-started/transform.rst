Transform
=========

Introduction
------------

Once data has been extracted from a ``DatabaseSource`` or ``CallableSource``, it can be cleaned and expanded upon by 'expansions'.

Conceptually, pylytics is a pipeline from extract to load.

Each row which comes from the source is passed through this pipeline.

The pipeline consists of a number of expansions, which transform the data. At the end of the pipeline, the data is ready to the loaded.

The expansions are simple, testable functions. For example::

    # transform.py

    def convert_datetime_to_date(data):
        """ We're only interested in the created date, and not the time.
        """
        data['created_date'] = data['created_datetime'].date()
        del data['created_datetime']


    def convert_str_to_int(data):
        """ The source returns integers as strings - convert them.
        """
        data['sales'] = int(data['sales'])

The ``data`` argument is a dictionary representing a single row.

Adding expansions to facts and dimensions
-----------------------------------------

For example::

    # load.py

    from transform import convert_datetime_to_date, convert_str_to_int


    class Sales(Fact):

        __source__ = DatabaseSource.define(
            database="sales",
            query="SELECT * FROM sales_table",
            expansions=[convert_datetime_to_date, convert_str_to_int]
        )

        ...

Both DatabaseSource and CallableSource accept the ``expansions`` argument.

The expansions are processed in the order they appear in the list.

Writing facts and dimensions
============================

Dimensions
----------

Column Types
~~~~~~~~~~~~

The two column types in a `Dimension` are `NaturalKey` and `Column`.

NaturalKey
**********

`NaturalKey` columns uniquely identify a row in a dimension table.

Examples of `natural keys` are telephone number, National Insurance number (UK), Social Security number (US), car number plate etc. They are values which are naturally unique, as opposed to a `surrogate key` which is an integer assigned to a row, but has no relationship to the underlying data in that row.

Taking the following as an example::

    # load.py

    class Store(Dimension):

        __source__ = NotImplemented

        store_id = NaturalKey('store_id', int)
        store_shortcode = NaturalKey('store_shortcode', basestring)
        store_size = Column('store_size', basestring)
        employees = Column('employees', int)


    class Sales(Fact):

        __source__ = NotImplemented

        sales = Metric('sales', int)
        store = DimensionKey('store', Store)
        ...

The NaturalKey has two arguments - the column name, and a Python type. The Python types are mapped to MySQL types when the tables are created. Here are some examples::

    _type_map = {
       bool: "TINYINT",
       date: "DATE",
       datetime: "TIMESTAMP",
       Decimal: "DECIMAL(%s,%s)",
       float: "DOUBLE",
       int: "INT",
       long: "INT",
       timedelta: "TIME",
       time: "TIME",
       basestring: "VARCHAR(%s)",
       str: "VARCHAR(%s)",
       unicode: "VARCHAR(%s)",
       bytearray: "VARBINARY(%s)"
    }

The Python type serves another purpose. Using the example above, if `Sales.store` = 'LON1', pylytics will try and find a matching `Store` row via the following process:

* Is there a `Store` `NaturalKey` with the same type?
* In this case yes - `Store.store_shortcode` is a `basestring`, which is a parent type of `str`.
* Find the surrogate key for the `Store` row where `Store.store_shortcode` == 'LON1'.
* Replace 'LON1' with the surrogate key, as a foreign key to the Store table.

Column
******

`Column` has no special interactions in pylytics. It just represents a database column, which describes the dimension.

As an example, for a date dimension, we can potentially have many columns, which provide lots of ways to filter the fact which references it::

    class Date(Dimension):

        __source__ = NotImplemented

        iso_date = NaturalKey('iso_date', basestring)  # 2000-01-01
        day = Column('day', int)  # 1 - 31
        day_name = Column('day_name', basestring)  # Wednesday
        day_of_week = Column('day_of_week', int)  # 1 - 7
        month_name = Column('month_name', basestring)  # December
        month_number = Column('month_number', int)  # 1 - 12
        year = Column('year', int)  # 2000
        quarter = Column('year', int)  # 1 - 4

Adding columns is an important part of making the star schema useful for analysis.


Facts
-----

Column Types
~~~~~~~~~~~~

Metric
******

`Metric` columns store numeric values.

A `Fact` doesn't have to include `Metric` columns. It can just be a collection of `DimensionKey` columns.

DimensionKey
************

A DimensionKey is defined as follows::

    class Sales(Fact):

        __source__ = NotImplemented

        store = DimensionKey('store', Store)
        ...

The first argument is the name of the column to be created. The second argument is a Dimension subclass.

Another argument which can be important is `optional`.

By default, dimension keys cannot be null. However, for some use cases, this is not possible.

An example is a user submitted questionnaire, where a lot of the fields have been missed out. In this case, we make the dimension key optional::

    class UserQuestionnaire(Fact):

        __source__ = NotImplemented

        rating = DimensionKey('rating', UserRating, optional=True)
        ...

DegenerateDimension
*******************

A `DegenerateDimension` stores the dimension value in the fact itself, rather than using a foreign key to a dimension table, as is the case with `DimensionKey`.

Use `DegenerateDimension` when the dimension doesn't warrant its own table - for example, in a sales fact there might be an order_id. Having this in a separate table doesn't save any space, and results in an unneccessary join.

    class Sales(Fact):

        __source__ = NotImplemented

        order_id = DegenerateDimension('order_id', basestring)

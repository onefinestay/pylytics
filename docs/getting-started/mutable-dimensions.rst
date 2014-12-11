Mutable Dimensions
==================

If you're new to pylytics, skip this section.

Introduction
------------

Dimensions change over time. To use our store example::

    class Store(Dimension):

        __source__ = NotImplemented

        store_id = NaturalKey('store_id', int)
        store_shortcode = NaturalKey('store_shortcode', basestring)
        store_size = Column('store_size', basestring)
        employees = Column('employees', int)

Over time, the number of ``employees`` might change, and so might the ``store_size`` if it gets an extension.

Fact and dimension tables in pylytics are idempotent, meaning you could run `manage.py historical fact_1`, and if the rows already exist they'll be left alone.

However, if any of the dimension columns change, a new row will be inserted. For example, if a store extension happens to 'LON1': 

========  =============== ========== ========= ===================
Store
------------------------------------------------------------------
store_id  store_shortcode store_size employees applicable_from
========  =============== ========== ========= ===================
1         'LON1'          'small'    100       2010-01-01 00:00:00
1         'LON1'          'medium'   100       2014-01-01 00:00:00
========  =============== ========== ========= ===================

Notice the 'applicable_from' column.

The next time the ``Store`` fact updates, it will refer to the latest version of the dimension, but existing fact rows will still point to the dimension row that was relevant when they were created.

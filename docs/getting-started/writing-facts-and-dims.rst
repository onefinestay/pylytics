Writing facts and dimensions
============================
The project folder contains *dim* and *fact* folders, which contain your dimension and fact scripts.

See the `examples folder <https://github.com/onefinestay/pylytics/tree/master/examples>`_ in the git repository for examples.

The naming convention is strict - all dim files must start with `dim_`, likewise all fact files much start with `fact_`.


Facts
*****
SQL
---
Add the SQL for creating the fact table into the `fact/sql` folder, e.g. `fact/sql/fact_count_all_the_sales.sql`.

.. include:: ../../examples/count_all_the_sales/fact/sql/fact_count_all_the_sales.sql
    :code:

Defining a view at the end is optional.

Script
------
Write a script to update the fact, and add it to the `fact` folder e.g. `fact/fact_count_all_the_sales.py`
    
.. include:: ../../examples/count_all_the_sales/fact/fact_count_all_the_sales.py
    :code:
    
When the fact is updated, `source_query` is run on `source_db`.

The query returns a tuple of tuples like this::

    (('2013-01-01', 'London', 300), ...)

`dim_names` maps each value to a corresponding dimension. If the value doesn't correspond to a dimension, then put None instead.

When the values are inserted into the star schema they're replaced with foreign keys pointing to the relevant dimension.

`dim_fields` is the dimension column used when trying to match the fact to the corresponding dimension.

Historical
~~~~~~~~~~
`source_query` is intended to be run every day, and just update the values for the current day.

You can also specify a `historical_source_query` which is iterated over.


Dimensions
**********
SQL
---
Write a script to update the dimension, e.g. `dim/dim_location.py`

.. include:: ../../examples/count_all_the_sales/dim/dim_location.py
    :code:

Script
------
Add the SQL for creating the dimension table, e.g. `dim/sql/dim_location.sql`

.. include:: ../../examples/count_all_the_sales/dim/sql/dim_location.sql
    :code:


TODO - overriding methods - like update.


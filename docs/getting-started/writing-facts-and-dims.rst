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

Script
------
Write a script to update the fact, and add it to the `fact` folder e.g. `fact/fact_count_all_the_sales.py`::
    
    """fact_count_all_the_sales.py - an example Fact script."""
    from pylytics.library.fact import Fact


    class FactCountAllthesales(Fact):
    
        source_db = 'test'  # A database defined in settings.py
    
        dim_names = ['dim_date', 'dim_location', None]
        dim_fields = ['date', 'location', None]

        source_query = """
            SELECT
                sale_date as `date`,
                location_name AS location,
                SUM(sale_value) AS total_sales
            FROM
                sales
            GROUP BY
                location, `date`;
            """

When the fact is updated, `source_query` is run on `source_db`.

Historical
----------
TODO


Dimensions
**********
Script
------
Add the SQL for creating the dimension table, e.g. `dim/sql/dim_location.sql`

SQL
---
Write a script to update the dimension, e.g. `dim/dim_location.py`


TODO - overriding methods - like update.


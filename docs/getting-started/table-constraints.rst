Table Constraints
=================

hash_key
--------

Pylytics creates the fact and dimension table with certain constraints.

If you look at the fact and dimension tables, they have a column called `hash_key`. This column has a unique constraint.

For a fact table, the hash_key is a hash of all of the dimension values. This is a very important thing to keep in mind when designing your fact tables. The fact row has to be uniquely identified by its dimensions.

In the example below, the fact table is for sales in a retail store.

The dimensions are store, customer, date, and order_id. The metric is sales_amount.

If the order_id dimension wasn't there, and the same customer purchased from the same store twice in one day, the fact table wouldn't be able to record two separate rows.

========  =============== ========== ========= ===================
Sales
------------------------------------------------------------------
store     customer        date       order_id  sales_amount
========  =============== ========== ========= ===================
1         12335           400        100       100.00
1         12335           400        101       10.50
========  =============== ========== ========= ===================

The dimension can be a DimensionKey, which is a pointer to a row in a dimension table, or a DegenerateDimension, which stores the dimension value in the fact table itself.

Reasoning
~~~~~~~~~

The hash_key is there for a very important reason. Most of the times when data is inserted into a datawarehouse, there is considerable overlap with data already in the datawarehouse. For example, you might be downloading the last 7 days worth of data from an API each day, and inserting it into the datawarehouse. Perhaps the API only allows you to retrieve data for the last 7 days, or you're concerned about there being missing rows. Either way, the datawarehouse shouldn't create multiple rows for the same data.


Insert Types
------------

By default pylytics inserts data using the 'INSERT IGNORE' syntax for MySQL.

This means that if you try and insert data which has the same dimension values as an existing row, it will fail silently.

This behaviour is fine for *most* cases, but definitely not all.

It works for data sources which don't change after they've been created. For example, the sales fact above. The `sales_amount` shouldn't change after it was created.

In situations where the metrics do change, you can use the 'REPLACE INTO' syntax for MySQL. If you try and insert a row with the same dimension values, it will replace the old row. To do this, add the following to your Fact classes::

    class MyFact(Fact):

        INSERT = 'REPLACE'

        # ...

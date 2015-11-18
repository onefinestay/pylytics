# -*- encoding: utf-8 -*-

from __future__ import unicode_literals

from contextlib import closing
import logging

from pylytics.library.warehouse import Warehouse
from test.dummy_project import Product, Sales, Stock, Store, PRODUCTS


log = logging.getLogger("pylytics")


class TestFactTableCreation(object):
    """ Test that the fact table can be created, if the corresponding dimension
    tables are partially, fully, or not created.
    """

    def test_can_create_fact_if_no_dimensions_exist(self, empty_warehouse):
        Warehouse.use(empty_warehouse)
        Sales.build()
        assert Sales.table_exists
        assert Product.table_exists
        assert Store.table_exists

    def test_can_create_fact_if_some_dimensions_exist(self, empty_warehouse):
        Warehouse.use(empty_warehouse)
        Product.create_table()
        Sales.build()
        assert Sales.table_exists
        assert Store.table_exists
        assert Product.table_exists

    def test_can_create_fact_if_all_dimensions_exist(self, empty_warehouse):
        Warehouse.use(empty_warehouse)
        Product.create_table()
        Store.create_table()
        Sales.build()
        assert Product.table_exists
        assert Store.table_exists
        assert Sales.table_exists


class TestFactRowInsertion(object):

    def _fetch_stock(self):
        connection = Warehouse.get()
        with closing(connection.cursor(dictionary=True)) as cursor:
            cursor.execute('SELECT * FROM %s' % Stock.__tablename__)
            rows = cursor.fetchall()
        return rows

    def test_can_insert_fact_record(self, empty_warehouse):
        Warehouse.use(empty_warehouse)
        Stock.build()
        Product.update()

        # Insert a fact row.
        fact_1 = Stock()
        fact_1.product = PRODUCTS[0]['product_id']
        fact_1.quantity = 5
        Stock.insert(fact_1)

        # Fetch that row back and make sure it's ok.
        rows = self._fetch_stock()
        assert len(rows) == 1
        row = rows[0]
        assert row["quantity"] == 5


class TestTriggers(object):
    """ Triggers are created to get around limitations of multiple timestamp
    values in MySQL. Make sure they can be created.
    """

    def test_create_trigger(self, empty_warehouse):
        """ Make sure a trigger can be created.
        """
        Warehouse.use(empty_warehouse)
        Sales.build()
        assert Sales.trigger_name in Warehouse.trigger_names

    def test_create_duplicate_trigger(self, empty_warehouse):
        """ Make sure a trigger isn't created if one already exists.
        """
        Warehouse.use(empty_warehouse)
        Sales.build()
        assert not Sales.create_trigger()

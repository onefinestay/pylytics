# -*- encoding: utf-8 -*-

from __future__ import unicode_literals

from contextlib import closing
import logging
import mock

from pylytics.library.column import DimensionKey
from pylytics.library.dimension import Dimension
from pylytics.library.fact import Fact
from pylytics.library.source import CallableSource
from pylytics.library.warehouse import Warehouse
from test.dummy_project import (Product, Sales, Stock, StockReplace, Store,
                                PRODUCTS, StockWithCode)


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


class TestColumnTypes(object):

    def test_degenerate_dimension(self, empty_warehouse):
        Warehouse.use(empty_warehouse)
        StockWithCode.build()


class TestFactRowInsertion(object):

    def _fetch_rows(self, fact_class):
        connection = Warehouse.get()
        with closing(connection.cursor(dictionary=True)) as cursor:
            cursor.execute('SELECT * FROM %s' % fact_class.__tablename__)
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
        rows = self._fetch_rows(Stock)
        assert len(rows) == 1
        row = rows[0]
        assert row["quantity"] == 5

    def test_duplicate_rows(self, empty_warehouse):
        """ An identical row, with the same dimension values, shouldn't get
        duplicated when inserted twice.
        """
        Warehouse.use(empty_warehouse)
        Stock.build()
        Product.update()

        # Create a fact row.
        fact_1 = Stock()
        fact_1.product = PRODUCTS[0]['product_id']
        fact_1.quantity = 5

        # Insert the fact twice
        Stock.insert(fact_1)
        Stock.insert(fact_1)

        # Also try inserting the same fact, with the same dimension values but
        # different metric values.
        fact_1.quantity = 4
        Stock.insert(fact_1)

        # Fetch that row back and make sure it's ok.
        rows = self._fetch_rows(Stock)
        assert len(rows) == 1
        row = rows[0]
        assert row["quantity"] == 5

    def test_replace_into(self, empty_warehouse):
        """ Test inserting two facts with the same dimensions, but with
        REPLACE_INTO as the insert method.
        """
        Warehouse.use(empty_warehouse)
        StockReplace.build()
        Product.update()

        # Create a fact row.
        fact_1 = StockReplace()
        fact_1.product = PRODUCTS[0]['product_id']
        fact_1.quantity = 5

        # Insert the fact
        StockReplace.insert(fact_1)

        # Insert the same fact, with the same dimension values but
        # different metric values.
        fact_1.quantity = 4
        StockReplace.insert(fact_1)

        # Fetch that row back and make sure it's ok.
        rows = self._fetch_rows(StockReplace)
        assert len(rows) == 1
        row = rows[0]
        assert row["quantity"] == 4

    def test_degenerate_dimension_uniqueness(self, empty_warehouse):
        """ Make sure that facts which contain degenerate dimensions behave the
        same (i.e. no duplicates inserted).
        """
        Warehouse.use(empty_warehouse)
        StockWithCode.build()
        Product.update()

        # Create a fact row.
        fact_1 = StockWithCode()
        fact_1.product = PRODUCTS[0]['product_id']
        fact_1.quantity = 5
        fact_1.stock_code = 'abcdefg'

        # Insert the fact twice
        StockWithCode.insert(fact_1)
        StockWithCode.insert(fact_1)

        # Also try inserting the same fact, with the same dimension values but
        # different metric values.
        fact_1.quantity = 4
        Stock.insert(fact_1)

        # Fetch that row back and make sure it's ok.
        rows = self._fetch_rows(StockWithCode)
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


class TestHistoricalSource(object):
    """ Make sure the correct sources are being called during a historical
    update.
    """

    def test_historical_source_called(self, empty_warehouse):
        callable_mock = mock.Mock(side_effect=lambda: [])

        class Dummy(Fact):
            __historical_source__ = CallableSource.define(
                _callable=staticmethod(callable_mock)
            )

        Dummy.historical()
        assert callable_mock.called

    def test_update_source_called(self, empty_warehouse):
        callable_mock = mock.Mock(side_effect=lambda: [])

        class Dummy(Fact):
            __source__ = CallableSource.define(
                _callable=staticmethod(callable_mock)
            )

        Dummy.update()
        assert callable_mock.called

    def test_dimension_source(self, empty_warehouse):
        """ When `historical` is called on a fact, then
        `__historical_source__` is triggered on the fact and corresponding
        dimensions if available, otherwise `__source__`.
        """
        mock_1 = mock.Mock(side_effect=lambda: [])
        mock_2 = mock.Mock(side_effect=lambda: [])
        mock_3 = mock.Mock(side_effect=lambda: [])

        class FirstDimension(Dimension):
            __historical_source__ = CallableSource.define(
                _callable=staticmethod(mock_1)
            )
            __source__ = CallableSource.define(
                _callable=staticmethod(mock_2)
            )

        class SecondDimension(Dimension):
            __source__ = CallableSource.define(
                _callable=staticmethod(mock_3)
            )

        class Dummy(Fact):
            __source__ = CallableSource.define(
                _callable=staticmethod(lambda: [])
            )
            first_dimension = DimensionKey('first_dimension', FirstDimension)
            second_dimension = DimensionKey('second_dimension', SecondDimension)

        Dummy.historical()
        assert mock_1.called
        assert mock_3.called

        Dummy.update()
        assert mock_2.called
        assert mock_3.called

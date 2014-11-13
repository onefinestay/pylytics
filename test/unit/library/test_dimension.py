from contextlib import closing

import pytest

from pylytics.library.dimension import Dimension
from pylytics.library.column import Column, NaturalKey
from pylytics.library.warehouse import Warehouse
from test.dummy_project import Store


@pytest.fixture
def store():
    obj = Store()
    obj['store_id'] = 1
    obj['manager'] = 'Mr Smith'
    return obj


@pytest.fixture
def modified_store():
    obj = Store()
    obj['store_id'] = 1
    obj['manager'] = 'Mrs Smith'
    return obj


@pytest.fixture
def null_store():
    obj = Store()
    obj['store_id'] = 1
    obj['manager'] = None
    return obj


class TestEvolvingDimensions(object):

    def _fetch_store_row_count(self):
        connection = Warehouse.get()
        with closing(connection.cursor()) as cursor:
            cursor.execute('SELECT COUNT(*) FROM %s' % Store.__tablename__)
            rows = cursor.fetchall()
        return rows[0][0]

    def test_single_insert(self, empty_warehouse, store):
        """ Insert a single Dimension instance. There shoudn't be any problems.
        """
        Warehouse.use(empty_warehouse)
        Store.build()
        Store.insert(store)
        assert self._fetch_store_row_count() == 1

    def test_duplicate_insert(self, empty_warehouse, store, modified_store):
        """ Insert a Dimension twice. The row shouldn't be duplicated.
        """
        Warehouse.use(empty_warehouse)
        Store.build()
        Store.insert(store)
        Store.insert(store)
        assert self._fetch_store_row_count() == 1

    def test_modified_insert(self, empty_warehouse, store, modified_store):
        """ Insert a Dimension, followed by a modified version.
        """
        Warehouse.use(empty_warehouse)
        Store.build()
        Store.insert(store)
        Store.insert(modified_store)
        assert self._fetch_store_row_count() == 2

    def test_subquery(self):
        """
        Test Store.__subquery__()
        """
        pass

    def test_null_values(self, empty_warehouse, null_store):
        """
        Test that inserting NULL values into Columns doesn't break the unique
        keys i.e. if we keep on inserting the same row, where one column is
        NULL, it won't keep on making copies of that row.
        """
        Warehouse.use(empty_warehouse)
        Store.build()
        Store.insert(null_store)
        Store.insert(null_store)
        assert self._fetch_store_row_count() == 1

    def test_null_values(self, empty_warehouse, store, null_store):
        """
        If the second instance contains a NULL column, then we expect a new
        row to be created.
        """
        Warehouse.use(empty_warehouse)
        Store.build()
        Store.insert(store)
        Store.insert(null_store)
        assert self._fetch_store_row_count() == 2

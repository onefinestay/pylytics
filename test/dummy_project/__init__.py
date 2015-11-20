"""
Facts and dimensions shared across all tests.

Pytest doesn't support class fixtures, which is why they're done this way,
and not in conftest.

"""

from pylytics.library.column import (Column, DimensionKey, Metric, NaturalKey,
                                     DegenerateDimension)
from pylytics.library.dimension import Dimension
from pylytics.library.fact import Fact
from pylytics.library.source import CallableSource


STORES = [
    {'store_id': 1, 'manager': 'Mrs Smith'},
    {'store_id': 2, 'manager': 'Dr Pepper'}
]

PRODUCTS = [
    {'product_id': 1, 'manager': 'Jeans'},
    {'product_id': 2, 'manager': 'Cheese'}
]


class Store(Dimension):

    __source__ = CallableSource.define(
        _callable=staticmethod(lambda: STORES)
    )

    store_id = NaturalKey('store_id', int, size=10)
    manager = Column('manager', str, size=100)


class Product(Dimension):

    __source__ = CallableSource.define(
        _callable=staticmethod(lambda: PRODUCTS)
    )

    product_id = NaturalKey('product_id', int, size=10)
    product_name = Column('product_name', str, size=100)


class Sales(Fact):

    __source__ = NotImplemented

    product = DimensionKey('product', Product)
    store = DimensionKey('store', Store)


class Stock(Fact):

    __source__ = NotImplemented

    product = DimensionKey('product', Product)
    quantity = Metric('quantity', int)


class StockWithCode(Fact):

    __source__ = NotImplemented

    product = DimensionKey('product', Product)
    quantity = Metric('quantity', int)
    stock_code = DegenerateDimension('stock_code', basestring)

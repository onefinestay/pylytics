"""
Facts and dimensions shared across all tests.

Pytest doesn't support class fixtures, which is why they're done this way,
and not in conftest.

"""

from pylytics.library.column import Column, DimensionKey, NaturalKey
from pylytics.library.dimension import Dimension
from pylytics.library.fact import Fact


class Store(Dimension):
    __source__ = NotImplemented

    store_id = NaturalKey('store_id', int, size=10)
    manager = Column('manager', str, size=100)


class Product(Dimension):
    __source__ = NotImplemented

    product_id = NaturalKey('product_id', int, size=10)
    product_name = Column('product_name', str, size=100)


class Sales(Fact):
    __source__ = NotImplemented

    product = DimensionKey('product', Product)
    store = DimensionKey('store', Store)

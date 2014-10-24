"""
Testing huge inserts.

"""

import os
import pickle

from pylytics.declarative.column import Column, DimensionKey, NaturalKey
from pylytics.declarative.dimension import Dimension
from pylytics.declarative.fact import Fact
from pylytics.declarative.warehouse import Warehouse


# The fact table generated is MAX_ITERATIONS ^ 2.
MAX_ITERATIONS = 1001


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


def _generate_store():
    instances = []
    for store_id in xrange(1, MAX_ITERATIONS):
        obj = Store()
        obj['store_id'] = store_id
        obj['manager'] = 'Mr Smith %i' % store_id
        instances.append(obj)
    return instances


def _generate_product():
    instances = []
    for product_id in xrange(1, MAX_ITERATIONS):
        obj = Product()
        obj['product_id'] = product_id
        obj['product_name'] = 'Essential Product %i' % product_id
        instances.append(obj)
    return instances


def _generate_sales():
    instances = []
    for store_id in xrange(1, MAX_ITERATIONS):
        for product_id in xrange(1, MAX_ITERATIONS):
            obj = Sales()
            obj['product'] = product_id
            obj['store'] = store_id
            instances.append(obj)
    return instances


def _get_instances(class_name):
    current_directory = os.path.dirname(__file__)
    file_name = os.path.join(current_directory, '%s.pickled' % class_name)

    if os.path.exists(file_name):
        print '%s already exists.' % file_name
        with open(file_name, 'r') as f:
            return pickle.load(f)

    with open(file_name, 'w') as pickle_file:
        instances = globals()['_generate_%s' % class_name]()
        pickle.dump(instances, pickle_file)

    return instances


def test_insert(empty_warehouse):
    """
    Inserts a fact with a million rows.
    """
    Warehouse.use(empty_warehouse)

    Store.build()
    stores = _get_instances('store')
    Store.insert(*stores)
    
    Product.build()
    products = _get_instances('product')
    Product.insert(*products)

    Sales.build()
    sales = _get_instances('sales')
    Sales.insert(*sales)

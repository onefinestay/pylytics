"""
Testing huge inserts.

"""

import os
import pickle

from pylytics.declarative.column import NaturalKey
from pylytics.declarative.dimension import Dimension
from pylytics.declarative.fact import Fact
from pylytics.declarative.warehouse import Warehouse


class Store(Dimension):
    __source__ = NotImplemented

    store_id = NaturalKey('store_id', int, size=10)
    manager = Column('manager', str, size=100)
    

class Product(Dimension):
    __source__ = NotImplemented

    product_id = NaturalKey('store_id', int, size=10)
    product_name = Column('product_name', str, size=100)


class Sales(Fact):
    __source__ = NotImplemented
    
    product = DimensionKey('product', Product)
    store = DimensionKey('store', Store)


def _generate_store():
    instances = []
    for store_id in xrange(1, 1000):
        instances.append(
            Store(store_id=store_id, manager='Mr Smith %i' % store_id)
        )
    return instances


def _generate_product():
    instances = []
    for product_id in xrange(1, 1000):
        instances.append(
            Store(store_id=store_id,
                  product_name='Essential Product %i' % product_id)
        )
    return instances


def _generate_sales():
    instances = []
    for store_id in xrange(1, 1000):
        for product_id in xrange(1, 1000):
            instances.append(
                Sales(store=store_id, product=product_id)
            )
    return instances


def _get_instances(class_name):
    current_directory = os.dirname(__file__)
    file_name = os.path.join(current_directory, '%s.pickled' % class_name)

    if os.path.exists(file_name):
        print '%s already exists.' % store_file
        return pickle.load(file_name)

    with open(store_file, 'w') as pickle_file:
        instances = getattr(globals(), '_generate_%s' % class_name)()
        pickle.dump(instances, pickle_file)

    return instances


def test_insert(empty_warehouse):
    """
    Inserts a fact with a million rows.
    """
    Warehouse.use(empty_warehouse)

    Store.build()
    Store.insert(_get_instances(store))
    
    Product.build()
    Product.insert(_get_instances(product))

    Sales.build()
    Sales.insert(_get_instances(sales))

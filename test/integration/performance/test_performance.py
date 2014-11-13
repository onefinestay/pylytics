"""
Testing huge inserts.

"""

import logging
import os
import pickle
import datetime

import pytest
from mysql.connector.errors import OperationalError

from pylytics.library.warehouse import Warehouse
from pylytics.library.main import enable_logging
from test.dummy_project import Product, Sales, Store


# The fact table generated is MAX_ITERATIONS ^ 2.
MAX_ITERATIONS = 1001

log = logging.getLogger("pylytics")


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
    Inserts a fact with MAX_ITERATIONS ^ 2 rows.
    """
    enable_logging()

    Warehouse.use(empty_warehouse)

    Store.build()
    stores = _get_instances('store')
    Store.insert(*stores)

    Product.build()
    products = _get_instances('product')
    Product.insert(*products)

    Sales.build()
    sales = _get_instances('sales')

    start_time = datetime.datetime.now()
    print 'Starting bulk insert of fact at ', start_time

    try:
        Sales.insert(*sales)
    except OperationalError:
        pytest.fail('The connection broke.')

    end_time = datetime.datetime.now()
    print 'Ending bulk insert of fact at ', end_time

    delta = end_time - start_time
    print 'Time taken = ', delta

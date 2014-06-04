from __future__ import absolute_import

import eventlet
eventlet.monkey_patch()

try:
    import nameko
except ImportError:
    raise RuntimeError(
        'nameko is required to use the functionality in '
        'pylitics.plugins.nameko, please install it using '
        '`pip install nameko`')

from collections import namedtuple
import json
import logging

from eventlet.queue import Queue
from nameko.dependencies import injection, InjectionProvider, DependencyFactory
from nameko.messaging import AMQP_URI_CONFIG_KEY
from nameko.rpc import rpc
from nameko.runners import ServiceRunner

from pylytics.library.connection import DB
import settings

_log = logging.getLogger(__name__)

SQLQuery = namedtuple('SQLQuery', ['query', 'values', 'many', 'get_cols'])

SHOULD_STOP = object()  # marker for gracefully stopping the DB connector


class DBConnector(InjectionProvider):
    """ Provides a centralised queue for executing sql statements over a single
    connection (as mysqldb is not compatible with eventlet).

    """

    def __init__(self):
        self.database = None
        self.gt = None
        self.query_queue = Queue()

    def prepare(self):
        self.database = DB(settings.pylytics_db)
        self.database.connect()

    def start(self):
        self.gt = self.container.spawn_managed_thread(self._run)

    def stop(self):
        self.query_queue.put(SHOULD_STOP)
        self.gt.wait()
        self.database.close()

    def kill(self):
        self.gt.kill()
        self.database.close()

    def _execute_query(self, query):
        self.database.execute(
            query=query.query,
            values=query.values,
            many=query.many,
            get_cols=query.get_cols,
        )
        self.database.commit()

    def _run(self):
        """ Processes the query queue until told to stop.

        This should not be called directly, rather the `start()` method
        should be used.
        """
        while True:
            query = self.query_queue.get()  # blocks until an item is available
            if query is SHOULD_STOP:
                break

            self._execute_query(query)
            self.query_queue.task_done()

    def acquire_injection(self, worker_ctx):
        return self.query_queue


@injection
def db_connector():
    """ Returns the execution queue for placing SQLQuery objects on
    """
    return DependencyFactory(DBConnector)


class FactCollector(DBConnector):

    def enqueue(self, fact_table, values):
        """ Stash the fact data represented by the data mapped in `values`.

        Serialises `values` into a JSON structure for processing into the
        target `fact_tables` by another utility.
        """
        query = SQLQuery(
            query='INSERT INTO stash_table (fact_table, value_map) '
                  'VALUES (%s, %s)',
            values=(fact_table, json.dumps(values)),
            many=False,
            get_cols=False,
        )
        self.query_queue.put(query)

    def acquire_injection(self, worker_ctx):
        return self.enqueue


@injection
def fact_collector():
    return DependencyFactory(FactCollector)


class NamekoCollectionService(object):

    name = 'pylytics'

    stash = fact_collector()

    @rpc
    def save(self, fact_table, **values):
        """ Stashes the information provided in `values` for later processing
        into the target fact table.

        Usage::

            pylitics.save(
                fact_table='fact_something_interesting',
                dimension='foo',
                another_dimension='bar',
            )

        :Returns:
            Nothing

        """
        self.stash(fact_table, values)


def run_collector():

    amqp_uri = getattr(settings, 'NAMEKO_AMQP_URI', None)

    if not amqp_uri:
        raise RuntimeError(
            'NAMEKO_AMQP_URI must be configured in order to run this collector'
        )

    config = {
        AMQP_URI_CONFIG_KEY: amqp_uri
    }

    service_runner = ServiceRunner(config)

    service_runner.add_service(NamekoCollectionService)

    service_runner.start()
    service_runner.wait()


if __name__ == '__main__':
    run_collector()

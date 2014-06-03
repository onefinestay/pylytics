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

import eventlet
from eventlet.event import Event
from nameko.dependencies import injection, InjectionProvider, DependencyFactory
from nameko.messaging import AMQP_URI_CONFIG_KEY
from nameko.rpc import rpc
from nameko.runners import ServiceRunner

from pylytics.library.connection import DB
import settings

_log = logging.getLogger(__name__)

SQLQuery = namedtuple('SQLQuery', ['query', 'values', 'many', 'get_cols'])


class DBConnector(InjectionProvider):

    def __init__(self, database_key):
        self.database_key = database_key
        self.database = None
        self.should_stop = Event()
        self.gt = None
        self.query_queue = []

    def prepare(self):
        self.database = DB(self.database_key)
        self.database.connect()

    def start(self):
        self.gt = self.container.spawn_managed_thread(self._run)

    def stop(self):
        self.should_stop.send(True)
        self.gt.wait()
        self.database.close()

    def kill(self):
        self.gt.kill()
        self.database.close()

    def _process_queue(self):
        if not self.query_queue:
            return

        query = self.query_queue.pop(0)
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
        while not self.should_stop.ready():
            self._process_queue()
            eventlet.sleep()

    def acquire_injection(self, worker_ctx):
        return self.database


@injection
def db_connector(database_key):
    return DependencyFactory(DBConnector, database_key)


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
        self.query_queue.append(query)

    def acquire_injection(self, worker_ctx):
        return self.enqueue


@injection
def fact_collector(database_key):
    return DependencyFactory(FactCollector, database_key)


class NamekoCollectionService(object):

    name = 'pylytics'

    stash = fact_collector('example')

    @rpc
    def save(self, fact_table, **values):
        """ Stashes the information provided in `values` for later processing
        into the target fact table.
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

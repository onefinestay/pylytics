from __future__ import absolute_import
from functools import partial

import eventlet
from nameko.events import event_handler
from pylytics.declarative import Staging, Warehouse

eventlet.monkey_patch()

try:
    import nameko
except ImportError:
    raise RuntimeError(
        'nameko is required to use the functionality in '
        'pylitics.plugins.nameko, please install it using '
        '`pip install nameko`')

from collections import namedtuple
from datetime import datetime
import json
import logging

from eventlet.queue import Queue
from MySQLdb import ProgrammingError
from nameko.dependencies import injection, InjectionProvider, DependencyFactory
from nameko.messaging import AMQP_URI_CONFIG_KEY
from nameko.rpc import rpc
from nameko.runners import ServiceRunner

from pylytics.library.connection import DB
from pylytics.library.settings import settings


_log = logging.getLogger(__name__)

SQLQuery = namedtuple('SQLQuery', ['query', 'values', 'many', 'get_cols'])

SHOULD_STOP = object()  # marker for gracefully stopping the DB connector

COLLECTOR_TYPE = 'nameko'


class FactCollector(InjectionProvider):
    """ Provides a centralised queue for stashing staging data as a
    compatibility layer between the nameko service runner and the mysql driver
    (which isn't greenthread-safe).

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
        try:
            self.database.execute(
                query=query.query,
                values=query.values,
                many=query.many,
                get_cols=query.get_cols,
            )
            self.database.commit()
        except ProgrammingError as exc:
            _log.error(
                'Unable to stash fact data {} MySQL exitied with error: '
                '"{}"'.format(query, exc))

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

    def enqueue(self, fact_table, values):
        """ Stash the fact data represented by the data mapped in `values`.

        Serialises `values` into a JSON structure for processing into the
        target `fact_tables` by another utility.
        """
        now = datetime.utcnow()
        query = SQLQuery(
            query='INSERT INTO staging '
                  '(collector_type, fact_table, value_map, created) '
                  'VALUES (%s, %s, %s, %s)',
            values=(COLLECTOR_TYPE, fact_table, json.dumps(values), now),
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

    # TODO: dynamically create these event handler functions

    @event_handler("booking", "booking_appointment_created")
    def collect_booking_appointment_created(self, event_data):
        record = Staging("booking.booking_appointment_created", event_data)
        Staging.insert(record)

    @event_handler("booking", "booking_appointment_changed")
    def collect_booking_appointment_changed(self, event_data):
        record = Staging("booking.booking_appointment_changed", event_data)
        Staging.insert(record)

    @event_handler("activity", "activity_task_completed")
    def collect_activity_task_completed(self, event_data):
        record = Staging("activity.activity_task_completed", event_data)
        Staging.insert(record)


def run_collector():

    amqp_uri = getattr(settings, 'NAMEKO_AMQP_URI', None)

    if not amqp_uri:
        raise RuntimeError(
            'NAMEKO_AMQP_URI must be configured in order to run this collector'
        )

    config = {
        AMQP_URI_CONFIG_KEY: amqp_uri
    }

    # Commented code shows attempt at dynamic event handler creation.
    # Probably needs tidier contract with Nameko for this.
    #
    # # TODO: get from list of active facts, don't hard code
    # collectible_events = {
    #     "booking": [
    #         "booking_appointment_created",
    #         "booking_appointment_changed",
    #     ],
    #     "activity": [
    #         "activity_task_completed",
    #     ],
    # }
    #
    # for service, events in collectible_events.items():
    #     for event in events:
    #
    #         def create_handler(service_name, event_name):
    #             full_event_name = ".".join((service_name, event_name))
    #
    #             @event_handler(service_name, event_name)
    #             def collect(self, event_data):
    #                 """ Insert the incoming event details directly
    #                 into the staging table.
    #                 """
    #                 Staging.insert(Staging(full_event_name, event_data))
    #
    #             return collect
    #
    #         handler_name = "_".join(("collect", service, event))
    #         handler = create_handler(service, event)
    #         setattr(NamekoCollectionService, handler_name, handler)

    service_runner = ServiceRunner(config)

    service_runner.add_service(NamekoCollectionService)

    service_runner.start()
    service_runner.wait()


if __name__ == '__main__':
    run_collector()

from mock import ANY, call, patch
import pytest

import eventlet
from nameko.runners import ServiceRunner
from nameko.testing.utils import get_container
from nameko.testing.services import entrypoint_hook

from pylytics.plugins.nameko import NamekoCollectionService

import settings


@pytest.yield_fixture
def patched_db():
    with patch('pylytics.plugins.nameko.DB') as DB:
        yield DB


@pytest.yield_fixture
def service_container(patched_db):
    config = {'AMQP_URI': settings.NAMEKO_AMQP_URI}
    runner = ServiceRunner(config)
    runner.add_service(NamekoCollectionService)
    runner.start()

    container = get_container(runner, NamekoCollectionService)
    yield container

    runner.stop()


def test_db_connection(patched_db, service_container):

    assert patched_db.called
    database = patched_db.return_value

    assert database.connect.called

    with entrypoint_hook(service_container, 'save') as save:

        save('fact_foo', foo='bar')

        with eventlet.Timeout(5):
            while database.execute.called is False:
                pass

        expected_call = call(
            query=ANY,
            values=('fact_foo', '{"foo": "bar"}'),
            many=False,
            get_cols=False,
        )

        assert database.execute.call_args == expected_call

from mock import ANY, call, patch
import pytest

import eventlet
from nameko.runners import ServiceRunner
from nameko.testing.utils import get_container
from nameko.testing.services import entrypoint_hook


# TODO: fix this properly with overhauled settings
try:
    from pylytics.plugins.nameko import NamekoCollectionService
    import settings
except ImportError:
    has_settings = False
else:
    has_settings = True


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


@pytest.mark.skipif(not has_settings, reason="requires settings")
def test_db_connection(patched_db, service_container):

    assert patched_db.called
    database = patched_db.return_value

    assert database.connect.called

    with entrypoint_hook(service_container, 'save') as save:

        save('fact_foo', foo='bar')

        with eventlet.Timeout(5):
            while not database.execute.called:
                pass

        expected_call = call(
            query=ANY,
            values=('fact_foo', '{"foo": "bar"}'),
            many=False,
            get_cols=False,
        )

        assert database.execute.call_args == expected_call


@pytest.mark.skipif(not has_settings, reason="requires settings")
def test_multiple_requests(patched_db, service_container):

    assert patched_db.called
    database = patched_db.return_value

    assert database.connect.called

    with entrypoint_hook(service_container, 'save') as save:

        for i in xrange(10):
            save('fact_foo', foo=i)

        with eventlet.Timeout(5):
            while database.execute.call_count < 10:
                pass

        assert database.execute.call_count == 10

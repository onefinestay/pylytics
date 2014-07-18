from mock import ANY, call, patch
import pytest

import eventlet
from nameko.runners import ServiceRunner
from nameko.testing.utils import get_container
from nameko.testing.services import entrypoint_hook
from pylytics.declarative import Warehouse, Staging

from pylytics.library.settings import settings
from pylytics.plugins.nameko import NamekoCollectionService


@pytest.yield_fixture
def patched_db():
    with patch('pylytics.plugins.nameko.DB') as DB:
        Warehouse.use(DB)
        yield DB


@pytest.yield_fixture
def mock_staging_insert():
    with patch.object(Staging, "insert") as mocked:
        yield mocked


@pytest.yield_fixture
def service_container(patched_db):
    config = {'AMQP_URI': settings.NAMEKO_AMQP_URI}
    runner = ServiceRunner(config)
    runner.add_service(NamekoCollectionService)
    runner.start()

    container = get_container(runner, NamekoCollectionService)
    yield container

    runner.stop()


@pytest.mark.usefixtures("patched_db")
def test_collect(service_container, mock_staging_insert):
    handler_name = 'collect_booking_appointment_created'
    event_name = 'booking.booking_appointment_created'
    event_data = {"booking_ref": "ABCD", "appointment_id": 123}

    with entrypoint_hook(service_container, handler_name) as collect:

        collect(event_data)

        expected = Staging(event_name, event_data)
        mock_staging_insert.assert_called_once_with(expected)


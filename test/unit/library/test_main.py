from datetime import time, timedelta

from mock import Mock, patch
import pytest
from pytz import UTC

from pylytics.library.main import find_scheduled
from pylytics.library.schedule import Schedule

from test.dummy_project import Product, Sales, Store


# def test_valid_time_range():
#     start_time = time(hour=0)
#     end_time = time(hour=23, minute=59)
#     delta = timedelta(minutes=30)
# 
#     values = []
#     for i in valid_time_range(start_time, end_time, delta):
#         values.append(i)
# 
#     # Test a range of values.
#     assert start_time in values
#     assert time(hour=0, minute=30) in values
#     assert time(hour=6) in values
#     assert time(hour=23, minute=30) in values
#     assert len(values) == 48


class TestFindScheduled(object):

    @patch('pylytics.library.schedule.get_now')
    def test_success(self, get_now):
        get_now.return_value = time(hour=1, tzinfo=UTC)

        SalesMock = Mock(Sales)
        SalesMock = Mock(Sales)
        SalesMock.__schedule__ = Schedule(repeats=timedelta(hours=1))

        scheduled_facts = find_scheduled([SalesMock])
        assert SalesMock in scheduled_facts

from datetime import time, timedelta

from mock import Mock, patch
import pytest
from pytz import UTC

from pylytics.library.main import find_scheduled, Commander
from pylytics.library.fact import Fact
from pylytics.library.schedule import Schedule

from test.dummy_project import Sales


@pytest.mark.parametrize(('schedule', 'should_run'), [
    (
        Schedule(repeats=timedelta(hours=1)),
        True
    ),
    (
        Schedule(repeats=timedelta(minutes=10)),
        True
    ),
    (
        Schedule(starts=time(hour=2, tzinfo=UTC)),
        False
    ),
    (
        Schedule(repeats=timedelta(hours=2)),
        False
    ),
    (
        Schedule(),  # Defaults to just running at midnight.
        False
    ),
])
@patch('pylytics.library.schedule.get_now')
def test_find_scheduled(get_now, schedule, should_run):
    get_now.return_value = time(hour=1, tzinfo=UTC)

    SalesMock = Mock(Sales)
    SalesMock.__schedule__ = schedule

    scheduled_facts = find_scheduled([SalesMock])
    present = SalesMock in scheduled_facts
    assert present == should_run


@patch('pylytics.library.main.get_all_fact_classes')
def test_isolation(get_all_fact_classes):
    """ Make sure that subsequent facts get called, even if the first
    fact fails.
    """

    class FirstFact(Fact):
        def update(self):
            raise Exception

    class SecondFact(Fact):
        update = Mock()

    get_all_fact_classes.return_value = [FirstFact, SecondFact]
    commander = Commander()
    commander.run('update', 'FirstFact', 'SecondFact')
    assert SecondTest.update.called

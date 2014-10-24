from datetime import time, timedelta

from pylytics.declarative.main import valid_time_range


def test_valid_time_range():
    start_time = time(hour=0)
    end_time = time(hour=23, minute=59)
    delta = timedelta(minutes=30)

    values = []
    for i in valid_time_range(start_time, end_time, delta):
        values.append(i)

    # Test a range of values.
    assert start_time in values
    assert time(hour=0, minute=30) in values
    assert time(hour=6) in values
    assert time(hour=23, minute=30) in values
    assert len(values) == 48

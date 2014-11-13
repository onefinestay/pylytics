from pylytics.library.schedule import get_now


def test_get_now():
    now = get_now()
    assert now.minute in range(0, 60, 10)
    assert now.second == 0
    assert now.microsecond == 0

import datetime

from mock import ANY, Mock, patch
import pytest


class TestGetUtcOffsets(object):

    # Patch the global settings module that pylytics.library.connection
    # requires.
    @patch.dict('sys.modules', {'settings': Mock()})
    def test_success(self):
        from pylytics.builtins.dim import dim_timezone

        utc_offsets = dim_timezone.get_utc_offsets()

        # Make sure the response format is as we expect.
        assert (u'GB', u'Europe/London', ANY, ANY, ANY) in utc_offsets

        # Test the ordering.
        utc_names = [utc_offset[0:2] for utc_offset in utc_offsets]
        first_index = utc_names.index((u'CI', u'Africa/Abidjan'))
        second_index = utc_names.index((u'GH', u'Africa/Accra'))
        assert first_index < second_index


class TestGetCurrentTimezoneAtMidnight(object):

    @patch.dict('sys.modules', {'settings': Mock()})
    @pytest.mark.parametrize(
        ('dt', 'timezone'), ((datetime.datetime(2014, 1, 1, 15, 0), 15.0),
                             (datetime.datetime(2014, 1, 1, 15, 15), 15.25),
                             (datetime.datetime(2014, 1, 1, 15, 30), 15.5),
                             (datetime.datetime(2014, 1, 1, 15, 45), 15.75),
                             (datetime.datetime(2014, 1, 1, 16, 0), 16.0))
        )
    def test_success(self, dt, timezone):
        from pylytics.builtins.dim import dim_timezone

        dim_timezone.get_utcnow = Mock(return_value=dt)

        assert dim_timezone.get_current_timezone_at_midnight() == timezone

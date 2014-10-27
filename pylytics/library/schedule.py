from datetime import time, timedelta

from pytz import UTC


class Schedule(object):
    """
    Used for scheduling when facts will update.

    The assumption here is that facts will update at least once a day.

    """

    def __init__(self, starts=time(hour=0), ends=time(hour=23, minute=59),
                 repeats=None, timezone=UTC):
        """
        Args:
            starts:
                A time object for the earliest time the fact will update.
            ends:
                A time object for the latest time the fact will update.
            repeats:
                A timedelta object, representing when the fact will update.
                For example timedelta(minute=30) will update every 30 minutes
                between `start` and `end`. If not specified, the fact will
                just update at `starts` each day.
            timezone:
                A tzinfo object.

        """
        self.starts = starts
        self.ends = ends
        self.repeats = repeats
        self.timezone = timezone

    @property
    def starts_tzaware(self):
        return self.starts.replace(tzinfo=self.timezone)

    @property
    def ends_tzaware(self):
        return self.ends.replace(tzinfo=self.timezone)

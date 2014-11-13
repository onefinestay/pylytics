import datetime

from pytz import UTC


def get_now():
    """
    Returns a coarse grained time object, with no seconds or microseconds,
    and rounded down to the nearest 10 minutes.

    """
    now = datetime.datetime.now(tz=UTC).timetz()
    return now.replace(
        minute=(now.minute - now.minute % 10),
        second=0,
        microsecond=0,
        )


class Schedule(object):
    """
    Used for scheduling when facts will update.

    The assumption here is that facts will update at least once a day i.e.
    even if Schedule isn't specified, the fact will still run at midnight
    each day.

    """

    def __init__(self, starts=datetime.time(hour=0),
                 ends=datetime.time(hour=23, minute=59),
                 repeats=datetime.timedelta(days=1),
                 timezone=UTC):
        """
        Args:
            starts:
                A time object for the earliest time the fact will update each
                day.
            ends:
                A time object for the latest time the fact will update each
                day.
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

    @property
    def valid_time_range(self):
        """ A generator that returns the times when this fact should be run.
        """
        # We have to convert time objects to datetime so timedeltas can be
        # used.
        fake_date = datetime.date(2000, 1, 1)
        start = datetime.datetime.combine(fake_date, self.starts_tzaware)
        end = datetime.datetime.combine(fake_date, self.ends_tzaware)

        while start < end:
            yield start.timetz()
            start += self.repeats

            if start.date().day == 2:
                # Time has wrapped around.
                break

    @property
    def should_run(self):
        """ Returns True or False depending on if this fact should run or not.
        """
        now = get_now()
        starts = self.starts_tzaware
        ends = self.ends_tzaware

        if starts > now or ends < now:
            return False

        if now in self.valid_time_range:
            return True
        else:
            return False

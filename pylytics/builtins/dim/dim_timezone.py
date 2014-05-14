import datetime

from iso8601.iso8601 import UTC
from pylytics.library.dim import Dim
import pytz


def get_utc_offsets():
    """
    Returns a list of timezones, along with their offsets from UTC.
    """
    normal = datetime.datetime.now()

    utc_offsets = []

    country_timezones = pytz.country_timezones

    for country_code, timezones in country_timezones.iteritems():
        for timezone in timezones:
            timezone_obj = pytz.timezone(timezone)
            utcoffset = timezone_obj.utcoffset(normal, is_dst=True)

            utcoffset_seconds = utcoffset.seconds
            utcoffset_minutes = utcoffset_seconds / 60
            # We need to divide by a decimal, since timezones can be in quarter
            # hour increments.
            utcoffset_hours = utcoffset_minutes / 60.0

            utc_offsets.append((country_code, timezone, utcoffset_hours,
                                utcoffset_minutes, utcoffset_seconds))

    # Make the ordering more rational - order by offset, then alphabetically by
    # timezone name.
    sorted_utc_offsets = sorted(utc_offsets, key=lambda x: (x[2], x[1]))
    return sorted_utc_offsets


def get_utcnow():
    """Get a timezone aware datetime object."""
    return datetime.datetime.now(UTC)


def get_current_timezone_at_midnight():
    """
    Returns the current timezone which is at midnight UTC.

    """
    utc_datetime = get_utcnow()
    hour = utc_datetime.hour
    minute = utc_datetime.minute

    if 0 <= minute < 7.5:
        timezone = float(hour)

    elif 7.5 <= minute < 22.5:
        # There aren't any timezones which are X hours and 15 minutes ahead
        # of UTC at the moment, but it's included here to make the logic
        # clearer.
        timezone = float(hour) + 0.25

    elif 22.5 <= minute < 37.5:
        timezone = float(hour) + 0.5

    elif 37.5 <= minute < 52.5:
        timezone = float(hour) + 0.75

    elif 52.5 <= minute < 60:
        timezone = float(hour) + 1

    return timezone


class DimTimezone(Dim):
    """
    Builtin timezone dimension. See docs for more information.

    This dimensions only records the current offsets. So if for example
    Daylight Saving Time is currently active, the offsets shown will reflect
    this.

    """

    def update(self):
        """
        Overriding the base update to populate timezone data programatically
        rather than through a SQL query.
        """
        utc_offsets = get_utc_offsets()

        query = """
            INSERT INTO `{table_name}`
                (country_code, timezone, utc_offset_in_hours,
                utc_offset_in_minutes, utc_offset_in_seconds)
                VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE timezone = VALUES(timezone)""".format(
                table_name=self.table_name)

        self.connection.execute(query, many=True, values=utc_offsets)

    def get_dictionary(self, field_name):
        """
        Overriding the base get_dictionary to only return a subset of the
        dimension rows (the timezones which are currently at midnight).
        """
        current_timezone_at_midnight = get_current_timezone_at_midnight()

        query = """
            SELECT {field_name}, id
            FROM {table_name}
            WHERE utc_offset_in_hours = {utc_offset}
            ORDER BY id asc;
            """.format(field_name=field_name, table_name=self.table_name,
                       utc_offset=current_timezone_at_midnight)

        data = self.connection.execute(query)

        # Map indexes to values.
        data_dict = dict(data)

        return data_dict

import datetime
import pprint

#from pylytics.library.dim import Dim
import pytz


def create_utc_offsets():
    # TODO is this right????
    normal = datetime.datetime.now()

    utc_offsets = []

    country_timezones = pytz.country_timezones

    # TODO Want each city, with the country next to it.
    for country_code, timezones in country_timezones.iteritems():
        for timezone in timezones:
            timezone_obj = pytz.timezone(timezone)
            utcoffset = timezone_obj.utcoffset(normal, is_dst=True)
            # TODO Want this as mins, seconds, or hours??? hours ... but may as well have them all ...
            utcoffset_seconds = utcoffset.seconds
            utcoffset_minutes = utcoffset_seconds / 60
            utcoffset_hours = utcoffset_minutes / 60
        
            # TODO need to be careful - we don't want people to think the
            # offset is the sum of all these ...
            utc_offsets.append((country_code, timezone, utcoffset_hours, utcoffset_minutes, utcoffset_seconds))
    
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(utc_offsets)


class DimTimezone():
    """Builtin timezone dimension. See docs for more information."""
    # TODO Overwrite getting data from SQL - generate it programatically.
    
    def update(self):
        create_utc_offsets()


if __name__ == '__main__':
    create_utc_offsets()

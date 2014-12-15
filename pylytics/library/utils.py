from datetime import date, datetime, time, timedelta
import re
import string


# MySQL minimum timestamps are slightly above the Unix epoch.
EPOCH = datetime(1970, 1, 1, 1, 1)


_camel_words = re.compile(r"([A-Z][a-z0-9_]+)")


class raw_sql(str):
    """ A custom type used for strings which shouldn't be escaped by pylytics
    before inserting into MySQL.
    """
    pass


def _camel_to_snake(s):
    """ Convert CamelCase to snake_case.
    """
    return "_".join(map(string.lower, _camel_words.split(s)[1::2]))


def _camel_to_title_case(s):
    """ Convert CamelCase to Title Case.
    """
    return " ".join(_camel_words.split(s)[1::2])


def escaped(s):
    """ Quote a string in backticks and double all backticks in the
    original string. This is used to ensure that odd characters and
    keywords do not cause a problem within SQL queries.
    """
    return "`" + s.replace("`", "``") + "`"


def dump(value):
    """ Convert the supplied value to a SQL literal.
    """
    if value is None:
        return "NULL"
    elif value is True:
        return "1"
    elif value is False:
        return "0"
    elif isinstance(value, raw_sql):
        return value
    elif isinstance(value, str):
        return "'%s'" % value.encode("utf-8").replace("'", "''")
    elif isinstance(value, unicode):
        return "'%s'" % value.replace("'", "''")
    elif isinstance(value, (date, time, datetime, timedelta)):
        return "'%s'" % value
    elif isinstance(value, bytearray):
        return "'%s'" % value.decode("utf-8").replace("'", "''")
    else:
        return unicode(value)


class classproperty(object):

    def __init__(self, func):
        self.func = func

    def __get__(self, inst, cls):
        return self.func(cls)

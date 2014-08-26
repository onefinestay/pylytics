from datetime import date, datetime, time, timedelta
import re
import string


_camel_words = re.compile(r"([A-Z][a-z0-9_]+)")


def _camel_to_snake(s):
    """ Convert CamelCase to snake_case.
    """
    return "_".join(map(string.lower, _camel_words.split(s)[1::2]))


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
    elif isinstance(value, str):
        return "'%s'" % value.encode("utf-8").replace("'", "''")
    elif isinstance(value, unicode):
        return "'%s'" % value.replace("'", "''")
    elif isinstance(value, (date, time, datetime, timedelta)):
        return "'%s'" % value
    else:
        return unicode(value)

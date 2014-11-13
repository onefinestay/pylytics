import datetime
import itertools
import logging
import math
import re
import string
import time

from pathos import multiprocessing

from settings import settings


# MySQL minimum timestamps are slightly above the Unix epoch.
EPOCH = datetime.datetime(1970, 1, 1, 1, 1)


_camel_words = re.compile(r"([A-Z][a-z0-9_]+)")


log = logging.getLogger("pylytics")


class raw_sql(str):
    """ A custom type used for strings which shouldn't be escaped by pylytics
    before inserting into MySQL.
    """
    pass


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
    elif isinstance(value, raw_sql):
        return value
    elif isinstance(value, str):
        return "'%s'" % value.encode("utf-8").replace("'", "''")
    elif isinstance(value, unicode):
        return "'%s'" % value.replace("'", "''")
    elif isinstance(value, (datetime.date, datetime.time, datetime.datetime,
                            datetime.timedelta)):
        return "'%s'" % value
    elif isinstance(value, bytearray):
        return "'%s'" % value.decode("utf-8").replace("'", "''")
    else:
        return unicode(value)


def batch_up(iterable, batch_size):
    """ Subdivides an iterable into smaller batches."""
    batch_number = int(math.ceil(len(iterable) / float(batch_size)))
    batches = [iterable[i * batch_size:(i + 1) * batch_size] for i in xrange(batch_number)]
    return batches


def batch_process(iterable, function, *args, **kwargs):
    """
    Uses multiprocessing to asynchronously process the data in `iterable`, by
    splitting it into batches, and passing it to `function`.

    Returns:
        A combined list of the results from each batch.

    """
    cores = multiprocessing.cpu_count() if settings.ENABLE_MP else 1
    batch_size = int(math.ceil(float(len(iterable)) / cores))

    results = []

    if not batch_size:
        return results

    batches = batch_up(iterable, batch_size)

    pool = multiprocessing.Pool(cores)
    for batch in batches:
        pool.apply_async(
            function,
            args = args + (batch,),
            callback=(lambda x: results.append(x)),
            )

    increment = 0
    no_of_batches = len(batches)
    while True:
        i = len(results)

        if i > increment:
            log.info('%s of %s batches have finished.' % (i, no_of_batches),
                     extra={"table": kwargs['tablename']})
            increment += 1

        if i == no_of_batches:
            break
        else:
            time.sleep(1)

    flattened = []
    for i in results:
        flattened.extend(i)

    return flattened

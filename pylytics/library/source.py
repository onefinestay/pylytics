from contextlib import closing
import json
import logging

from column import *
from connection import NamedConnection
from table import Table
from utils import dump
from warehouse import Warehouse


__all__ = ['Source', 'DatabaseSource']
log = logging.getLogger("pylytics")


def hydrated(cls, data):
    """ Inflate the data provided into an instance of a table class
    by mapping key to column name.
    """
    log.debug("Hydrating data %s", data)
    inst = cls()
    # TODO Isn't dict(data).items() redundant?
    for key, value in dict(data).items():
        try:
            inst[key] = value
        except KeyError:
            log.debug("No column found for key '%s'", key)
    return inst


class Source(object):
    """ Base class for data sources used by `fetch`.
    """

    @classmethod
    def define(cls, **attributes):
        return type(cls.__name__, (cls,), attributes)

    @classmethod
    def select(cls, for_class, since=None):
        """ Select data from this data source and yield each record as an
        instance of the fact class provided.
        """
        raise NotImplementedError("No select method defined for this source")

    @classmethod
    def finish(cls, for_class):
        """ Mark a selection as finished, performing any necessary clean-up
        such as deleting rows. By default, this method takes no action but
        can be overridden by subclasses.
        """
        pass

    @classmethod
    def select(cls, for_class, since=None):
        for source in (cls.execute(since=since),
                       getattr(cls, 'extra_rows', [])):
            for record in source:
                dict_record = dict(record)
                cls._apply_expansions(dict_record)
                yield hydrated(for_class, dict_record.items())

    @classmethod
    def _apply_expansions(cls, data):
        try:
            expansions = getattr(cls, "expansions")
        except AttributeError:
            pass
        else:
            for exp in expansions:
                if isinstance(exp, type) and issubclass(exp, DatabaseSource):
                    for record in exp.execute(**data):
                        data.update(record)
                elif hasattr(exp, "__call__"):
                    exp(data)
                else:
                    log.debug("Unexpected expansion type: %s",
                              exp.__class__.__name__)


class DatabaseSource(Source):
    """ Base class for remote databases used as data sources for table
    data. This class is intended to be overridden with the `database`
    and `query` attributes populated with appropriate values.

    (See unit tests for example of usage)

    """

    @classmethod
    def execute(cls, **params):
        database = getattr(cls, "database")
        query = getattr(cls, "query").format(
            **{key: dump(value) for key, value in params.items()})

        with NamedConnection(database) as connection:
            with closing(connection.cursor(dictionary=True)) as cursor:
                cursor.execute(query)
                rows = []
                for row in cursor:
                    # Dump the rows immediately into memory, otherwise
                    # the connection might timeout.
                    rows.append(row)

        for row in rows:
            yield row


class CallableSource(Source):
    """ A data source which is generated from a callable object
    (e.g. a function). The callable provided must return an iterable with each
    element being a dictionary, mapping the column names to values.

    e.g. [{'column_1': 'foo', 'column_2': 'bar'},
          {'column_1': 'more_foo', 'column_2': 'more_bar'} ...]

    """

    @classmethod
    def execute(cls, **params):
        _callable = getattr(cls, "_callable")
        args = getattr(cls, "args", [])
        kwargs = getattr(cls, "kwargs", {})
        for row in _callable(*args, **kwargs):
            yield row

# -*- encoding: utf-8 -*-

from __future__ import unicode_literals

import json
import logging
import re
import string

from datetime import date, datetime, time, timedelta
from decimal import Decimal

from column import *
from source import *


log = logging.getLogger("pylytics")


_camel_words = re.compile(r"([A-Z][a-z0-9_]+)")
_type_map = {
    bool: "TINYINT",
    date: "DATE",
    datetime: "TIMESTAMP",
    Decimal: "DECIMAL",
    float: "DOUBLE",
    int: "INT",
    long: "INT",
    str: "VARCHAR(%s)",
    time: "TIME",
    unicode: "VARCHAR(%s)",
}


def _camel_to_snake(s):
    """ Convert CamelCase to snake_case.
    """
    return "_".join(map(string.lower, _camel_words.split(s)[1::2]))


def _raw_name(name):
    for prefix in ("dim_", "fact_"):
        if name.startswith(prefix):
            name = name[len(prefix):]
    for suffix in ("_dim", "_fact"):
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    return name


def _column_name(table, column):
    table = _raw_name(table)
    if table == column:
        return column
    else:
        return "%s_%s" % (table, column)


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


def escaped(s):
    """ Quote a string in backticks and double all backticks in the
    original string. This is used to ensure that odd characters and
    keywords do not cause a problem within SQL queries.
    """
    return "`" + s.replace("`", "``") + "`"


def hydrated(cls, data):
    """ Inflate the data provided into an instance of a table class
    by mapping key to column name.
    """
    log.debug("Hydrating data %s", data)
    inst = cls()
    for key, value in dict(data).items():
        try:
            inst[key] = value
        except KeyError:
            log.debug("No column found for key '%s'", key)
    return inst


class _ColumnSet(object):
    """ Internal class for grouping and ordering column
    attributes; used by TableMetaclass.
    """

    def __init__(self):
        self.__columns = {}
        self.__primary_key = None

    def update(self, attributes):
        for key in attributes:
            if not key.startswith("_"):
                col = attributes[key]
                if isinstance(col, Column):
                    order_key = (col.__columnblock__, col.order)
                    self.__columns.setdefault(order_key, []).append((key, col))
                    if isinstance(col, PrimaryKey):
                        self.__primary_key = col

    @property
    def columns(self):
        ordered_columns = []
        for order_key, column_list in sorted(self.__columns.items()):
            ordered_columns.extend(sorted(column_list))
        return [value for key, value in ordered_columns]

    @property
    def primary_key(self):
        return self.__primary_key

    @property
    def dimension_keys(self):
        return [c for c in self.columns if isinstance(c, DimensionKey)]

    @property
    def metrics(self):
        return [c for c in self.columns if isinstance(c, Metric)]

    @property
    def natural_keys(self):
        return [c for c in self.columns if isinstance(c, NaturalKey)]


class Warehouse(object):
    """ Global data warehouse pointer singleton. This class avoids
    having to pass a data warehouse connection into every table
    operation at the expense of the ability to easily work with
    multiple data warehouses simultaneously.
    """

    __connection = None

    @classmethod
    def get(cls):
        """ Get the current data warehouse connection, warning if
        none has been defined.
        """
        if cls.__connection is None:
            log.warning("No data warehouse connection defined")
        return cls.__connection

    @classmethod
    def use(cls, connection):
        """ Register a new data warehouse connection for use by all
        table operations.
        """
        cls.__connection = connection

    @classmethod
    def execute(cls, sql, commit=False, **kwargs):
        """ Execute and optionally commit a SQL query against the
        currently registered data warehouse connection.
        """
        connection = cls.get()
        log.debug(sql)
        result = connection.execute(sql, **kwargs)
        if commit:
            connection.commit()
        return result

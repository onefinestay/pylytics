from __future__ import unicode_literals
from contextlib import closing
import logging

from column import *
from exceptions import classify_error
from schedule import Schedule
from selector import DimensionSelector
from table import Table
from utils import dump, escaped
from warehouse import Warehouse


log = logging.getLogger("pylytics")


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


class Fact(Table):
    """ Base class for all facts. Columns should be either of
    type DimensionKey or Metric.
    """

    # Attributes specific to facts only. These will be filled
    # in by the TableMetaclass on creation.
    __dimensionkeys__ = NotImplemented
    __metrics__ = NotImplemented
    __compositekey__ = NotImplemented

    # These attributes aren't touched by the metaclass.
    __dimension_selector__ = DimensionSelector()
    __schedule__ = Schedule()

    # Generic columns.
    id = PrimaryKey()
    hash_key = HashKey()
    created = CreatedTimestamp()

    @classmethod
    def build(cls):
        for dimension_key in cls.__dimensionkeys__:
            dimension_key.dimension.build()
        super(Fact, cls).build()

    @classmethod
    def update(cls, since=None, historical=False):
        if not (cls.__historical_source__ if historical and
                cls.__historical_source__ else cls.__source__):
            # Bail early before building dimensions.
            raise NotImplementedError("No data source defined")

        # Remove any duplicate dimensions.
        unique_dimensions = []
        for dimension_key in cls.__dimensionkeys__:
            if dimension_key.dimension not in unique_dimensions:
                unique_dimensions.append(dimension_key.dimension)

        for dimension in unique_dimensions:
            dimension.update(since=since, historical=historical)
        return super(Fact, cls).update(since=since, historical=historical)

    # TODO Consider adding historical to dimensions.
    @classmethod
    def historical(cls):
        """ Historical is only intended to be run once to populate a fact
        table with historical data after creation.

        Some fact tables won't have a historical query either because
        historical data isn't available, or there is no interest in the
        historical data.

        """
        cls.update(historical=True)

    @classmethod
    def insert(cls, *instances):
        """ Insert fact instances (overridden to handle Dimensions correctly)
        """
        if instances:
            columns = [column for column in cls.__columns__
                       if not isinstance(column, AutoColumn)]
            sql = "%s INTO %s (\n  %s\n)\n" % (
                cls.INSERT, escaped(cls.__tablename__),
                ",\n  ".join(escaped(column.name) for column in columns))

            batches = cls.batch(instances)
            for iteration, batch in enumerate(batches, start=1):
                log.debug('Inserting batch %s' % (iteration),
                          extra={"table": cls.__tablename__})

                insert_statement = sql
                link = "VALUES"

                for instance in batch:
                    values = []
                    for column in columns:
                        value = instance[column.name]
                        if isinstance(column, DimensionKey):
                            if not value and column.optional:
                                values.append(dump(value))
                            else:
                                values.append(
                                    "(%s)" % column.dimension.__subquery__(
                                        value,
                                        instance.__dimension_selector__.timestamp(instance) # TODO This is a bit messy - shouldn't have to pass the instance back in.
                                        )
                                    )
                        else:
                            values.append(dump(value))
                    insert_statement += link + (" (\n  %s\n)" % ",\n  ".join(values))
                    link = ","

                connection = Warehouse.get()
                try:
                    with closing(connection.cursor()) as cursor:
                        cursor.execute(insert_statement)
                except Exception as e:
                    classify_error(e)
                    log.error(e)
                    log.error(insert_statement)
                    connection.rollback()
                else:
                    connection.commit()

from contextlib import closing
import math
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

    # These attributes aren't touched by the metaclass.
    __dimension_selector__ = DimensionSelector()
    __schedule__ = Schedule()
    __historical_source__ = None

    id = PrimaryKey()
    created = CreatedTimestamp()

    @classmethod
    def build(cls):
        for dimension_key in cls.__dimensionkeys__:
            dimension_key.dimension.build()
        super(Fact, cls).build()
        # TODO: copy view functionality to here
        # TODO: Fix the rolling view for when the same dimensions is used
        # twice. The problem is having multiple join clauses.
        # Need something like this instead - INNER JOIN X AS Y ON ...
        # cls.create_or_replace_rolling_view()
        # cls.create_or_replace_midnight_view() -- only if a date column is defined

    @classmethod
    def update(cls, since=None, historical=False):
        if not (cls.__historical_source__ if historical else cls.__source__):
            # Bail early before building dimensions.
            raise NotImplementedError("No data source defined")

        # Remove any duplicate dimensions.
        unique_dimensions = []
        for dimension_key in cls.__dimensionkeys__:
            if dimension_key.dimension not in unique_dimensions:
                unique_dimensions.append(dimension_key.dimension)

        for dimension in unique_dimensions:
            dimension.update(since=since)
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
    def create_or_replace_rolling_view(cls):
        """ Build a base level view against the table that explodes all
        dimension data into one wider set of columns.
        """
        fact_table_name = cls.__tablename__
        view_name = _raw_name(fact_table_name) + "_rolling_view"
        columns = ["`fact`.`id` AS fact_id"]
        clauses = ["CREATE OR REPLACE VIEW {view} AS",
                   "SELECT\n    {columns}",
                   "FROM {source} AS fact"]
        for fact_column in cls.__columns__:
            if isinstance(fact_column, DimensionKey):
                dimension = fact_column.dimension
                table_name = dimension.__tablename__
                escaped_table_name = escaped(table_name)
                for dim_column in dimension.__columns__:
                    column_name = dim_column.name
                    alias = _column_name(table_name, column_name)
                    columns.append("%s.%s AS %s" % (
                        escaped_table_name,
                        escaped(column_name),
                        escaped(alias)))
                clauses.append("INNER JOIN %s ON %s.`id` = `fact`.%s" % (
                    escaped_table_name, escaped_table_name,
                    escaped(fact_column.name)))
            elif isinstance(fact_column, Metric):
                columns.append("`fact`.%s AS %s" % (
                    escaped(fact_column.name),
                    escaped(_raw_name(fact_column.name))))
        sql = "\n".join(clauses).format(
            view=escaped(view_name),
            source=escaped(fact_table_name),
            columns=",\n    ".join(columns))

        connection = Warehouse.get()
        try:
            with closing(connection.cursor()) as cursor:
                cursor.execute(sql)
        except:
            # TODO We want to log the sql to file.
            connection.rollback()
        else:
            connection.commit()

    @classmethod
    def insert(cls, *instances):
        """ Insert fact instances (overridden to handle Dimensions correctly)
        """
        if instances:
            columns = [column for column in cls.__columns__
                       if not isinstance(column, AutoColumn)]
            sql = "INSERT INTO %s (\n  %s\n)\n" % (
                escaped(cls.__tablename__),
                ",\n  ".join(escaped(column.name) for column in columns))

            # We can't insert too many at once, otherwise the target
            # database will 'go away'.
            # TODO These should be dynamically sized based on the
            # max_packet_size.
            # TODO Move this batching into a separate method.
            batch_size = 100
            batch_number = int(math.ceil(len(instances) / float(batch_size)))
            batches = [instances[i * batch_size:(i + 1) * batch_size] for i in xrange(batch_number)]

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
                    # TODO We want to log the sql to file.
                    connection.rollback()
                else:
                    connection.commit()

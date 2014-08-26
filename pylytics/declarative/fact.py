from column import *
from table import Table
from utils import dump, escaped
from warehouse import Warehouse


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

    id = PrimaryKey()
    created = CreatedTimestamp()

    @classmethod
    def build(cls):
        for dimension_key in cls.__dimensionkeys__:
            dimension_key.dimension.build()
        super(Fact, cls).build()
        # TODO: copy view functionality to here
        cls.create_or_replace_rolling_view()
        #cls.create_or_replace_midnight_view() -- only if a date column is defined

    @classmethod
    def update(cls, since=None):
        for dimension_key in cls.__dimensionkeys__:
            dimension_key.dimension.update(since=since)
        return super(Fact, cls).update(since)

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
        Warehouse.execute(sql)

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
            link = "VALUES"
            for instance in instances:
                values = []
                for column in columns:
                    value = instance[column.name]
                    if isinstance(column, DimensionKey):
                        values.append("(%s)" %
                                      column.dimension.__subquery__(value))
                    else:
                        values.append(dump(value))
                sql += link + (" (\n  %s\n)" % ",\n  ".join(values))
                link = ","
            Warehouse.execute(sql, commit=True)

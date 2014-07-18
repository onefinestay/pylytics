import json
import logging
import re
import string

from datetime import date, datetime, time
from decimal import Decimal
from pylytics.library.connection import DB


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
    name = "%s_%s" % (_raw_name(table), column)
    if name == "date_date":
        name = "date"
    return name


def dump(value):
    """ Convert the supplied value to a SQL literal.
    """
    if value is None:
        return "NULL"
    elif value is True:
        return "1"
    elif value is False:
        return "0"
    elif isinstance(value, (str, unicode)):
        return "'%s'" % value.replace("'", "''")
    elif isinstance(value, (date, time, datetime)):
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
        inst[key] = value
    return inst


class Column(object):
    """ A column in a table. This class has a number of subclasses
    that represent more specific categories of column, e.g. PrimaryKey.
    """

    __columnblock__ = 5  # TODO: explain
    default_size = 40

    def __init__(self, name, type, size=None, optional=False,
                 default=NotImplemented, order=None, comment=None):
        self.name = name
        self.type = type
        self.size = size
        self.optional = optional
        self.default = default
        self.order = order
        self.comment = comment

    def __repr__(self):
        return self.expression

    @property
    def expression(self):
        s = [escaped(self.name), self.type_expression]
        if not self.optional:
            s.append("NOT NULL")
        default_expression = self.default_clause
        if default_expression:
            s.append(default_expression)
        if self.comment:
            s.append("COMMENT %s" % dump(self.comment))
        return " ".join(s)

    @property
    def default_clause(self):
        if self.default is NotImplemented:
            return None
        else:
            return "DEFAULT %s" % dump(self.default)

    @property
    def type_expression(self):
        if isinstance(self.type, tuple):
            # enum
            return "ENUM(%s)" % ", ".join(map(dump, self.type))
        else:
            # standard type
            try:
                sql_type = _type_map[self.type]
            except KeyError:
                raise TypeError(self.type)
            else:
                if "%s" in sql_type:
                    if self.size is None:
                        return sql_type % str(self.default_size)
                    else:
                        return sql_type % self.size
                else:
                    return sql_type


class AutoColumn(Column):
    """ Subclass for defining columns that are not intended for
    direct insertion or updates.
    """


class PrimaryKey(AutoColumn):
    """ Column representing the primary key in a table, usually
    named 'id'.
    """

    __columnblock__ = 1

    def __init__(self, name="id", order=None, comment=None):
        Column.__init__(self, name, int, optional=False, order=order,
                        comment=comment)

    @property
    def expression(self):
        return (super(PrimaryKey, self).expression +
                " AUTO_INCREMENT PRIMARY KEY")


class NaturalKey(Column):
    """ A Dimension column that can be used as a natural key for record
    selection. This column type necessarily has a unique constraint.
    """

    __columnblock__ = 2

    @property
    def expression(self):
        return super(NaturalKey, self).expression + " UNIQUE KEY"


class DimensionKey(Column):
    """ A Fact column that is used to hold a foreign key referencing
    a Dimension table.
    """

    __columnblock__ = 3

    def __init__(self, name, dimension, order=None, comment=None):
        Column.__init__(self, name, int, order=order, comment=comment)
        self.dimension = dimension

    @property
    def expression(self):
        dimension = self.dimension
        foreign_key = "FOREIGN KEY (%s) REFERENCES %s (%s)" % (
            escaped(self.name), escaped(dimension.__tablename__),
            escaped(dimension.__primarykey__.name))
        return super(DimensionKey, self).expression + ", " + foreign_key


class Metric(Column):
    """ A column used to store fact metrics.
    """

    __columnblock__ = 4


class CreatedTimestamp(AutoColumn):
    """ An auto-populated timestamp column for storing when the
    record was created.
    """

    __columnblock__ = 6

    def __init__(self, name="created", order=None, comment=None):
        Column.__init__(self, name, datetime, order=order, comment=comment)

    @property
    def default_clause(self):
        return "DEFAULT CURRENT_TIMESTAMP"


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


class TableMetaclass(type):
    """ Metaclass for constructing all Table classes. This applies number
    of magic attributes which are used chiefly for reflection.
    """

    def __new__(mcs, name, bases, attributes):
        attributes.setdefault("__tablename__", _camel_to_snake(name))

        column_set = _ColumnSet()
        for base in bases:
            column_set.update(base.__dict__)
        column_set.update(attributes)

        attributes["__columns__"] = column_set.columns
        attributes["__primarykey__"] = column_set.primary_key

        cls = super(TableMetaclass, mcs).__new__(mcs, name, bases, attributes)

        # These attributes apply to subclasses so should only be populated
        # if an attribute exists with that name. We do this after class
        # creation as the attribute keys will probably only exist in a
        # base class, not in the `attributes` dictionary.
        if "__dimensionkeys__" in dir(cls):
            cls.__dimensionkeys__ = column_set.dimension_keys
        if "__metrics__" in dir(cls):
            cls.__metrics__ = column_set.metrics
        if "__naturalkeys__" in dir(cls):
            cls.__naturalkeys__ = column_set.natural_keys

        return cls


class Table(object):
    """ Base class for all Table classes. The class represents the table
    itself and instances represent records for that table.

    This class has two main subclasses: Fact and Dimension.

    """
    __metaclass__ = TableMetaclass

    # All these attributes should get populated by the metaclass.
    __columns__ = NotImplemented
    __primarykey__ = NotImplemented
    __tablename__ = NotImplemented

    # These attributes aren't touched by the metaclass.
    __source__ = None
    __tableargs__ = {
        "ENGINE": "InnoDB",
        "CHARSET": "utf8",
        "COLLATE": "utf8_bin",
    }

    @classmethod
    def build(cls):
        """ Create this table. Override this method to also create
        dependent tables and any related views that do not already exist.
        """
        try:
            # If this uses the staging table or similar, we can
            # automatically build this here too.
            cls.__source__.build()
        except AttributeError:
            pass
        cls.create_table(if_not_exists=True)

    @classmethod
    def create_table(cls, if_not_exists=False):
        """ Create this table in the current data warehouse.
        """
        if if_not_exists:
            verb = "CREATE TABLE IF NOT EXISTS"
        else:
            verb = "CREATE TABLE"
        columns = ",\n  ".join(col.expression for col in cls.__columns__)
        sql = "%s %s (\n  %s\n)" % (verb, cls.__tablename__, columns)
        for key, value in cls.__tableargs__.items():
            sql += " %s=%s" % (key, value)
        Warehouse.execute(sql, commit=True)

    @classmethod
    def drop_table(cls, if_exists=False):
        """ Drop this table from the current data warehouse.
        """
        if if_exists:
            verb = "DROP TABLE IF EXISTS"
        else:
            verb = "DROP TABLE"
        sql = "%s %s" % (verb, cls.__tablename__)
        Warehouse.execute(sql, commit=True)

    @classmethod
    def table_exists(cls):
        """ Check if this table exists in the current data warehouse.
        """
        connection = Warehouse.get()
        return cls.__tablename__ in connection.table_names

    @classmethod
    def fetch(cls, since=None):
        """ Fetch data from the source defined for this table and
        yield as each is received.
        """
        if cls.__source__:
            try:
                for inst in cls.__source__.select(cls, since=since):
                    yield inst
            except Exception as error:
                log.error("Error raised while fetching data: (%s: %s)",
                          error.__class__.__name__, error,
                          extra={"table_name": cls.__tablename__})
                raise
            else:
                # Only mark as finished if we've not had errors.
                cls.__source__.finish(cls)
        else:
            raise NotImplementedError("No data source defined")

    @classmethod
    def insert(cls, *instances):
        """ Insert one or more instances into the table as records.
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
                    values.append(dump(value))
                sql += link + (" (\n  %s\n)" % ",\n  ".join(values))
                link = ","
            Warehouse.execute(sql, commit=True)

    @classmethod
    def update(cls, since=None):
        """ Fetch some data from source and insert it directly into the table.
        """
        instances = list(cls.fetch(since=since))
        count = len(instances)
        log.info("Fetched %s record%s", count, "" if count == 1 else "s",
                 extra={"table_name": cls.__tablename__})
        cls.insert(*instances)

    def __getitem__(self, column_name):
        """ Get a value by table column name.
        """
        for key in dir(self):
            if not key.startswith("_"):
                column = getattr(self.__class__, key, None)
                if isinstance(column, Column) and column.name == column_name:
                    value = getattr(self, key)
                    return None if value is column else value
        raise KeyError("No such table column '%s'" % column_name)

    def __setitem__(self, column_name, value):
        """ Set a value by table column name.
        """
        for key in dir(self):
            if not key.startswith("_"):
                column = getattr(self.__class__, key, None)
                if isinstance(column, Column) and column.name == column_name:
                    setattr(self, key, value)
                    return
        raise KeyError("No such table column '%s'" % column_name)


class Dimension(Table):
    """ Base class for all dimensions. Note that a Dimension should
    always contain at least one NaturalKey column.
    """

    # Attributes specific to dimensions only. These will be filled
    # in by the TableMetaclass on creation.
    __naturalkeys__ = NotImplemented

    id = PrimaryKey()
    created = CreatedTimestamp()

    @classmethod
    def __subquery__(cls, value):
        """ Return a SQL SELECT query to use as a subquery within a
        fact INSERT. Does not append parentheses or a LIMIT clause.
        """
        natural_keys = cls.__naturalkeys__
        if not natural_keys:
            raise ValueError("Dimension has no natural keys")
        sql = "SELECT %s FROM %s WHERE %s" % (
            escaped(cls.__primarykey__.name), escaped(cls.__tablename__),
            " OR ".join("%s = %s" % (escaped(key.name), dump(value))
                        for key in natural_keys))
        return sql

    def __repr__(self):
        return unicode(self[self.__naturalkeys__[0].name])


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
            columns = cls.__dimensionkeys__ + cls.__metrics__
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


class Source(object):
    """ Base class for data sources used by `fetch`.
    """

    @classmethod
    def with_attributes(cls, **attributes):
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


class DatabaseSource(Source):
    """ Base class for remote databases used as data sources for table
    data. This class is intended to be overridden with the `database`
    and `query` attributes populated with appropriate values.

    (See unit tests for example of usage)

    """

    @classmethod
    def select(cls, for_class, since=None):
        database = getattr(cls, "database")
        query = getattr(cls, "query").format(since=since)
        with DB(database) as connection:
            rows, col_names, _ = connection.execute(query, get_cols=True)
        for row in rows:
            yield hydrated(for_class, zip(col_names, row))


class Staging(Source, Table):
    """ Staging is both a table and a data source.
    """

    # Keep details of records to be deleted.
    __recycling = set()

    id = PrimaryKey()
    event_name = Column("event_name", unicode, size=80)
    value_map = Column("value_map", unicode, size=2048)
    created = CreatedTimestamp()

    @classmethod
    def select(cls, for_class, since=None):
        extra = {"table_name": for_class.__tablename__}

        connection = Warehouse.get()
        log.debug("Fetching rows from staging table", extra=extra)

        events = list(getattr(cls, "events"))
        sql = """\
        SELECT id, value_map FROM staging
        WHERE event_name IN %s
        ORDER BY created, id
        """ % ("(" + repr(events)[1:-1] + ")")
        results = connection.execute(sql)

        for id_, value_map in results:
            try:
                data = json.loads(value_map)
                inst = hydrated(for_class, data)
            except Exception as error:
                log.error("Broken record (%s: %s) -- %s",
                          error.__class__.__name__, error,
                          value_map, extra=extra)
            else:
                yield inst
            finally:
                # We'll recycle the row regardless of whether or
                # not we've been able to hydrate and yield it. If
                # broken, it gets logged anyway.
                cls.__recycling.add(id_)

    @classmethod
    def finish(cls, for_class):
        if cls.__recycling:
            sql = "DELETE FROM staging WHERE id in (%s)" % (
                ",".join(map(str, cls.__recycling)))
            Warehouse.execute(sql)
            cls.__recycling.clear()

    def __init__(self, event_name, value_map):
        self.event_name = event_name
        self.value_map = json.dumps(value_map, separators=",:")

    def __eq__(self, other):
        return (self.event_name == other.event_name and
                self.value_map == other.value_map)

    def __ne__(self, other):
        return not self.__eq__(other)

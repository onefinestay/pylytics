import logging
import re
import string

from datetime import date, datetime, time
from decimal import Decimal


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


def escape(s):
    """ Quote a string in backticks and double all backticks in the
    original string. This is used to ensure that odd characters and
    keywords do not cause a problem within SQL queries.
    """
    return "`" + s.replace("`", "``") + "`"


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
        s = [escape(self.name), self.type_expression]
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
                raise TypeError(type)
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
            escape(self.name), escape(dimension.__tablename__),
            escape(dimension.__primarykey__.name))
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
    def execute(cls, sql, commit=False):
        """ Execute and optionally commit a SQL query against the
        currently registered data warehouse connection.
        """
        connection = cls.get()
        log.debug(sql)
        result = connection.execute(sql)
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
        """ Fetch records from the data source defined for this table.
        """
        if cls.__source__:
            return cls.__source__.fetch(since=since)
        else:
            raise NotImplementedError("No data source defined")

    @classmethod
    def insert(cls, *instances):
        """ Insert one or more instances into the table as records.
        """
        if not instances:
            return
        columns = [column for column in cls.__columns__
                   if not isinstance(column, AutoColumn)]
        sql = "INSERT INTO %s (\n  %s\n)\n" % (
            escape(cls.__tablename__),
            ",\n  ".join(escape(column.name) for column in columns))
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
    def pull(cls, since=None):
        instances = cls.fetch(since=since)
        count = len(instances)
        log.info("Fetched %s record%s", count, "" if count == 1 else "s",
                 extra={"table_name": cls.__tablename__})
        cls.insert(*instances)

    def __getitem__(self, column_name):
        """ Get a value by table column name.
        """
        for key in dir(self):
            if not key.startswith("_"):
                column = getattr(self.__class__, key)
                if isinstance(column, Column) and column.name == column_name:
                    value = getattr(self, key)
                    return None if value is column else value
        raise KeyError("No such table column '%s'" % column_name)

    def __setitem__(self, column_name, value):
        """ Set a value by table column name.
        """
        for key in dir(self):
            if not key.startswith("_"):
                column = getattr(self.__class__, key)
                if isinstance(column, Column) and column.name == column_name:
                    setattr(self, key, value)
                    return
        raise KeyError("No such table column '%s'" % column_name)

    def __iter__(self):
        """ Iterate through column-value pairs in order.
        """
        for column in self.__columns__:
            yield column, self[column.name]


class Dimension(Table):
    """ Base class for all dimensions.
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
            escape(cls.__primarykey__.name), escape(cls.__tablename__),
            " OR ".join("%s = %s" % (escape(key.name), dump(value))
                        for key in natural_keys))
        return sql

    def __repr__(self):
        return unicode(self[self.__naturalkeys__[0].name])


class Fact(Table):
    """ Base class for all facts.
    """

    # Attributes specific to facts only. These will be filled
    # in by the TableMetaclass on creation.
    __dimensionkeys__ = NotImplemented
    __metrics__ = NotImplemented

    id = PrimaryKey()
    created = CreatedTimestamp()

    @classmethod
    def build(cls):
        cls.create_table(if_not_exists=True)
        # TODO: copy view functionality to here

    @classmethod
    def create_table(cls, if_not_exists=False):
        for dimension_key in cls.__dimensionkeys__:
            dimension_key.dimension.create_table(if_not_exists=True)
        super(Fact, cls).create_table(if_not_exists=if_not_exists)

    @classmethod
    def insert(cls, *instances):
        """ Insert fact instances (overridden to handle Dimensions correctly)
        """
        if not instances:
            return
        columns = cls.__dimensionkeys__ + cls.__metrics__
        sql = "INSERT INTO %s (\n  %s\n)\n" % (
            escape(cls.__tablename__),
            ",\n  ".join(escape(column.name) for column in columns))
        link = "VALUES"
        for instance in instances:
            values = []
            for column in columns:
                value = instance[column.name]
                if isinstance(column, DimensionKey):
                    values.append("(%s)" % column.dimension.__subquery__(value))
                else:
                    values.append(dump(value))
            sql += link + (" (\n  %s\n)" % ",\n  ".join(values))
            link = ","
        Warehouse.execute(sql, commit=True)


class Source(object):

    def __init__(self, database, query):
        self.database = database
        self.query = query

    def fetch(self, since=None):
        return []

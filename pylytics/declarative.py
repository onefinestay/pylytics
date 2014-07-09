import logging
import re
import string

from datetime import date, datetime, time
from decimal import Decimal


log = logging.getLogger("pylytics")


_camel_words = re.compile(r"([A-Z][a-z0-9_]+)")
_type_map = {
    bool: "BIT",
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


def dump(s):
    if s is None:
        return "NULL"
    elif isinstance(s, (str, unicode)):
        return "'%s'" % s.replace("'", "''")
    else:
        return s


class Column(object):

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
        s = ["`" + self.name + "`", self.type_expression]
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


class PrimaryKey(Column):

    __columnblock__ = 1

    def __init__(self, name="id", order=None, comment=None):
        Column.__init__(self, name, int, optional=False, order=order,
                        comment=comment)

    @property
    def __ddl__(self):
        return (super(PrimaryKey, self).expression +
                " AUTO_INCREMENT PRIMARY KEY")


class NaturalKey(Column):

    __columnblock__ = 2

    @property
    def __ddl__(self):
        return super(NaturalKey, self).expression + " UNIQUE KEY"


class DimensionKey(Column):

    __columnblock__ = 3

    def __init__(self, name, dimension, order=None, comment=None):
        Column.__init__(self, name, int, order=order, comment=comment)
        self.dimension = dimension

    @property
    def __ddl__(self):
        dimension = self.dimension
        reference_clause = "REFERENCES %s(`%s`)" % (
            dimension.__tablename__, dimension.__primarykey__.name)
        return super(DimensionKey, self).expression + " " + reference_clause


class Metric(Column):

    __columnblock__ = 4


class CreatedTimestamp(Column):

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
    def dimensions(self):
        return [c for c in self.columns if isinstance(c, DimensionKey)]

    @property
    def metrics(self):
        return [c for c in self.columns if isinstance(c, Metric)]


class TableMetaclass(type):

    def __new__(mcs, name, bases, attributes):
        attributes.setdefault("__tablename__", _camel_to_snake(name))

        column_set = _ColumnSet()
        for base in bases:
            column_set.update(base.__dict__)
        column_set.update(attributes)

        attributes["__columns__"] = column_set.columns
        attributes["__dimensions__"] = column_set.dimensions
        attributes["__metrics__"] = column_set.metrics
        attributes["__primarykey__"] = column_set.primary_key

        return super(TableMetaclass, mcs).__new__(mcs, name, bases, attributes)


class Table(object):
    __metaclass__ = TableMetaclass

    __columns__ = NotImplemented
    __tablename__ = NotImplemented
    __tableargs__ = {
        "ENGINE": "InnoDB",
        "CHARSET": "utf8",
        "COLLATE": "utf8_bin",
    }

    @classmethod
    def create(cls, connection):
        verb = "CREATE TABLE"
        columns = ",\n    ".join(col.expression for col in cls.__columns__)
        sql = "%s %s (\n    %s\n)" % (verb, cls.__tablename__, columns)
        for key, value in cls.__tableargs__.items():
            sql += " %s=%s" % (key, value)
        log.debug(sql)
        connection.execute(sql)


class Dimension(Table):

    id = PrimaryKey()
    created = CreatedTimestamp()


class Fact(Table):

    id = PrimaryKey()
    created = CreatedTimestamp()

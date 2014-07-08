import re
import string

from datetime import date, datetime, time
from decimal import Decimal


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


def ddl(obj):
    if obj is None:
        return None
    else:
        return obj.__ddl__


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
        return self.__ddl__

    @property
    def __ddl__(self):
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
        return (super(PrimaryKey, self).__ddl__ +
                " AUTO_INCREMENT PRIMARY KEY")


class NaturalKey(Column):

    __columnblock__ = 2

    @property
    def __ddl__(self):
        return super(NaturalKey, self).__ddl__ + " UNIQUE KEY"


class DimensionKey(Column):

    __columnblock__ = 3

    def __init__(self, name, dimension, order=None, comment=None):
        Column.__init__(self, name, int, order=order, comment=comment)
        self.dimension = dimension

    @property
    def __ddl__(self):
        dimension = self.dimension
        reference_clause = "REFERENCES %s(`%s`)" % (
            dimension.__tablename__, dimension.__primary_key)
        return super(DimensionKey, self).__ddl__ + " " + reference_clause


class Metric(Column):

    __columnblock__ = 4


class CreatedTimestamp(Column):

    __columnblock__ = 6

    def __init__(self, name="created", order=None, comment=None):
        Column.__init__(self, name, datetime, order=order, comment=comment)

    @property
    def default_clause(self):
        return "DEFAULT CURRENT_TIMESTAMP"


class Table(object):

    __tableargs__ = {
        "ENGINE": "InnoDB",
        "CHARSET": "utf8",
        "COLLATE": "utf8_bin",
    }

    def __init__(self):
        pass

    @property
    def __tablename__(self):
        return _camel_to_snake(self.__class__.__name__)

    @property
    def __primarykey__(self):
        keys = [key for key in dir(self) if not key.startswith("_")]
        for key in keys:
            value = getattr(self, key)
            if isinstance(value, PrimaryKey):
                return key
        return None

    @property
    def __columns__(self):
        keys = [key for key in dir(self) if not key.startswith("_")]
        column_dict = {}
        for key in keys:
            value = getattr(self, key)
            if isinstance(value, Column):
                order_key = (value.__columnblock__, value.order)
                column_dict.setdefault(order_key, []).append((key, value))
        ordered = []
        for order_key, column_list in sorted(column_dict.items()):
            ordered.extend(sorted(column_list))
        return ordered

    @property
    def __ddl__(self):
        verb = "CREATE TABLE"
        columns = ",\n    ".join(
            ddl(column) for key, column in self.__columns__)
        sql = "%s %s (\n    %s\n)" % (verb, self.__tablename__, columns)
        for key, value in self.__tableargs__.items():
            sql += " %s=%s" % (key, value)
        return sql


class Dimension(Table):

    id = PrimaryKey()
    created = CreatedTimestamp()

    def __init__(self):
        Table.__init__(self)


class Fact(Table):

    id = PrimaryKey()
    created = CreatedTimestamp()

    def __init__(self):
        Table.__init__(self)

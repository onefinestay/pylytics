from datetime import date, datetime, time, timedelta
from decimal import Decimal

from utils import dump, escaped


__all__ = ['Column', 'NaturalKey', 'DimensionKey', 'Metric', 'AutoColumn',
           'PrimaryKey', 'HashKey', 'CreatedTimestamp', 'ApplicableFrom']


_type_map = {
   bool: "TINYINT",
   date: "DATE",
   datetime: "TIMESTAMP",
   Decimal: "DECIMAL(%s,%s)",
   float: "DOUBLE",
   int: "INT",
   long: "INT",
   timedelta: "TIME",
   time: "TIME",
   basestring: "VARCHAR(%s)",
   str: "VARCHAR(%s)",
   unicode: "VARCHAR(%s)",
   bytearray: "VARBINARY(%s)"
}


class Column(object):
    """ A column in a table. This class has a number of subclasses
    that represent more specific categories of column, e.g. PrimaryKey.
    """

    __columnblock__ = 5
    default_size = {
        basestring: 40,
        str: 40,
        unicode: 40,
        Decimal: (6, 2),
        bytearray: 20
        }

    def __init__(self, name, type, size=None, optional=False,
                 default=NotImplemented, order=None, comment=None):
        self.name = name
        self.type = type
        self.size = size
        self.optional = optional
        self.default = default
        self.order = order
        self.comment = comment
        self.__schemaname__ = name.replace('_', ' ')

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
                        return sql_type % self.default_size[self.type]
                    else:
                        return sql_type % self.size
                else:
                    return sql_type


class NaturalKey(Column):
    """ A Dimension column that can be used as a natural key for record
    selection. This column type necessarily has a unique constraint.
    """

    __columnblock__ = 2

    @property
    def index_expression(self):
        """ All Natural Keys have indexes created for them by appending
        this to the CREATE table statement.
        """
        return ('INDEX `%s` (`%s`)' % (self.name, self.name))


class DimensionKey(Column):
    """ A Fact column that is used to hold a foreign key referencing
    a Dimension table.
    """

    __columnblock__ = 3

    def __init__(self, name, dimension, order=None, comment=None,
                 optional=False):
        Column.__init__(self, name, int, order=order, comment=comment,
                        optional=optional)
        self.dimension = dimension

    @property
    def expression(self):
        dimension = self.dimension
        foreign_key = "FOREIGN KEY (%s) REFERENCES %s (%s)" % (
            escaped(self.name), escaped(dimension.__tablename__),
            escaped(dimension.__primarykey__.name))
        return super(DimensionKey, self).expression + ", " + foreign_key


class DegenerateDimension(Column):
    """ A Fact column that is used to hold a dimension value in the fact table
    rather than having a foreign key to a separate dimension table. Use it
    sparingly. It makes sense for dimensions such as 'order number' in a sales
    fact. Each row will have a unique order number, so moving it out into a
    separate dimension table doesn't save any space, and results in an
    unnecessary join.
    """

    __columnblock__ = 3


class Metric(Column):
    """ A column used to store fact metrics.
    """

    __columnblock__ = 4


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


class HashKey(Column):
    """ A hash of the user defined columns is used as the unique key in
    dimension and fact tables.

    This is because composite unique keys in MySQL have a
    size limit, which means that composite unique keys consisting of a lot of
    columns will likely fail. Also, the hash value can be used to verify data
    integrity.

    """
    __columnblock__ = 6

    def __init__(self, name="hash_key", order=None, comment=None):
        Column.__init__(self, name, bytearray, optional=False, size=20,
                        order=order, comment=comment)

    @property
    def expression(self):
        return super(HashKey, self).expression + " UNIQUE KEY"


class ApplicableFrom(Column):
    """ This is a special column which is only used in dimensions.

    Some dimension rows are only applicable over certain time periods. This
    column allows facts to match on dimensions rows, not just just based on
    dimension values, but also when that dimension is valid.

    """

    __columnblock__ = 7

    def __init__(self, name="applicable_from", order=None, comment=None):
        Column.__init__(self, name, datetime, optional=False, order=order,
                        comment=comment)

    @property
    def default_clause(self):
        return "DEFAULT CURRENT_TIMESTAMP"


class CreatedTimestamp(AutoColumn):
    """ An auto-populated timestamp column for storing when the
    record was created.
    """

    __columnblock__ = 8

    def __init__(self, name="created", order=None, comment=None):
        Column.__init__(self, name, datetime, order=order, comment=comment)

    @property
    def default_clause(self):
        return "DEFAULT 0"

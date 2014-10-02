from datetime import date, datetime, time, timedelta
from decimal import Decimal

from utils import dump, escaped


__all__ = ['Column', 'NaturalKey', 'DimensionKey', 'Metric', 'AutoColumn',
           'PrimaryKey', 'CreatedTimestamp']


_type_map = {
   bool: "TINYINT",
   date: "DATE",
   datetime: "TIMESTAMP",
   Decimal: "DECIMAL",
   float: "DOUBLE",
   int: "INT",
   long: "INT",
   str: "VARCHAR(%s)",
   timedelta: "TIME",
   time: "TIME",
   unicode: "VARCHAR(%s)",
}


class Column(object):
    """ A column in a table. This class has a number of subclasses
    that represent more specific categories of column, e.g. PrimaryKey.
    """

    __columnblock__ = 5  # TODO: explain
    default_size = 40

    def __init__(self, name, type, size=None, optional=False,
                 default=NotImplemented, order=None, comment=None, null=None):
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

    def __init__(self, name, dimension, order=None, comment=None, null=None):
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

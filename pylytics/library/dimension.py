import datetime

from column import *
from table import Table
from utils import dump, escaped


class Dimension(Table):
    """ Base class for all dimensions. Note that a Dimension should
    always contain at least one NaturalKey column.
    """

    # Attributes specific to dimensions only. These will be filled
    # in by the TableMetaclass on creation.
    __naturalkeys__ = NotImplemented

    INSERT = "INSERT IGNORE"

    id = PrimaryKey()
    applicable_from = ApplicableFrom()
    created = CreatedTimestamp()

    @classmethod
    def __subquery__(cls, value, timestamp):
        """ Return a SQL SELECT query to use as a subquery within a
        fact INSERT. Does not append parentheses or a LIMIT clause.
        """
        value_type = type(value)
        natural_keys = [key for key in cls.__naturalkeys__
                        if key.type is value_type]
        if not natural_keys:
            raise ValueError("Value type '%s' does not match type of any "
                             "natural key for dimension "
                             "'%s'" % (value_type.__name__, cls.__name__))

        sql = 'SELECT {primary_key} FROM {table_name} WHERE {selector} AND `applicable_from` = (SELECT max(`applicable_from`) FROM {table_name} WHERE {selector} AND `applicable_from` <= "{timestamp}")'.format(
            primary_key=escaped(cls.__primarykey__.name),
            table_name=escaped(cls.__tablename__),
            selector=" OR ".join("%s = %s" % (escaped(key.name), dump(value)) for key in natural_keys),
            timestamp=timestamp
            )
        return sql

    def __repr__(self):
        return unicode(self[self.__naturalkeys__[0].name])

from contextlib import closing
from datetime import date
from distutils.version import StrictVersion
import logging

from column import *
from exceptions import classify_error
from settings import settings
from utils import _camel_to_snake, dump, escaped
from warehouse import Warehouse


log = logging.getLogger("pylytics")


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

    INSERT = "INSERT"

    @classmethod
    def create_trigger(cls):
        """ There's a constraint in earlier versions of MySQL where only one
        timestamp column can have a CURRENT_TIMESTAMP default value.

        These triggers get around that problem.

        """
        drop_trigger = """\
        DROP TRIGGER IF EXISTS created_timestamp_{tablename}
        """
        create_trigger = """\
        CREATE TRIGGER created_timestamp_{tablename}
        BEFORE INSERT ON {tablename}
        FOR EACH ROW BEGIN
            IF NEW.created = '0000-00-00 00:00:00' THEN
                SET NEW.created = NOW();
            END IF;
        END
        """
        min_version = settings.MYSQL_MIN_VERSION        
        if min_version and StrictVersion(Warehouse.version) >= StrictVersion(
                settings.MYSQL_MIN_VERSION):
            return

        connection = Warehouse.get()
        with closing(connection.cursor()) as cursor:
            for query in (drop_trigger, create_trigger):
                try:
                    cursor.execute(query.format(tablename=cls.__tablename__))
                except Exception as exception:
                    classify_error(exception)
                    raise exception

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
        cls.create_trigger()

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

        connection = Warehouse.get()
        with closing(connection.cursor()) as cursor:
            try:
                cursor.execute(sql)
            except Exception as exception:
                classify_error(exception)
                raise exception

    @classmethod
    def drop_table(cls, if_exists=False):
        """ Drop this table from the current data warehouse.
        """
        if if_exists:
            verb = "DROP TABLE IF EXISTS"
        else:
            verb = "DROP TABLE"
        sql = "%s %s" % (verb, cls.__tablename__)

        connection = Warehouse.get()
        with closing(connection.cursor()) as cursor:
            try:
                cursor.execute(sql)
            except:
                connection.rollback()
            else:
                connection.commit()

    @classmethod
    def table_exists(cls):
        """ Check if this table exists in the current data warehouse.
        """
        return cls.__tablename__ in Warehouse.table_names

    @classmethod
    def fetch(cls, since=None, historical=False):
        """ Fetch data from the source defined for this table and
        yield as each is received.
        """
        source = cls.__historical_source__ if historical else cls.__source__
        if source:
            try:
                for inst in source.select(cls, since=since):
                    yield inst
            except Exception as error:
                log.error("Error raised while fetching data: (%s: %s)",
                          error.__class__.__name__, error,
                          extra={"table": cls.__tablename__})
                raise
            else:
                # Only mark as finished if we've not had errors.
                source.finish(cls)
        else:
            raise NotImplementedError("No data source defined")

    @classmethod
    def insert(cls, *instances):
        """ Insert one or more instances into the table as records.
        """
        if instances:
            columns = [column for column in cls.__columns__
                       if not isinstance(column, AutoColumn)]
            sql = "%s INTO %s (\n  %s\n)\n" % (
                cls.INSERT, escaped(cls.__tablename__),
                ",\n  ".join(escaped(column.name) for column in columns))
            link = "VALUES"
            for instance in instances:
                values = []
                for column in columns:
                    value = instance[column.name]
                    values.append(dump(value))
                sql += link + (" (\n  %s\n)" % ",\n  ".join(values))
                link = ","

            connection = Warehouse.get()
            with closing(connection.cursor()) as cursor:
                try:
                    cursor.execute(sql)
                except:
                    connection.rollback()
                else:
                    connection.commit()

    @classmethod
    def update(cls, since=None, historical=False):
        """ Fetch some data from source and insert it directly into the table.
        """
        instances = list(cls.fetch(since=since, historical=historical))
        count = len(instances)
        log.info("Fetched %s record%s", count, "" if count == 1 else "s",
                 extra={"table": cls.__tablename__})
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

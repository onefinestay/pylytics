from __future__ import unicode_literals
from contextlib import closing
import logging
import math

from column import *
from exceptions import classify_error, BrokenPipeError
from settings import settings
from template import TemplateConstructor
from utils import (_camel_to_snake, _camel_to_title_case, dump, escaped,
                   classproperty, raw_sql)
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

    @property
    def composite_key(self):
        """Returns the values which make up the composite unique key."""
        _ = (AutoColumn, ApplicableFrom, HashKey, Metric)
        return [c for c in self.columns if not isinstance(c, _)]


class TableMetaclass(type):
    """ Metaclass for constructing all Table classes. This applies number
    of magic attributes which are used chiefly for reflection.
    """

    def __new__(mcs, name, bases, attributes):
        tablename = _camel_to_snake(name)
        if 'dimension' in [i.__name__.lower() for i in bases]:
            tablename += '_dimension'
        attributes.setdefault("__tablename__", tablename)
        attributes.setdefault("__schemaname__", _camel_to_title_case(name))

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
        if "__compositekey__" in dir(cls):
            cls.__compositekey__ = column_set.composite_key

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
    __historical_source__ = None
    __source__ = None
    __tableargs__ = {
        "ENGINE": "InnoDB",
        "CHARSET": "utf8",
        "COLLATE": "utf8_bin",
    }

    INSERT = "INSERT IGNORE"

    def __init__(self, *args, **kwargs):
        values = ', '.join(
            ["IFNULL(%s,'NULL')" % escaped(c.name) for c in
            self.__compositekey__]
            )
        self['hash_key'] = raw_sql("UNHEX(SHA1(CONCAT_WS(',', %s)))" % values)

    @classproperty
    def trigger_name(cls):
        return 'created_timestamp_' + cls.__tablename__

    @classmethod
    def create_trigger(cls):
        """ There's a constraint in earlier versions of MySQL where only one
        timestamp column can have a CURRENT_TIMESTAMP default value.

        These triggers get around that problem.

        Returns:
            True if a trigger was created, or False if the trigger already
            exists.

        """
        if cls.trigger_name in Warehouse.trigger_names:
            log.info('%s already exists - skipping.' % cls.trigger_name)
            return False

        trigger = """\
        CREATE TRIGGER %s
        BEFORE INSERT ON %s
        FOR EACH ROW BEGIN
            IF NEW.created = '0000-00-00 00:00:00' THEN
                SET NEW.created = NOW();
            END IF;
        END
        """ % (cls.trigger_name, cls.__tablename__)

        connection = Warehouse.get()
        with closing(connection.cursor()) as cursor:
            try:
                cursor.execute(trigger)
            except Exception as exception:
                classify_error(exception)
                raise exception
            else:
                log.info('%s created.' % cls.trigger_name)
                return True

    @classmethod
    def build(cls):
        """ Create this table. Override this method to also create
        dependent tables and any related views that do not already exist.
        """
        cls.create_table()
        cls.create_trigger()

    @classmethod
    def create_table(cls):
        """ Create this table in the current datawarehouse.

        Returns:
            True if the table was created, or False if the table already
            exists.

        """
        if cls.table_exists:
            log.info('%s already exists - skipping.' % cls.__tablename__)
            return False

        verb = "CREATE TABLE"
        columns = [col.expression for col in cls.__columns__]

        if hasattr(cls, '__naturalkeys__'):
            indexes = [col.index_expression for col in cls.__naturalkeys__]
        else:
            indexes = []

        body = ",\n  ".join(columns + indexes)

        sql = "%s %s (\n  %s\n)" % (verb, cls.__tablename__, body)

        for key, value in cls.__tableargs__.items():
            sql += " %s=%s" % (key, value)

        connection = Warehouse.get()
        with closing(connection.cursor()) as cursor:
            try:
                cursor.execute(sql)
            except Exception as exception:
                classify_error(exception)
                raise exception
            else:
                return True

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

    @classproperty
    def table_exists(cls):
        """ Check if this table exists in the current data warehouse.
        """
        return cls.__tablename__ in Warehouse.table_names

    @classmethod
    def fetch(cls, since=None, historical=False):
        """ Fetch data from the source defined for this table and
        yield as each is received.
        """
        source = (cls.__historical_source__ if historical and
                  cls.__historical_source__ else cls.__source__)
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
    def batch(cls, instances):
        """ Subdivides instances into smaller batches ready for insertion."""
        batch_size = settings.BATCH_SIZE
        batch_number = int(math.ceil(len(instances) / float(batch_size)))
        batches = [instances[i * batch_size:(i + 1) * batch_size] for i in xrange(batch_number)]
        return batches

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

            batches = cls.batch(instances)
            for iteration, batch in enumerate(batches, start=1):
                log.debug('Inserting batch %s' % (iteration),
                          extra={"table": cls.__tablename__})

                insert_statement = sql
                link = "VALUES"

                for instance in batch:
                    values = []
                    for column in columns:
                        value = instance[column.name]
                        values.append(dump(value))
                    insert_statement += link + (" (\n  %s\n)" % ",\n  ".join(values))
                    link = ","

                for i in range(1, 3):
                    connection = Warehouse.get()
                    try:
                        cursor = connection.cursor()
                        cursor.execute(insert_statement)
                        cursor.close()

                    except Exception as e:
                        classify_error(e)
                        if e.__class__ == BrokenPipeError and i == 1:
                            log.info(
                                'Trying once more with a fresh connection',
                                extra={"table": cls.__tablename__}
                                )
                            connection.close()
                        else:
                            log.error(e)
                            return
                    else:
                        connection.commit()
                        break

        log.debug('Finished updating %s' % cls.__tablename__,
                  extra={"table": cls.__tablename__})

    @classmethod
    def update(cls, since=None, historical=False):
        """ Fetch some data from source and insert it directly into the table.
        """
        instances = list(cls.fetch(since=since, historical=historical))
        count = len(instances)
        log.info("Fetched %s record%s", count, "" if count == 1 else "s",
                 extra={"table": cls.__tablename__})
        cls.insert(*instances)

    @classmethod
    def template(cls):
        print TemplateConstructor(cls).rendered

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

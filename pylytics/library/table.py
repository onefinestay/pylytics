import datetime
from importlib import import_module
import logging
import os
import warnings

from MySQLdb import IntegrityError

from pylytics.library.connection import DB
from pylytics.library.exceptions import NoSuchTableError
from pylytics.library.settings import settings

from utils.text_conversion import camelcase_to_underscore


log = logging.getLogger("pylytics")

STAGING = "__staging__"


class Table(object):
    """Base class."""

    DROP_TABLE = """\
    DROP TABLE IF EXISTS `{table}`
    """
    DROP_TABLE_FORCE = """\
    SET foreign_key_checks = 0;
    DROP TABLE IF EXISTS `{table}`;
    SET foreign_key_checks = 1;
    """
    SELECT_COUNT = """\
    SELECT COUNT(*) FROM `{table}`
    """
    SELECT_NONE = """\
    SELECT * FROM `{table}` LIMIT 0,0
    """
    SHOW_PRIMARY_KEY = """\
    SHOW INDEXES FROM `{table}`
    WHERE Key_name = 'PRIMARY'
    """

    base_package = None
    surrogate_key_column = "id"
    natural_key_column = None
    source_db = None
    source_query = None

    def __init__(self, *args, **kwargs):
        if 'connection' in kwargs:
            self.connection = kwargs['connection']
        self.warehouse_connection = DB(settings.pylytics_db)
        self.class_name = self.__class__.__name__
        self.table_name = camelcase_to_underscore(self.class_name)
        self.dim_or_fact = None
        if "base_package" in kwargs:
            self.base_package = kwargs["base_package"]
        if "surrogate_key_column" in kwargs:
            self.surrogate_key_column = kwargs["surrogate_key_column"]
        if "natural_key_column" in kwargs:
            self.natural_key_column = kwargs["natural_key_column"]

    @property
    def ddl_file_path(self):
        """ The expected absolute file path of the SQL file containing the
        CREATE TABLE statement for this table.
        """
        parts = [self.dim_or_fact, 'sql', "%s.sql" % self.table_name]
        if self.base_package:
            module = import_module("test.unit.library.fixtures")
            parts.insert(0, os.path.dirname(module.__file__))
        return os.path.join(*parts)

    def load_ddl(self):
        """ Load DDL from SQL file associated with this table.
        """
        path = self.ddl_file_path
        log.info("Loading SQL from {}".format(path))
        try:
            with open(path) as f:
                sql = f.read()
        except IOError as error:
            log.error("Unable to load DDL from file {}".format(error))
            raise
        else:
            return sql.strip()

    @classmethod
    def _values_placeholder(cls, length):
        """
        Returns a placeholder string of the specified length (to use within a
        MySQL INSERT query).

        Example usage:
        > self._values_placeholder(3)
        Returns:
        > '%s, %s, %s'

        """
        return ", ".join(["%s"] * length)

    @property
    def frequency(self):
        """
        This needs thinking about ... what if you wanted to run each Sunday?
        """
        return datetime.timedelta(days=1)

    def test(self):
        """
        This needs to be overridden for each instance of Table.
        """
        warnings.simplefilter('always')
        warnings.warn("There is no test for this class!", Warning)

    def drop(self, force=False):
        """
        Drops the table.

        If 'force' is True, ignores the foreign key constraints and forces the
        table to be deleted.

        """
        if force:
            log.info("Dropping table {} (with force)".format(self.table_name))
            sql = self.DROP_TABLE_FORCE.format(table=self.table_name)
        else:
            log.info("Dropping table {}".format(self.table_name))
            sql = self.DROP_TABLE.format(table=self.table_name)
        try:
            self.connection.execute(sql)
        except IntegrityError:
            log.error("Table could not be deleted due to foreign key "
                      "constraints, try removing the fact tables first")

    def exists(self):
        """ Determine whether or not the table exists and return a boolean
        to indicate which.
        """
        statement = self.SELECT_NONE.format(table=self.table_name)
        try:
            self.connection.execute(statement)
        except NoSuchTableError:
            return False
        else:
            return True

    def count(self):
        """ Return a count of the number of rows present in this table.
        """
        sql = self.SELECT_COUNT.format(table=self.table_name)
        return self.connection.execute(sql)[0][0]

    def primary_key(self):
        """ Fetch the name of the primary key column for this table.
        """
        sql = self.SHOW_PRIMARY_KEY.format(table=self.table_name)
        rows, columns, _ = self.connection.execute(sql, get_cols=True)
        if rows:
            return rows[0][columns.index("Column_name")]
        else:
            return None

    def build(self, sql=None):
        """ Ensure the table is built, attempting to create it if necessary.

        Returns: `True` if the table already existed or was successfully
                 built, `False` otherwise.

        """
        # If the table already exists, exit as successful.
        if self.exists():
            log.debug("%s | Table already exists", self.table_name)
            return True

        # Load SQL from file if none is supplied.
        if sql is None:
            try:
                sql = self.load_ddl()
            except IOError:
                return False

        log.debug("%s | Executing SQL: %s", self.table_name, sql)
        try:
            self.connection.execute(sql)
        except Exception as error:
            log.error("%s | SQL execution error: %s", self.table_name, error)
            self.connection.rollback()
            return False
        else:
            self.connection.commit()
            log.info("%s | Table successfully built", self.table_name)
            return True

    def _fetch(self, *args, **kwargs):
        """ Fetch data from the appropriate source, as described by
        the `source_db` attribute.
        """
        if self.source_db == STAGING:
            rows = self._fetch_from_staging()
        else:
            rows = self._fetch_from_source()
        return rows

    def _fetch_from_source(self):
        """ Fetch data from a SQL data source as described by the `source_db`
        and `source_query` attributes. Should return a `SourceData` instance.
        """
        log.error("%s | Cannot fetch rows from source database",
                  self.table_name)

    def _fetch_from_staging(self):
        """ Fetch data from staging table and inflate it ready for insertion.
        Should return a `SourceData` instance.
        """
        log.error("%s | Cannot fetch rows from staging table", self.table_name)

    def _insert(self, data):
        """ Insert rows from the supplied `SourceData` instance into the table.
        """
        log.error("%s | Cannot insert rows into table", self.table_name)

    def update(self):
        """ Update the table by fetching data from its designated origin and
        inserting it into the table.
        """
        rows = self._fetch()
        # Insert data
        if rows:
            log.debug("%s | Updating table", self.table_name)
            self._insert(rows)
            log.debug("%s | Update complete", self.table_name)
        else:
            log.debug("%s | No update required - nothing "
                      "to insert", self.table_name)


class SourceData(object):

    def __init__(self, **kwargs):
        self.column_names = kwargs.get("column_names")
        self.column_types = kwargs.get("column_types")
        self.rows = kwargs.get("rows")

    def __len__(self):
        return len(self.rows)

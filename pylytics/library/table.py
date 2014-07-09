import datetime
from importlib import import_module
import inspect
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


def filter_dict(d, prefix):
    len_prefix = len(prefix)
    return {k[len_prefix:]: v for k, v in d.items() if k.startswith(prefix)}


class Table(object):
    """Base class."""

    DESCRIBE = """\
    DESCRIBE `{table}`
    """
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
    SHOW_TABLES = """\
    SHOW TABLES
    """

    base_package = None
    surrogate_key_column = "id"
    natural_key_column = None
    source_db = None
    source_query = None

    __description = None

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

    def __getattr__(self, name):
        if not name.startswith("log_"):
            raise AttributeError(name)

        # Grab everything after the underscore, e.g. log_info -> info
        level = name[4:]

        # Close over the level and self.table_name variables
        def log_closure(msg, *args, **kwargs):
            __traceback_hide__ = True

            # Collect extra metadata to attach to log record.
            stack = inspect.stack()
            top_frame = stack[1][0]
            code = top_frame.f_code
            func_name = code.co_name
            file_name = "".join(code.co_filename.partition("pylytics")[1:])
            extra = {
                "culprit": "%s in %s" % (func_name, file_name),
                "stack": [(frame[0], frame[2]) for frame in stack[1:]],
                "table": self.table_name,
            }

            # Look up the appropriate log function and call it
            # with the extra metadata attached.
            log_x = getattr(log, level)
            log_x(msg, *args, extra=extra, **kwargs)

        return log_closure

    @property
    def table_stem(self):
        stem = self.table_name
        for prefix in ("dim_", "fact_"):
            if stem.startswith(prefix):
                stem = stem[len(prefix):]
        for suffix in ("_dim", "_fact"):
            if stem.endswith(suffix):
                stem = stem[:-len(suffix)]
        return stem

    @property
    def description(self):
        """ Lazily fetch and return the current table description
        from the database.
        """
        if self.__description is None:
            sql = self.DESCRIBE.format(table=self.table_name)
            self.__description = self.connection.execute(sql)
        return self.__description

    @property
    def column_names(self):
        """ Tuple of all column names within this table.
        """
        return tuple(column[0] for column in self.description)

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
        self.log_info("Loading SQL from %s", path)
        try:
            with open(path) as f:
                sql = f.read()
        except IOError as error:
            self.log_error("Unable to load DDL from file %s", error)
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
            self.log_info("Dropping table (with force)")
            sql = self.DROP_TABLE_FORCE.format(table=self.table_name)
        else:
            self.log_info("Dropping table")
            sql = self.DROP_TABLE.format(table=self.table_name)
        try:
            self.connection.execute(sql)
        except IntegrityError:
            self.log_error("Table could not be deleted due to foreign key "
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
            self.log_debug("Table already exists")
            return True

        # Load SQL from file if none is supplied.
        if sql is None:
            try:
                sql = self.load_ddl()
            except IOError:
                return False

        self.log_debug("Executing SQL: %s", sql)
        try:
            self.connection.execute(sql)
        except Exception as error:
            self.log_error("SQL execution error: %s", error)
            self.connection.rollback()
            return False
        else:
            self.connection.commit()
            self.log_info("Table successfully built")
            return True

    def _fetch(self, *args, **kwargs):
        """ Fetch data from the appropriate source, as described by
        the `source_db` attribute.
        """
        if self.source_db == STAGING:
            staging_kwargs = filter_dict(kwargs, "staging_")
            rows = self._fetch_from_staging(*args, **staging_kwargs)
        else:
            source_kwargs = filter_dict(kwargs, "source_")
            rows = self._fetch_from_source(*args, **source_kwargs)
        return rows

    def _fetch_from_source(self, *args, **kwargs):
        """ Fetch data from a SQL data source as described by the `source_db`
        and `source_query` attributes. Should return a `SourceData` instance.
        """
        self.log_error("Cannot fetch rows from source database")

    def _fetch_from_staging(self, *args, **kwargs):
        """ Fetch data from staging table and inflate it ready for insertion.
        Should return a `SourceData` instance.
        """
        self.log_error("Cannot fetch rows from staging table")

    def _insert(self, data):
        """ Insert rows from the supplied `SourceData` instance into the table.
        """
        self.log_error("Cannot insert rows into table")

    def update(self):
        """ Update the table by fetching data from its designated origin and
        inserting it into the table.
        """
        rows = self._fetch(staging_delete=True)
        # Insert data
        if rows:
            self.log_debug("Updating table")
            self._insert(rows)
            self.log_debug("Update complete")
        else:
            self.log_debug("No update required - nothing to insert")


class SourceData(object):

    def __init__(self, **kwargs):
        self.column_names = kwargs.get("column_names")
        self.column_types = kwargs.get("column_types")
        self.rows = kwargs.get("rows")

    def __len__(self):
        return len(self.rows)

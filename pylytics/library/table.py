import datetime
import os
import sys
import textwrap
import warnings

from utils.text_conversion import camelcase_to_underscore
from utils.terminal import print_status


class Table(object):
    """Base class."""

    surrogate_key_column = "id"
    natural_key_column = None

    def __init__(self, *args, **kwargs):
        if 'connection' in kwargs:
            self.connection = kwargs['connection']
        self.class_name = self.__class__.__name__
        self.table_name = camelcase_to_underscore(self.class_name)
        self.dim_or_fact = None
        if "surrogate_key_column" in kwargs:
            self.surrogate_key_column = kwargs["surrogate_key_column"]
        if "natural_key_column" in kwargs:
            self.natural_key_column = kwargs["natural_key_column"]

    @classmethod
    def _print_status(cls, message, **kwargs):
        print_status(message, **kwargs)

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
        return " ".join(['%s,' for i in range(length)])[:-1]
    
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
        # Status.
        msg = "Dropping %s (force='%s')" % (self.table_name, str(force))
        self._print_status(msg)

        query = 'DROP TABLE IF EXISTS `%s`' % self.table_name

        if force:
            query = """
                SET foreign_key_checks = 0;
                %s;
                SET foreign_key_checks = 1;
                """ % query
        try:
            self.connection.execute(query)
        except MySQLdb.IntegrityError:
            print """--> Table could not be deleted, due to foreign key \
                constraints. Try removing the fact tables first."""

        if self.class_name == 'Fact':
            # Try and drop the corresponding view.
            query = 'DROP VIEW IF EXISTS `vw_%s' % self.table_name
            try:
                self.connection.execute(query)
            except MySQLdb.IntegrityError:
                self._print_status("Unable to drop view for %s" % (
                                                            self.table_name))

    def exists(self):
        """ Determine whether or not the table exists and return a boolean
        to indicate which.
        """
        try:
            self.connection.execute("SELECT * FROM `%s` "
                                    "LIMIT 0,0" % self.table_name)
        except Exception as db_error:
            if 1146 in db_error.args:
                return False
            else:
                raise
        else:
            return True

    def count(self):
        """ Return a count of the number of rows present in this table.
        """
        return self.connection.execute("SELECT COUNT(*) "
                                       "FROM `%s`" % self.table_name)[0][0]

    def build(self, sql=None):
        """ Builds the table using SQL from a file or, optionally, passed in
        directly as a string.
        """
        
        # Status.
        msg = "Building %s on %s" % (self.table_name, self.connection.database)
        self._print_status(msg)
        
        # Build the table only if it doesn't already exist.
        if self.exists():
            self._print_status("Database already exists - {0} on {1}".format(
                               self.table_name, self.connection.database))
            return False

        self._print_status("Reading SQL")
        if sql is None:
            # Load from file instead.

            file_name = os.path.join(self.dim_or_fact, 'sql',
                                     "%s.sql" % self.table_name)
            # Read the sql file.
            try:
                with open(file_name) as sql_file:
                    sql = sql_file.read().strip()
            except Exception as e:
                self._print_status("Cannot read SQL: {}".format(e))
                return False

        self._print_status("Executing SQL")
        try:
            self.connection.execute(sql)
        except Exception as e:
            self._print_status("Cannot execute SQL: {}".format(e))
            self.connection.rollback()
            return False
        else:
            self._print_status('Table built.')
            self.connection.commit()

        # Table successfully built: return True.
        return True

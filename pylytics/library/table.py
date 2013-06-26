import datetime
import os
import sys
import textwrap
import warnings

import MySQLdb

from utils import camelcase_to_underscore


class Table(object):
    """Base class."""
    
    def __init__(self, *args, **kwargs):
        if 'connection' in kwargs:
            self.connection = kwargs['connection']
        self.class_name = self.__class__.__name__
        self.table_name = camelcase_to_underscore(self.class_name)

    def _print_status(self, message):
        """Use this for all printing all output."""
        sys.stdout.write("%s %s\n" % (str(datetime.datetime.now()), message))

    def _values_placeholder(self, length):
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
            self._print_status('Success')
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

    def build(self):
        """Builds the table."""
        # Status.
        msg = "Building %s on %s" % (self.table_name, self.connection.database)
        self._print_status(msg)

        # Read the sql file.
        with open(os.path.join(self.dim_or_fact, 'sql',
                               "%s.sql" % self.table_name)) as sql_file:
            sql = sql_file.read().strip()

        # Execute the sql.
        try:
            self.connection.execute(sql)
        except MySQLdb.OperationalError, db_error:
            if 1050 in db_error.args:
                sys.stdout.write("Database already exists - %s on %s\n" % (
                                    self.table_name, self.connection.database))
            else:
                raise db_error

        self._print_status('Success')

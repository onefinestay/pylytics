from contextlib import closing
import logging


log = logging.getLogger("pylytics")


class Warehouse(object):
    """ Global data warehouse pointer singleton. This class avoids
    having to pass a data warehouse connection into every table
    operation at the expense of the ability to easily work with
    multiple data warehouses simultaneously.
    """

    __connection = None

    @classmethod
    def get(cls):
        """ Get the current data warehouse connection, warning if
        none has been defined.
        """
        if cls.__connection is None:
            log.warning("No data warehouse connection defined")
        if not cls.__connection.is_connected():
            cls.__connection.reconnect(attempts=5)
        return cls.__connection

    @classmethod
    def use(cls, connection):
        """ Register a new data warehouse connection for use by all
        table operations.
        """
        cls.__connection = connection

    @property
    def table_names(self):
        """ List of names of all the tables (and views) currently
        defined within the database.
        """
        connection = self.get()
        with closing(connection.cursor()) as cursor:
            return [record[0] for record in cursor.execute("SHOW TABLES")]

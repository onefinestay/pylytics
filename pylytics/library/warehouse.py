from contextlib import closing
import logging

from utils import classproperty


log = logging.getLogger("pylytics")


class Warehouse(object):
    """ Global data warehouse pointer singleton. This class avoids
    having to pass a data warehouse connection into every table
    operation at the expense of the ability to easily work with
    multiple data warehouses simultaneously.
    """

    __connection = None
    __version = None

    @classmethod
    def get(cls):
        """ Get the current data warehouse connection, warning if
        none has been defined.
        """
        if cls.__connection is None:
            log.warning("No data warehouse connection defined")
        elif not cls.__connection.is_connected():
            cls.__connection.reconnect(attempts=5)
        return cls.__connection

    @classmethod
    def use(cls, connection):
        """ Register a new data warehouse connection for use by all
        table operations.
        """
        cls.__connection = connection
        cls.__version = None

    @classproperty
    def table_names(cls):
        """ List of names of all the tables (and views) currently
        defined within the database.
        """
        connection = cls.get()
        with closing(connection.cursor()) as cursor:
            cursor.execute("SHOW TABLES")
            return [record[0] for record in cursor]

    @classproperty
    def trigger_names(cls):
        """ List of trigger names which exist in the database.
        """
        connection = cls.get()
        with closing(connection.cursor()) as cursor:
            cursor.execute("""
                SELECT trigger_name FROM information_schema.triggers
                """)
            return [record[0] for record in cursor]

    @classproperty
    def version(cls):
        """ Returns the MySQL server version number."""
        if not cls.__version:
            cls.__version = "{}.{}.{}".format(*cls.get().get_server_version())

        return cls.__version

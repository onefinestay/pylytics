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
        return cls.__connection

    @classmethod
    def use(cls, connection):
        """ Register a new data warehouse connection for use by all
        table operations.
        """
        cls.__connection = connection

    @classmethod
    def execute(cls, sql, commit=False, **kwargs):
        """ Execute and optionally commit a SQL query against the
        currently registered data warehouse connection.
        """
        connection = cls.get()
        log.debug(sql)
        result = connection.execute(sql, **kwargs)
        if commit:
            connection.commit()
        return result

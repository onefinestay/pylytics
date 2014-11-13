import warnings

from mysql import connector

from pylytics.library.settings import settings


def execute(connection, statement):
    """ Execute a single SQL statement against a connection.
    """
    cursor = connection.cursor()
    cursor.execute(statement)
    cursor.close()


# TODO This is redundant now - use get_named_connection instead.
def db_fixture(database):
    """ Create and return a database fixture based on details from the
    global database settings.
    """
    db_settings = settings.DATABASES[database]

    connection = connector.connect(
        host=db_settings["host"],
        user=db_settings["user"],
        passwd=db_settings["passwd"],
        use_unicode=True,
        charset='utf8',
        )

    with warnings.catch_warnings():
        # Hide warnings
        warnings.simplefilter("ignore")
        execute(connection, "DROP DATABASE IF EXISTS {}".format(db_settings["db"]))
    execute(connection, "CREATE DATABASE {}".format(db_settings["db"]))
    connection.commit()

    connection.database = db_settings["db"]
    return connection

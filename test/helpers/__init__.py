import warnings

import MySQLdb as mysql

from pylytics.library import connection


def execute(conn, statement):
    """ Execute a single SQL statement against a connection.
    """
    cursor = conn.cursor()
    cursor.execute(statement)
    cursor.close()


def db_fixture(database):
    """ Create and return a database fixture based on details from the
    global database settings.
    """
    db_settings = dict(connection.settings.DATABASES[database])

    conn = mysql.connect(host=db_settings["host"], user=db_settings["user"],
                         passwd=db_settings["passwd"])
    with warnings.catch_warnings():
        # Hide warnings
        warnings.simplefilter("ignore")
        execute(conn, "DROP DATABASE IF EXISTS {}".format(db_settings["db"]))
    execute(conn, "CREATE DATABASE {}".format(db_settings["db"]))
    conn.commit()
    conn.close()

    fixture = connection.DB(database)
    fixture.connect()
    return fixture

import warnings

import MySQLdb as mysql

from pylytics.library import connection


def execute(conn, statement):
    """ Execute a single SQL statement against a connection.
    """
    cursor = conn.cursor()
    cursor.execute(statement)
    cursor.close()


def db_fixture(request, **kwargs):
    """ Create and return a database fixture.
    """
    host = kwargs.get("host", "localhost")
    user = kwargs["user"]
    passwd = kwargs["passwd"]
    db = kwargs["db"]

    credentials = {
        'host': host,
        'user': user,
        'passwd': passwd,
        'db': db,
    }

    conn = mysql.connect(host=host, user=user, passwd=passwd)
    with warnings.catch_warnings():
        # Hide warnings
        warnings.simplefilter("ignore")
        execute(conn, "DROP DATABASE IF EXISTS {}".format(db))
    execute(conn, "CREATE DATABASE IF NOT EXISTS {}".format(db))
    conn.commit()
    conn.close()

    connection.settings.DATABASES.update({db: credentials})
    connection.settings.pylytics_db = db

    fixture = connection.DB(db)
    fixture.connect()
    return fixture

"""
Utilities for making database connections easier.
"""

import MySQLdb

import settings


def run_query(database, query):
    """
    Very high level interface for running database queries.

    Example usage:
    response = run_query('ecommerce', 'SELECT * from SOME_TABLE')

    """
    with DB(database) as database:
        response = database.execute(query)
        return response


class DB(object):
    """
    Create a connection to a database in settings.py.

    High level usage:
    with DB('ecommerce') as db:
        response = db.execute('SELECT * FROM SOME_TABLE')

    Lower level usage:
    example = DB('example')
    example.connect()
    content = example.execute('SELECT * FROM SOME_TABLE')
    example.close()

    """

    def __init__(self, database):
        if database not in (settings.DATABASES.keys()):
            raise Exception("The Database isn't recognised! Check your \
                             settings in settings.py")
        else:
            self.database = database
            self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = MySQLdb.connect(
                    **settings.DATABASES[self.database])

    def close(self):
        """You should always call this after opening a connection."""
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def execute(self, query, values=None, many=False, get_count_cols=False):
        cursor = None
        data = None
        count_cols = None

        if not self.connection:
            raise Exception('You must connect first!')
        else:
            cursor = self.connection.cursor()

            if not values:
                # SELECT query
                cursor.execute(query)
                data = cursor.fetchall()

            else:
                # INSERT or REPLACE query
                if many:
                    cursor.executemany(query, values)
                else:
                    cursor.execute(query, values)

            if get_count_cols:
                # Get column count
                if values:
                    raise Exception("Only works on a SELECT query.")
                count_cols = len(cursor.description)

            cursor.close()

        if get_count_cols:
            return (data, count_cols)
        else:
            return data

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

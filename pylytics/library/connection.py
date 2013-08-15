"""
Utilities for making database connections easier.
"""

import MySQLdb

import settings


class UnknownColumnTypeError(Exception):
    def __init__(self, error):
        self.error = error
    
    def __str__(self):
        return "The type code {}, which has been retrieved from " \
               "a SELECT query, doesn't exist in the " \
               "'field_types' dictionary.".format(self.error)


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
    
    # List of SQL types
    field_types = {
         0: 'DECIMAL',
         1: 'INT(11)',
         2: 'INT(11)',
         3: 'INT(11)',
         4: 'FLOAT',
         5: 'DOUBLE',
         6: 'TEXT',
         7: 'TIMESTAMP',
         8: 'INT(11)',
         9: 'INT(11)',
         10: 'DATE',
         11: 'TIME',
         12: 'DATETIME',
         13: 'YEAR',
         14: 'DATE',
         15: 'VARCHAR(255)',
         16: 'BIT',
         246: 'DECIMAL',
         247: 'VARCHAR(255)',
         248: 'SET',
         249: 'TINYBLOB',
         250: 'MEDIUMBLOB',
         251: 'LONGBLOB',
         252: 'BLOB',
         253: 'VARCHAR(255)',
         254: 'VARCHAR(255)',
         255: 'VARCHAR(255)'
    }
    
    def __init__(self, database):
        if database not in (settings.DATABASES.keys()):
            raise Exception("The Database %s isn't recognised! Check your \
                             settings in settings.py" % database)
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

    def execute(self, query, values=None, many=False, get_cols=False):
        cursor = None
        data = None
        cols_names = None
        cols_types = None

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

            if get_cols:
                # Get columns list
                if values:
                    raise Exception("Only works on a SELECT query.")
                cols_names, cols_types_ids = zip(*cursor.description)[0:2]
                try:
                    cols_types = [self.field_types[i] for i in cols_types_ids]
                except Exception as e:
                    raise UnknownColumnTypeError(e)

            cursor.close()

        if get_cols:
            return (data, cols_names, cols_types)
        else:
            return data

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

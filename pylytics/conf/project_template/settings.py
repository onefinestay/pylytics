"""This is an example settings.py file."""

import os


# specify which of your DBs will hold the data created by pylytics
pylytics_db = "example"

# define all databases
DATABASES = {
    'example': {
        'host': 'localhost',
        'user': 'test',
        'passwd': 'test',
        'db': 'example',
    }
}

CLIENT_CONFIG = os.path.join(os.path.dirname(__file__), 'client.cnf')

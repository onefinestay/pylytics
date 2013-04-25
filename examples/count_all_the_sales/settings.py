"""This is an example settings.py file."""

# specify which of your DBs will hold the data created by pylytics
pylytics_db = "test"

# define all databases, sources of data and target repo for created data
DATABASES = {
    'test': {
        'host': 'localhost',
        'user': 'root',
        'passwd': '',
        'db': 'pylytics_test',
    },
}

"""This is an example settings.py file."""

# specify which of your DBs will hold the data created by pylytics
pylytics_db = "fact"

# define all databases, sources of data and target repo for created data
DATABASES = {
    'fact': {
        'host': 'localhost',
        'user': 'root',
        'passwd': '',
        'db': 'pylytics_fact',
    },
    'sales': {
        'host': 'localhost',
        'user': 'root',
        'passwd': '',
        'db': 'pylytics_sales',
    },
}

"""
This is an example environment.py file.

Insert your credentials where indicated, and rename the file to environment.py.

"""
# specify the full path to your facts directory
facts_dir = "/path/to/dir/"

# specify which of your DBs will hold the data created by pylytics
pylytics_db = "example"

# define all databases, sources of data and target repo for created data
DATABASES = {
    'example': {
        'host': 'localhost',
        'user': 'test',
        'passwd': 'test',
        'db': 'example',
    }
}

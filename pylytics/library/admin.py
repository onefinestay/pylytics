from pylytics.library.collection import CREATE_STAGING_TABLE
from pylytics.library.connection import DB
from pylytics.library.settings import settings


def init_tables():
    """ Initialises core tables to be used by pylytics functionality
    """
    with DB(settings.pylytics_db) as database:
        database.execute(CREATE_STAGING_TABLE)
        database.commit()

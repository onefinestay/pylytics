from .collection import CREATE_STAGING_TABLE
from .connection import DB

import settings


def init_tables():
    """ Initialises core tables to be used by pylytics functionality
    """
    with DB(settings.pylytics_db) as database:
        database.execute(CREATE_STAGING_TABLE)
        database.commit()

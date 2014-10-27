from pylytics.library.column import Column, NaturalKey
from pylytics.library.dimension import Dimension
from pylytics.library.source import DatabaseSource


class User(Dimension):

    __source__ = DatabaseSource.define(
        database="test",
        query="""
            SELECT
                user_id,
                username
            FROM users
            """
        )

    user_id = NaturalKey('user_id', int)
    username = Column('username', str)

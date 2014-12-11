from pylytics.library.column import Column, NaturalKey
from pylytics.library.dimension import Dimension
from pylytics.library.source import DatabaseSource

from transform import convert_str_to_int


class Store(Dimension):
    """ Just an example Dimension.
    """

    __source__ = DatabaseSource.define(
        database="test",
        query="""
            SELECT
                store_id,
                store_shortcode,
                store_size,
                employees
            FROM store
            """,
        expansions=[convert_str_to_int],
        )

    store_id = NaturalKey('store_id', int)
    store_shortcode = NaturalKey('store_shortcode', basestring)
    store_size = Column('store_size', basestring)
    employees = Column('employees', int)

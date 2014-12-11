from datetime import timedelta

from pylytics.library.column import DimensionKey, Metric
from pylytics.library.fact import Fact
from pylytics.library.schedule import Schedule
from pylytics.library.source import DatabaseSource

from dimension.store import Store


class Sales(Fact):
    """ Just an example Fact.
    """

    __source__ = DatabaseSource.define(
        database="sales",
        query="SELECT store, sales_amount FROM sales_table"
        )

    __schedule__ = Schedule(repeats=timedelta(hours=1))

    store = DimensionKey('store', Store)
    sales_amount = Metric('sales_amount', int)

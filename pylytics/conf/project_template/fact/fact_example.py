from decimal import Decimal

from pylytics.library.column import DimensionKey, Metric
from pylytics.library.fact import Fact
from pylytics.library.source import DatabaseSource


from ..dimension.dim_example import User


class Sales(Fact):

    __source__ = DatabaseSource.define(
        database="test",
        query="""
            SELECT
                user_id,
                sales_amount
            FROM sales
            """
        )

    user = DimensionKey('date', User)
    sales_amount = Metric('sales_amount', Decimal, size=(12, 6))

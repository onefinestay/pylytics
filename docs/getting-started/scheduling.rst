Scheduling
==========

Rather than maintaining several CRONs to update facts at certain times, pylytics contains basic scheduling capabilities.

Defining schedules
******************

Here is an example fact which uses scheduling::

    from datetime import timedelta
    
    from pylytics.library.fact import Fact
    from pylytics.library.schedule import Schedule
    from pylytics.library.column import Metric, DimensionKey

    from dimension.store import Manager, Store
    from dimension.date import Date
    from dimension.time import Time


    class Sales(Fact):

        __source__ = DatabaseSource.define(
            database="sales",
            query="SELECT * FROM sales_table"
        )

        __schedule__ = Schedule(repeats=timedelta(hours=1))

        date = DimensionKey('date', Date)
        time = DimensionKey('time', Time)
        store = DimensionKey('store', Store)
        manager = DimensionKey('manager', Manager)
        sales_amount = Metric('sales_amount', int)

It will update every hour.

There are three arguments you can pass into Schedule:

* repeats
* starts
* ends
* timezone

repeats
~~~~~~~

This is a timedelta objects which specifies how frequently the fact updates.

If `starts` is 3pm, and `ends` is 4pm, and `repeats` is 30 minutes, then the fact is scheduled to run at 3pm, 3.30pm and 4pm.

This schedule would look like::

    __schedule__ = Schedule(repeats=timedelta(minutes=30), starts=time(hour=3),
                            ends=time(hour=4))

The smallest permissible `repeats` value is 10 minutes. It's unlikely any fact will need to be updated more frequently than this.

Default schedule
****************

If no ``Schedule`` is defined, the fact will just be scheduled to run at midnight every day.

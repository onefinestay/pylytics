from datetime import date

from pylytics.declarative import Column, Dimension, Fact, Metric, DimensionKey


DAY_NAMES = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
MONTH_NAMES = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')


class DateDimension(Dimension):
    __tablename__ = "dim_date"

    date = NaturalKey("date", date)
    day = Column("day", int)
    day_name = Column("day_name", DAY_NAMES)
    day_of_week = Column("day_of_week", int)
    week = Column("week", int)
    full_week = Column("full_week", str, size=15)
    month = Column("month", int)
    month_name = Column("month_name", MONTH_NAMES)
    full_month = Column("full_name", str, size=15)
    quarter = Column("quarter", int)
    quarter_name = Column("quarter_name", ('Q1', 'Q2', 'Q3', 'Q4'))
    full_quarter = Column("full_quarter", str, size=15)
    year = Column("year", int)


class PlaceDimension(Dimension):
    __tablename__ = "dim_place"


class BoringEventFact(Fact):
    __tablename__ = "boring_event_facts"

    date = DimensionKey("when", DateDimension)
    place = DimensionKey("where", PlaceDimension)
    people = Metric("num_people", int)
    duration = Metric("duration", float)
    very_boring = Metric("very_boring", bool)

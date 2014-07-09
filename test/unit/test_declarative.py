from datetime import date

import pytest

from pylytics.declarative import (
    Column, Dimension, DimensionKey, Fact, Metric, NaturalKey)
from pylytics.library.exceptions import TableExistsError


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
    pass


class BoringEventFact(Fact):
    __tablename__ = "boring_event_facts"

    date = DimensionKey("when", DateDimension)
    place = DimensionKey("where", PlaceDimension)
    people = Metric("num_people", int)
    duration = Metric("duration", float)
    very_boring = Metric("very_boring", bool)


### TESTS ###


def test_can_create_dimension(empty_warehouse):
    DateDimension.create(empty_warehouse)
    assert DateDimension.exists(empty_warehouse)


def test_cannot_create_dimension_twice(empty_warehouse):
    DateDimension.create(empty_warehouse)
    with pytest.raises(TableExistsError):
        DateDimension.create(empty_warehouse)


def test_can_create_dimension_only_if_not_exists(empty_warehouse):
    DateDimension.create(empty_warehouse)
    DateDimension.create(empty_warehouse, if_not_exists=True)


def test_dimension_has_sensible_defaults():
    assert PlaceDimension.__tablename__ == "place_dimension"
    columns = PlaceDimension.__columns__
    assert len(columns) == 2
    assert columns[0].name == "id"
    assert columns[-1].name == "created"


def test_can_drop_dimension(empty_warehouse):
    DateDimension.create(empty_warehouse)
    DateDimension.drop(empty_warehouse)
    assert not DateDimension.exists(empty_warehouse)


def test_can_create_fact_if_no_dimensions_exist(empty_warehouse):
    BoringEventFact.create(empty_warehouse)
    assert BoringEventFact.exists(empty_warehouse)
    assert DateDimension.exists(empty_warehouse)
    assert PlaceDimension.exists(empty_warehouse)

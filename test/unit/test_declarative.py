from datetime import date, timedelta, datetime
import logging

import pytest

from pylytics.declarative import (
    Column, Dimension, DimensionKey, Fact, Metric, NaturalKey, Warehouse,
    escaped, DatabaseSource, Staging)
from pylytics.library.exceptions import TableExistsError


log = logging.getLogger("pylytics")


class Date(Dimension):
    __tablename__ = "dim_date"

    # Just use a fixed three year period for testing.
    start_date = date(1999, 1, 1)
    end_date = date(2001, 12, 31)

    day_names = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
    month_names = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')

    date = NaturalKey("date", date)
    day = Column("day", int)
    day_name = Column("day_name", day_names)
    day_of_week = Column("day_of_week", int)
    week = Column("week", int)
    full_week = Column("full_week", str, size=15)
    month = Column("month", int)
    month_name = Column("month_name", month_names)
    full_month = Column("full_name", str, size=15)
    quarter = Column("quarter", int)
    quarter_name = Column("quarter_name", ('Q1', 'Q2', 'Q3', 'Q4'))
    full_quarter = Column("full_quarter", str, size=15)
    year = Column("year", int)

    @classmethod
    def fetch(cls, since=None):
        """ Create date instances as there's no remote data source
        for this one.
        """
        table_name = cls.__tablename__
        log.info("Fetching data from the depths of time itself",
                 extra={"table_name": table_name})

        # Get the last inserted date
        sql = "SELECT MAX(`date`) FROM %s" % escaped(table_name)
        cur_date = Warehouse.execute(sql)[0][0]

        if cur_date is None:
            # Build history.
            cur_date = cls.start_date

        dates = []
        while cur_date <= cls.end_date:
            yield Date(cur_date)
            cur_date = cur_date + timedelta(days=1)

    def __init__(self, date_obj):
        quarter = (date_obj.month - 1) // 3 + 1
        self.date = date_obj
        self.day = date_obj.day
        self.day_name = date_obj.strftime("%a")          # e.g. Mon
        self.day_of_week = int(date_obj.strftime("%u"))  # ISO day no of week
        self.week = int(date_obj.strftime("%U"))         # ISO week number
        self.full_week = date_obj.strftime("%Y-%U")      # Year and week no
        self.month = int(date_obj.strftime("%m"))        # Month number
        self.month_name = date_obj.strftime("%b")        # Month name
        self.full_month = date_obj.strftime("%Y-%m")     # Year and month
        self.quarter = quarter                           # Quarter no
        self.quarter_name = "Q{}".format(quarter)        # e.g. Q1
        self.full_quarter = '{0}-{1}'.format(date_obj.year, quarter)
        self.year = date_obj.year


@pytest.fixture
def place_source(empty_warehouse):
    empty_warehouse.execute("CREATE TABLE place_source(code varchar(40))")
    empty_warehouse.execute("INSERT INTO place_source(code) VALUES('MOON')")


class Place(Dimension):

    # This class method creates a custom subclass of `DatabaseSource`
    # that selects using a SQL query.
    __source__ = DatabaseSource.definition(
        database="test_warehouse",
        query="select code as geo_code from place_source",
    )

    geo_code = NaturalKey("geo_code", str, size=20)


class BoringEvent(Fact):
    __tablename__ = "boring_event_fact"

    # This class method creates a custom subclass of `Staging`
    # that filters only "boring" events. One expansion is also
    # defined to explode the "expansion_key_1" value into more
    # values drawn from the defined database source. This
    # mechanism can be used to seek further details on
    # bookings, etc.
    __source__ = Staging.definition(
        events=["boring"],
        expansions=[
            DatabaseSource.definition(
                database="test_warehouse",
                query="""\
                SELECT
                    colour AS colour_of_stuff,
                    size AS size_of_stuff
                FROM extra_table
                WHERE id = {expansion_key_1}
                """,
            ),
        ],
    )

    date = DimensionKey("when", Date)
    place = DimensionKey("where", Place)
    people = Metric("num_people", int)
    duration = Metric("duration", float)
    very_boring = Metric("very_boring", bool)
    stuff_colour = Metric("colour_of_stuff", str, optional=True)
    stuff_size = Metric("size_of_stuff", str, optional=True)


### TESTS ###


def test_cannot_create_a_column_with_an_odd_type():
    column = Column("foo", object)
    with pytest.raises(TypeError):
        _ = column.type_expression


def test_can_create_dimension(empty_warehouse):
    Warehouse.use(empty_warehouse)
    Date.create_table()
    assert Date.table_exists()


def test_cannot_create_dimension_twice(empty_warehouse):
    Warehouse.use(empty_warehouse)
    Date.create_table()
    with pytest.raises(TableExistsError):
        Date.create_table()


def test_can_create_dimension_only_if_not_exists(empty_warehouse):
    Warehouse.use(empty_warehouse)
    Date.create_table()
    Date.create_table(if_not_exists=True)


def test_dimension_has_sensible_defaults():
    assert Place.__tablename__ == "place"
    columns = Place.__columns__
    assert columns[0].name == "id"
    assert columns[-1].name == "created"


def test_can_drop_dimension(empty_warehouse):
    Warehouse.use(empty_warehouse)
    Date.create_table()
    Date.drop_table()
    assert not Date.table_exists()


def test_can_create_fact_if_no_dimensions_exist(empty_warehouse):
    Warehouse.use(empty_warehouse)
    BoringEvent.build()
    assert BoringEvent.table_exists()
    assert Date.table_exists()
    assert Place.table_exists()


def test_can_create_fact_if_some_dimensions_exist(empty_warehouse):
    Warehouse.use(empty_warehouse)
    Date.create_table()
    BoringEvent.build()
    assert BoringEvent.table_exists()
    assert Date.table_exists()
    assert Place.table_exists()


def test_can_create_fact_if_all_dimensions_exist(empty_warehouse):
    Warehouse.use(empty_warehouse)
    Date.create_table()
    Place.create_table()
    BoringEvent.build()
    assert BoringEvent.table_exists()
    assert Date.table_exists()
    assert Place.table_exists()


@pytest.mark.usefixtures("place_source")
def test_can_insert_fact_record(empty_warehouse):
    Warehouse.use(empty_warehouse)

    BoringEvent.build()
    Date.update()
    Place.update()

    fact_1 = BoringEvent()
    fact_1.date = date(2000, 7, 16)
    fact_1.place = "MOON"
    fact_1.people = 3
    fact_1.duration = 10.7
    fact_1.very_boring = False
    BoringEvent.insert(fact_1)

    rows, columns, _ = Warehouse.execute(
        "select * from %s" % BoringEvent.__tablename__, get_cols=True)
    data = [dict(zip(columns, row)) for row in rows]
    assert len(data) == 1
    datum = data[0]
    assert datum["num_people"] == 3
    assert datum["duration"] == 10.7
    assert bool(datum["very_boring"]) is False


@pytest.mark.usefixtures("place_source")
def test_can_insert_fact_record_from_staging_source(empty_warehouse):
    Warehouse.use(empty_warehouse)
    Staging.build()
    BoringEvent.build()

    # Prepare expansion data ready for expansion.
    Warehouse.execute("""\
    create table extra_table (
        id int primary key,
        colour varchar(20),
        size varchar(20)
    )""")
    Warehouse.execute("""\
    insert into extra_table (id, colour, size)
    values (12, 'pink', '37kg'), (13, 'grey', '9 miles')
    """)

    # Insert staging record.
    Staging.insert(Staging("boring", {
        "when": date(2000, 7, 16).isoformat(),
        "where": "MOON",
        "num_people": 3,
        "duration": 10.7,
        "very_boring": False,
        "pointless_ignored_value": "spoon",
        "expansion_key_1": 12,
    }))

    # Perform update.
    BoringEvent.update()

    # Check a record has been correctly inserted.
    rows, columns, _ = Warehouse.execute(
        "select * from %s" % BoringEvent.__tablename__, get_cols=True)
    data = [dict(zip(columns, row)) for row in rows]
    assert len(data) == 1
    datum = data[0]
    assert datum["num_people"] == 3
    assert datum["duration"] == 10.7
    assert bool(datum["very_boring"]) is False
    assert datum["colour_of_stuff"] == "pink"
    assert datum["size_of_stuff"] == "37kg"

import json
import pytest
from pylytics.library.table import Table

from test.unit.library.conftest import JOURNEY
from test.unit.library.fixtures.fact.fact_ring_journey import FactRingJourney
from test.unit.library.fixtures.fact.fact_ring_journey_from_staging import \
    FactRingJourneyFromStaging
from test.unit.library.fixtures.fact.fact_ring_journey_with_custom_loader import \
    FactRingJourneyWithCustomLoader


FIXTURE_PACKAGE = "test.unit.library.fixtures"


@pytest.mark.usefixtures("middle_earth")
class TestFactFromSource(object):

    @pytest.fixture
    def fact(self, empty_warehouse):
        return FactRingJourney(connection=empty_warehouse,
                               base_package=FIXTURE_PACKAGE)

    def test_can_build_from_source(self, fact):
        # when
        fact.build()
        # then
        assert fact.exists()
        table_names = fact.connection.table_names
        assert fact.rolling_view_name in table_names
        # we have no `dim_date` so can't build a midnight view
        assert fact.midnight_view_name not in table_names

    def test_can_update_from_source(self, fact):
        # when
        fact.update()
        # then
        assert fact.exists()
        assert fact.count() == len(JOURNEY)


@pytest.mark.usefixtures("middle_earth", "staging")
class TestFactFromStaging(object):

    @pytest.fixture
    def fact(self, empty_warehouse):
        return FactRingJourneyFromStaging(connection=empty_warehouse,
                                          base_package=FIXTURE_PACKAGE)

    @pytest.fixture
    def source(self, fact, warehouse, staging):
        for id_, dim_ring, dim_location, fellowship_count in JOURNEY:
            sql = """\
            INSERT INTO staging(collector_type, fact_table, value_map)
            VALUES('TEST', '{}', '{}')
            """.format(fact.table_name, json.dumps({
                "id": id_,
                "dim_ring": dim_ring,
                "dim_location": dim_location,
                "fellowship_count": fellowship_count,
            }, separators=",:"))
            warehouse.execute(sql)
            warehouse.commit()

    def test_can_build_from_staging(self, fact):
        # when
        fact.build()
        # then
        assert fact.exists()

    @pytest.mark.usefixtures("source")
    def test_can_update_from_staging(self, fact):
        # when
        fact.update()
        # then
        assert fact.count() == len(JOURNEY)

    @pytest.mark.usefixtures("source")
    def test_update_removes_rows_from_staging(self, warehouse, fact):
        # given
        class Staging(Table):
            pass
        staging = Staging(connection=warehouse)
        # when
        fact.update()
        # then
        assert staging.count() == 0


@pytest.mark.usefixtures("middle_earth", "staging")
class TestFactWithCustomLoader(object):

    @pytest.fixture
    def fact(self, empty_warehouse):
        return FactRingJourneyWithCustomLoader(connection=empty_warehouse,
                                               base_package=FIXTURE_PACKAGE)

    @pytest.fixture
    def source(self, fact, warehouse, staging):
        for id_, dim_ring, dim_location, fellowship_count in JOURNEY:
            sql = """\
            INSERT INTO staging(collector_type, fact_table, value_map)
            VALUES('TEST', '{}', '{}')
            """.format(fact.table_name, json.dumps({
                "data": {
                    "id": id_,
                    "dim_ring": dim_ring,
                    "dim_location": dim_location,
                    "fellowship_count": fellowship_count,
                },
            }, separators=",:"))
            warehouse.execute(sql)
            warehouse.commit()

    def test_can_build_from_staging(self, fact):
        # when
        fact.build()
        # then
        assert fact.exists()

    @pytest.mark.usefixtures("source")
    def test_can_update_from_staging(self, fact):
        # when
        fact.update()
        # then
        assert fact.count() == len(JOURNEY)

    @pytest.mark.usefixtures("source")
    def test_update_removes_rows_from_staging(self, warehouse, fact):
        # given
        class Staging(Table):
            pass
        staging = Staging(connection=warehouse)
        # when
        fact.update()
        # then
        assert staging.count() == 0

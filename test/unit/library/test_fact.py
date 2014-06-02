import pytest

from test.unit.library.conftest import (
    build_ring_journey_fact_table, build_ring_dimension_table,
    build_location_dimension_table)


@pytest.mark.usefixtures("warehouse")
class TestFact(object):

    def test_can_build(self, warehouse, middle_earth):
        # when
        fact = build_ring_journey_fact_table(warehouse)
        # then
        assert fact.exists()

    @pytest.mark.usefixtures("ring_journey_source")
    def test_can_update(self, warehouse):
        # given
        dim_ring = build_ring_dimension_table(warehouse)
        dim_location = build_location_dimension_table(warehouse)
        fact = build_ring_journey_fact_table(warehouse)
        # when
        fact.update()
        # then
        assert False

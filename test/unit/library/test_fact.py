import pytest

from test.unit.library.fixtures.fact.fact_ring_journey import FactRingJourney


FIXTURE_PACKAGE = "test.unit.library.fixtures"


@pytest.mark.usefixtures("middle_earth")
class TestFact(object):

    @pytest.fixture
    def fact_ring_journey(self, empty_warehouse):
        return FactRingJourney(connection=empty_warehouse,
                               base_package=FIXTURE_PACKAGE)

    def test_can_build(self, fact_ring_journey):
        # when
        fact_ring_journey.build()
        # then
        assert fact_ring_journey.exists()

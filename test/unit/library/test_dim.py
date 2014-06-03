import pytest

from test.unit.library.conftest import RINGS_OF_POWER
from test.unit.library.fixtures.dim.dim_ring import DimRing


@pytest.mark.usefixtures("middle_earth")
class TestDimension(object):

    @pytest.fixture
    def ring_dimension(self, empty_warehouse, fixture_package):
        return DimRing(connection=empty_warehouse,
                       base_package=fixture_package)

    def test_can_build(self, ring_dimension):
        # when
        ring_dimension.build()
        # then
        assert ring_dimension.exists()

    def test_can_update(self, ring_dimension):
        # given
        ring_dimension.build()
        # when
        ring_dimension.update()
        # then
        assert ring_dimension.count() == len(RINGS_OF_POWER)

    def test_can_get_dictionary(self, ring_dimension):
        # given
        ring_dimension.build()
        ring_dimension.update()
        # when
        actual = ring_dimension.get_dictionary("name")
        # then
        expected = dict(zip(*reversed(zip(*RINGS_OF_POWER))))
        assert expected == actual


@pytest.mark.usefixtures("middle_earth")
class TestDimensionWithAlternativeSurrogateKeyColumn(TestDimension):

    @pytest.fixture
    def ring_dimension(self, empty_warehouse, fixture_package):
        return DimRing(connection=empty_warehouse,
                       base_package=fixture_package,
                       surrogate_key_column="pk")

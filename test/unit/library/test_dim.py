from mock import ANY
import pytest

from test.unit.library.conftest import LOCATIONS, RINGS_OF_POWER
from test.unit.library.fixtures.dim.dim_location import DimLocation
from test.unit.library.fixtures.dim.dim_ring import DimRing


@pytest.mark.usefixtures("middle_earth")
class TestDimension(object):

    key = "name"
    expected_dict = dict(zip(*reversed(zip(*RINGS_OF_POWER))))

    @pytest.fixture
    def dimension(self, empty_warehouse, fixture_package):
        return DimRing(connection=empty_warehouse,
                       base_package=fixture_package)

    def test_can_build(self, dimension):
        # when
        dimension.build()
        # then
        assert dimension.exists()

    def test_can_update(self, dimension):
        # given
        dimension.build()
        # when
        dimension.update()
        # then
        assert dimension.count() == len(self.expected_dict)

    def test_can_get_dictionary(self, dimension):
        # given
        dimension.build()
        dimension.update()
        # when
        actual_dict = dimension.get_dictionary(self.key)
        # then
        assert self.expected_dict == actual_dict


@pytest.mark.usefixtures("middle_earth")
class TestDimensionWithAlternativeSurrogateKeyColumn(TestDimension):

    @pytest.fixture
    def dimension(self, empty_warehouse, fixture_package):
        return DimRing(connection=empty_warehouse,
                       base_package=fixture_package,
                       surrogate_key_column="pk")

    def test_has_stored_surrogate_key_column(self, dimension):
        assert dimension.surrogate_key_column == "pk"


@pytest.mark.usefixtures("middle_earth")
class TestDimensionWithMarkedNaturalKeyColumn(TestDimension):

    key = "code"
    expected_dict = dict(zip(dict(LOCATIONS).keys(), [ANY] * len(LOCATIONS)))

    @pytest.fixture
    def dimension(self, empty_warehouse, fixture_package):
        return DimLocation(connection=empty_warehouse,
                           base_package=fixture_package,
                           natural_key_column=self.key)

    def test_has_stored_natural_key_column(self, dimension):
        assert dimension.natural_key_column == self.key

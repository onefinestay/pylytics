import pytest

from test.unit.library.conftest import build_ring_dimension_table, RINGS


@pytest.mark.usefixtures("warehouse", "middle_earth")
class TestDimension(object):

    def test_can_build(self, warehouse):
        # when
        dim = build_ring_dimension_table(warehouse)
        # then
        assert dim.exists()

    def test_can_update(self, warehouse):
        # given
        dim = build_ring_dimension_table(warehouse)
        # when
        dim.update()
        # then
        assert dim.count() == len(RINGS)

    def test_can_get_dictionary(self, warehouse):
        # given
        dim = build_ring_dimension_table(warehouse, update=True)
        # when
        actual = dim.get_dictionary("name")
        # then
        expected = dict(zip(*reversed(zip(*RINGS))))
        assert expected == actual


@pytest.mark.usefixtures("warehouse", "middle_earth")
class TestDimensionWithAlternativeSurrogateKeyColumn(object):

    def test_can_build(self, warehouse):
        # when
        dim = build_ring_dimension_table(warehouse, surrogate_key_column="pk")
        # then
        assert dim.exists()
        assert dim.surrogate_key_column == "pk"

    def test_can_update(self, warehouse):
        # given
        dim = build_ring_dimension_table(warehouse, surrogate_key_column="pk")
        # when
        dim.update()
        # then
        assert dim.count() == len(RINGS)

    def test_can_get_dictionary(self, warehouse):
        # given
        dim = build_ring_dimension_table(warehouse, update=True,
                                   surrogate_key_column="pk")
        # when
        actual = dim.get_dictionary("name")
        # then
        expected = dict(zip(*reversed(zip(*RINGS))))
        assert expected == actual

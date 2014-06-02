import pytest

from pylytics.library.dim import Dim
from test.helpers import db_fixture


RINGS = [
    (1, 'One Ring'),
    (2, 'Narya'),
    (3, 'Nenya'),
    (4, 'Vilna'),
]


@pytest.fixture(scope="session")
def middle_earth(request, local_mysql_credentials):
    """ Source database fixture.
    """
    return db_fixture(request, db="middle_earth", **local_mysql_credentials)


@pytest.fixture
def ring_source(request, middle_earth):
    """ Source table fixture.
    """
    middle_earth.execute("DROP TABLE IF EXISTS rings_of_power")
    middle_earth.execute("""\
    CREATE TABLE rings_of_power (
        id int,
        name varchar(40),
        primary key (id)
    ) CHARSET=utf8
    """)
    middle_earth.commit()
    middle_earth.execute("""\
    INSERT INTO rings_of_power (id, name)
    VALUES {}
    """.format(", ".join(map(repr, RINGS))))
    middle_earth.commit()


class DimRing(Dim):
    """ Example dimension class.
    """
    source_db = "middle_earth"
    source_query = "SELECT name FROM rings_of_power"


def build_ring_dimension(warehouse, update=False, surrogate_key_column="id"):
    """ Build and optionally update an example dimension.
    """
    dim = DimRing(connection=warehouse,
                  surrogate_key_column=surrogate_key_column)
    dim.drop()
    sql = """\
    CREATE TABLE dim_ring (
        {surrogate_key_column} INT AUTO_INCREMENT COMMENT 'surrogate key',
        name VARCHAR(40) COMMENT 'natural key',
        created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY ({surrogate_key_column})
    ) CHARSET=utf8
    """.format(surrogate_key_column=surrogate_key_column)
    if dim.build(sql):
        if update:
            dim.update()
        return dim
    else:
        raise Exception("Unable to build dimension")


@pytest.mark.usefixtures("warehouse")
class TestDimension(object):

    def test_can_build(self, warehouse):
        # when
        dim = build_ring_dimension(warehouse)
        # then
        assert dim.exists()

    @pytest.mark.usefixtures("ring_source")
    def test_can_update(self, warehouse):
        # given
        dim = build_ring_dimension(warehouse)
        # when
        dim.update()
        # then
        assert dim.count() == len(RINGS)

    @pytest.mark.usefixtures("ring_source")
    def test_can_get_dictionary(self, warehouse):
        # given
        dim = build_ring_dimension(warehouse, update=True)
        # when
        actual = dim.get_dictionary("name")
        # then
        expected = dict(zip(*reversed(zip(*RINGS))))
        assert expected == actual


@pytest.mark.usefixtures("warehouse")
class TestDimensionWithAlternativeSurrogateKeyColumn(object):

    def test_can_build(self, warehouse):
        # when
        dim = build_ring_dimension(warehouse, surrogate_key_column="pk")
        # then
        assert dim.exists()
        assert dim.surrogate_key_column == "pk"

    @pytest.mark.usefixtures("ring_source")
    def test_can_update(self, warehouse):
        # given
        dim = build_ring_dimension(warehouse, surrogate_key_column="pk")
        # when
        dim.update()
        # then
        assert dim.count() == len(RINGS)

    @pytest.mark.usefixtures("ring_source")
    def test_can_get_dictionary(self, warehouse):
        # given
        dim = build_ring_dimension(warehouse, update=True,
                                   surrogate_key_column="pk")
        # when
        actual = dim.get_dictionary("name")
        # then
        expected = dict(zip(*reversed(zip(*RINGS))))
        assert expected == actual

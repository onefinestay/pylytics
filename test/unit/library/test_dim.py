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
    source_query = "SELECT name FROM rings_of_power ORDER BY id"


@pytest.fixture
def dim_ring(warehouse):
    """ Example dimension fixture.
    """
    dim = DimRing(connection=warehouse)
    dim.drop()
    sql = """\
    CREATE TABLE dim_ring (
        id INT AUTO_INCREMENT COMMENT 'surrogate key',
        name VARCHAR(40) COMMENT 'natural key',
        created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    ) CHARSET=utf8
    """
    if dim.build(sql):
        return dim
    else:
        raise Exception("Unable to build dimension")


@pytest.mark.usefixtures("warehouse")
class TestDimension(object):

    def test_can_build_dimension(self, dim_ring):
        assert dim_ring.exists()

    @pytest.mark.usefixtures("ring_source")
    def test_can_update_dimension(self, dim_ring):
        dim_ring.update()
        assert dim_ring.count() == len(RINGS)

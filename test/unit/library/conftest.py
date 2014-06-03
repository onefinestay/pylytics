import pytest

from test.helpers import db_fixture


BASE_PACKAGE = "test.unit.library.fixtures"

LOCATIONS = [
    ("HOB", "Hobbiton"),
    ("RIV", "Rivendell"),
    ("MRA", "Moria"),
    ("MDR", "Mordor"),
    ("MTD", "Mount Doom"),
]

# Please note that if this variable name is ever spoken
# aloud, a deep, booming voice must be used.
RINGS_OF_POWER = [
    (1, 'One Ring'),
    (2, 'Narya'),
    (3, 'Nenya'),
    (4, 'Vilna'),
]


@pytest.fixture(scope="session")
def fixture_package():
    return "test.unit.library.fixtures"


@pytest.fixture(scope="session")
def middle_earth(request, local_mysql_credentials):
    """ Source database fixture.
    """
    db = db_fixture(request, db="middle_earth", **local_mysql_credentials)

    # Rings
    db.execute("DROP TABLE IF EXISTS rings_of_power")
    db.execute("""\
    CREATE TABLE rings_of_power (
        id int,
        name varchar(40),
        primary key (id)
    ) CHARSET=utf8
    """)
    db.commit()
    db.execute("""\
    INSERT INTO rings_of_power (id, name)
    VALUES {}
    """.format(", ".join(map(repr, RINGS_OF_POWER))))
    db.commit()

    # Locations
    db.execute("DROP TABLE IF EXISTS locations")
    db.execute("""\
    CREATE TABLE locations (
        code char(3),
        name varchar(40),
        primary key (code)
    ) CHARSET=utf8
    """)
    db.commit()
    db.execute("""\
    INSERT INTO locations (code, name)
    VALUES {}
    """.format(", ".join(map(repr, LOCATIONS))))
    db.commit()

    # Journey
    db.execute("DROP TABLE IF EXISTS ring_journey")
    db.execute("""\
    CREATE TABLE ring_journey (
        id int,
        ring_name varchar(40),
        checkpoint varchar(3),
        primary key (id)
    ) CHARSET=utf8
    """)
    db.commit()
    db.execute("""\
    INSERT INTO ring_journey (id, ring_name, checkpoint)
    VALUES (1, 'One Ring', 'HOB'),
           (2, 'One Ring', 'RIV'),
           (3, 'One Ring', 'MRA'),
           (4, 'One Ring', 'MDR'),
           (5, 'One Ring', 'MTD')
    """)
    db.commit()

    return db

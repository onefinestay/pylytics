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
#
#
# def build_ring_dimension_table(warehouse, update=False,
#                                surrogate_key_column="id"):
#     """ Build and optionally update an example dimension table.
#     """
#     dim = DimRing(connection=warehouse,
#                   surrogate_key_column=surrogate_key_column)
#     dim.drop()
#     sql = """\
#     CREATE TABLE dim_ring (
#         {surrogate_key_column} INT AUTO_INCREMENT COMMENT 'surrogate key',
#         name VARCHAR(40) COMMENT 'natural key',
#         created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
#                                  ON UPDATE CURRENT_TIMESTAMP,
#         PRIMARY KEY ({surrogate_key_column})
#     ) CHARSET=utf8
#     """.format(surrogate_key_column=surrogate_key_column)
#     if dim.build(sql):
#         if update:
#             dim.update()
#         return dim
#     else:
#         raise Exception("Unable to build dimension table")
#
#
# def build_location_dimension_table(warehouse, update=False,
#                                    surrogate_key_column="id"):
#     """ Build and optionally update an example dimension table.
#     """
#     dim = DimLocation(connection=warehouse,
#                       surrogate_key_column=surrogate_key_column)
#     dim.drop()
#     sql = """\
#     CREATE TABLE dim_locations (
#         {surrogate_key_column} INT AUTO_INCREMENT COMMENT 'surrogate key',
#         code CHAR(3) COMMENT 'natural key',
#         name VARCHAR(40),
#         created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
#                                  ON UPDATE CURRENT_TIMESTAMP,
#         PRIMARY KEY ({surrogate_key_column})
#     ) CHARSET=utf8
#     """.format(surrogate_key_column=surrogate_key_column)
#     if dim.build(sql):
#         if update:
#             dim.update()
#         return dim
#     else:
#         raise Exception("Unable to build dimension table")
#
#
# def build_ring_journey_fact_table(warehouse, update=False):
#     """ Build an example fact table.
#     """
#     # build_ring_dimension_table(warehouse, update=update)
#     # build_location_dimension_table(warehouse, update=update)
#     fact = FactRingJourney(connection=warehouse, base_package=BASE_PACKAGE)
#     fact.drop()
#     if fact.build():
#         if update:
#             fact.update()
#         return fact
#     else:
#         raise Exception("Unable to build fact table")

from mysql.connector.errors import OperationalError
import pytest

from pylytics.library.collection import CREATE_STAGING_TABLE

from test.helpers import db_fixture, execute


@pytest.fixture(scope="session")
def warehouse():
    return db_fixture("test_warehouse")


@pytest.fixture
def empty_warehouse(warehouse):
    cursor = warehouse.cursor()
    cursor.execute("SHOW TABLES")
    tables = [_[0] for _ in cursor]
    cursor.close()

    execute(warehouse, "SET foreign_key_checks = 0")

    for table in tables:
        try:
            execute(warehouse, "DROP TABLE {}".format(table))
        except OperationalError:
            execute(warehouse, "DROP VIEW {}".format(table))
    execute(warehouse, "SET foreign_key_checks = 1")
    warehouse.commit()
    return warehouse


@pytest.fixture
def staging(warehouse):
    execute(warehouse, "DROP TABLE IF EXISTS staging")
    execute(warehouse, CREATE_STAGING_TABLE)
    warehouse.commit()

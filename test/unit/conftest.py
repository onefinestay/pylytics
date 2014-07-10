from MySQLdb import OperationalError
import pytest

from pylytics.library.collection import CREATE_STAGING_TABLE

from test.helpers import db_fixture


@pytest.fixture(scope="session")
def warehouse():
    return db_fixture("test_warehouse")


@pytest.fixture
def empty_warehouse(warehouse):
    tables = [_[0] for _ in warehouse.execute("SHOW TABLES")]
    warehouse.execute("SET foreign_key_checks = 0")
    for table in tables:
        try:
            warehouse.execute("DROP TABLE {}".format(table))
        except OperationalError:
            warehouse.execute("DROP VIEW {}".format(table))
    warehouse.execute("SET foreign_key_checks = 1")
    warehouse.commit()
    return warehouse


@pytest.fixture
def staging(warehouse):
    warehouse.execute("DROP TABLE IF EXISTS staging")
    warehouse.execute(CREATE_STAGING_TABLE)
    warehouse.commit()

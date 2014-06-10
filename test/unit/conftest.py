import pytest

from pylytics.library.collection import CREATE_STAGING_TABLE

from test.helpers import db_fixture


@pytest.fixture(scope="session")
def warehouse():
    return db_fixture("test_warehouse")


@pytest.fixture
def empty_warehouse(warehouse):
    tables = [_[0] for _ in warehouse.execute("SHOW TABLES")]
    for prefix in ("fact_", "dim_"):
        for table in tables:
            if table.startswith(prefix):
                warehouse.execute("DROP TABLE {}".format(table))
    return warehouse


@pytest.fixture
def staging(warehouse):
    warehouse.execute("DROP TABLE IF EXISTS staging")
    warehouse.execute(CREATE_STAGING_TABLE)
    warehouse.commit()

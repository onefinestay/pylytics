import pytest

from test.helpers import db_fixture


@pytest.fixture(scope="session")
def test_settings():
    """ Loads settings for tests from a module or, if not available, falls back
    to default settings.

    The settings module is expected to reside in test/settings.py and should
    contain and export a dictionary named `test_settings`. For an example, see
    the test/settings.py.example file. Since these settings are environment
    specific, the test/settings.py file itself should not be stored in the code
    repository.

    """
    try:
        from test.settings import test_settings
    except ImportError:
        test_settings = {
            "local_mysql_credentials": {
                "user": "root",
                "passwd": "",
            }
        }
    return test_settings


@pytest.fixture(scope="session")
def local_mysql_credentials(test_settings):
    return test_settings["local_mysql_credentials"]


@pytest.fixture(scope="session")
def warehouse(request, local_mysql_credentials):
    return db_fixture(request, db="test_warehouse", **local_mysql_credentials)


@pytest.fixture
def empty_warehouse(warehouse):
    tables = [_[0] for _ in warehouse.execute("SHOW TABLES")]
    for prefix in ("fact_", "dim_"):
        for table in tables:
            if table.startswith(prefix):
                warehouse.execute("DROP TABLE {}".format(table))
    return warehouse

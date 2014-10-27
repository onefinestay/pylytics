from mysql.connector.errors import OperationalError
import pytest

from test.helpers import db_fixture, execute


CREATE_STAGING_TABLE = """\
CREATE TABLE `staging` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `collector_type` varchar(127) NOT NULL,
  `fact_table` varchar(255) NOT NULL,
  `value_map` text NOT NULL,
  `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `collector_type` (`collector_type`),
  KEY `fact_table` (`fact_table`),
  KEY `created` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""


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

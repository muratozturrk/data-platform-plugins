import re

import pytest

from common.lakehouse import get_pyiceberg_catalog, get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("lakehouse-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def catalog():
    return get_pyiceberg_catalog()


@pytest.fixture(scope="module")
def ns(spark, request):
    """One isolated namespace per test module; tables are purged afterwards."""
    name = re.sub(r"\W", "_", request.module.__name__.split(".")[-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    yield name
    for row in spark.sql(f"SHOW TABLES IN {name}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {name}.{row.tableName} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {name}")

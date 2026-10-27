import re

import pytest

from common.lakehouse import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("lakehouse-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def ns(spark, request):
    """One isolated namespace per test module, dropped afterwards.

    No PURGE: Nessie sets gc.enabled=false on its tables (files may be shared by other branches),
    so Iceberg refuses to purge. The files go away with `docker compose down -v`.
    """
    name = re.sub(r"\W", "_", request.module.__name__.split(".")[-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    yield name
    for row in spark.sql(f"SHOW TABLES IN {name}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {name}.{row.tableName}")
    spark.sql(f"DROP NAMESPACE IF EXISTS {name}")

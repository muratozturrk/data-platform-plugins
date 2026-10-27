import re

import pytest

from common.history import build_history
from common.lakehouse import get_spark

SHARED_NS = "metadata_shared"


@pytest.fixture(scope="session")
def spark():
    session = get_spark("metadata-tables-tests")
    session.sparkContext.setLogLevel("ERROR")
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SHARED_NS}")
    yield session
    for row in session.sql(f"SHOW TABLES IN {SHARED_NS}").collect():
        session.sql(f"DROP TABLE IF EXISTS {SHARED_NS}.{row.tableName} PURGE")
    session.sql(f"DROP NAMESPACE IF EXISTS {SHARED_NS}")
    session.stop()


@pytest.fixture(scope="session")
def hist(spark):
    """The rich v2 history (see common/history.py). Read-only: tests must not change it."""
    return build_history(spark, f"{SHARED_NS}.orders", format_version=2)


@pytest.fixture(scope="session")
def hist_v3(spark):
    """Same history on a v3 table: deletes are deletion vectors in a Puffin file."""
    return build_history(spark, f"{SHARED_NS}.orders_v3", format_version=3)


@pytest.fixture(scope="module")
def ns(spark, request):
    """One namespace per test module for tables a test may change."""
    name = re.sub(r"\W", "_", request.module.__name__.split(".")[-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    yield name
    for row in spark.sql(f"SHOW TABLES IN {name}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {name}.{row.tableName} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {name}")

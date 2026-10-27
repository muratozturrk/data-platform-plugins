import re

import pytest

from common.lakehouse import get_spark, pg_connect
from common.legacy_platform import run_workload, seed_legacy_lake
from platform_audit.audit import run_audit


@pytest.fixture(scope="session")
def pg():
    conn = pg_connect()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def lake_rows():
    """Ground truth: rows per legacy-lake dataset, as written by the seeder."""
    return seed_legacy_lake()


@pytest.fixture(scope="session")
def audit_report(pg, lake_rows):
    """The audit every lab builds on: seed the lake, replay the workload, audit both systems."""
    run_workload(pg)
    return run_audit(pg)


@pytest.fixture(scope="session")
def spark():
    session = get_spark("migration-planning-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def ns(spark, request):
    """One isolated namespace per test module; tables are dropped afterwards."""
    name = re.sub(r"\W", "_", request.module.__name__.split(".")[-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    yield name
    for row in spark.sql(f"SHOW TABLES IN {name}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {name}.{row.tableName}")   # no PURGE: some tables reference legacy files
    spark.sql(f"DROP NAMESPACE IF EXISTS {name}")

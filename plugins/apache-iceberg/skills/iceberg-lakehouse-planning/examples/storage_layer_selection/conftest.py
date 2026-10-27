import re
import uuid

import pytest

from common.lakehouse import get_spark
from common.s3 import ensure_bucket

_MEASUREMENTS: list[tuple[str, str, object]] = []


@pytest.fixture(scope="session")
def spark():
    # The RustFS REST catalog writes to s3://warehouse/ on RustFS: the bucket must exist first.
    ensure_bucket("rustfs", "warehouse")
    session = get_spark("storage-selection-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def ns(spark, request):
    """One namespace per test module in the `lake` catalog; tables are purged afterwards."""
    name = re.sub(r"\W", "_", request.module.__name__.split(".")[-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{name}")
    yield name
    for row in spark.sql(f"SHOW TABLES IN lake.{name}").collect():
        spark.sql(f"DROP TABLE IF EXISTS lake.{name}.{row.tableName} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS lake.{name}")


@pytest.fixture
def uid() -> str:
    """Short unique suffix, so reruns against a live stack never collide."""
    return uuid.uuid4().hex[:6]


@pytest.fixture
def measure(request):
    """Record a measured value; all values are printed in the pytest terminal summary."""
    def _record(key: str, value):
        _MEASUREMENTS.append((request.node.name, key, value))
    return _record


def pytest_terminal_summary(terminalreporter):
    if _MEASUREMENTS:
        terminalreporter.section("measurements (local stack, not benchmarks)")
        for test, key, value in _MEASUREMENTS:
            terminalreporter.write_line(f"{test} | {key} = {value}")

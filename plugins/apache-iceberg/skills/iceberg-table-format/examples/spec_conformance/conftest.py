import re

import pytest

from common.lakehouse import get_pyiceberg_catalog, get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("iceberg-spec-tests")
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


_MEASUREMENTS: list[tuple[str, str, object]] = []


@pytest.fixture
def measure(request):
    """Record a measured value; all values are printed in the pytest terminal summary."""
    def _record(key: str, value):
        _MEASUREMENTS.append((request.node.name, key, value))
    return _record


def pytest_terminal_summary(terminalreporter):
    if not _MEASUREMENTS:
        return
    terminalreporter.section("measurements")
    for test, key, value in _MEASUREMENTS:
        terminalreporter.write_line(f"{test} | {key} = {value}")

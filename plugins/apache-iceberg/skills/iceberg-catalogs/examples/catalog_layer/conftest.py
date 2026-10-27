import re
import uuid

import pytest

from common.lakehouse import SPARK_POLARIS_CATALOG

_MEASUREMENTS: list[tuple[str, str, object]] = []


@pytest.fixture(scope="session")
def spark():
    from common.lakehouse import get_spark
    from common.polaris import PolarisAdmin
    PolarisAdmin().ensure_catalog(SPARK_POLARIS_CATALOG)
    session = get_spark("lakehouse-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def polaris_admin():
    from common.polaris import PolarisAdmin
    return PolarisAdmin()


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
    if not _MEASUREMENTS:
        return
    terminalreporter.section("measurements")
    for test, key, value in _MEASUREMENTS:
        terminalreporter.write_line(f"{test} | {key} = {value}")


def safe_name(text: str) -> str:
    return re.sub(r"\W", "_", text).lower()

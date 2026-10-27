import json
from pathlib import Path

import pytest

from common.seed import seed_lakehouse

MEASUREMENTS = Path(__file__).parent / "measurements.json"
_measured: dict[str, dict] = {}


@pytest.fixture(scope="session", autouse=True)
def lakehouse():
    """Iceberg facts (15,000 TPC-H tiny orders) exist before any lab runs."""
    return seed_lakehouse()


@pytest.fixture
def record(request):
    """Store measured values per test; written to measurements.json for later review."""
    def _record(**values):
        _measured.setdefault(request.node.name, {}).update(
            {k: (str(v) if not isinstance(v, (int, float, str, bool, list, dict, type(None))) else v)
             for k, v in values.items()})
    return _record


def pytest_sessionfinish(session, exitstatus):
    if _measured:
        MEASUREMENTS.write_text(json.dumps(_measured, indent=2, sort_keys=True, default=str))

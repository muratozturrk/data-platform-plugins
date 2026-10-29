import json
import pathlib
import uuid

import pytest

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"


@pytest.fixture
def uid() -> str:
    return uuid.uuid4().hex[:6]


@pytest.fixture(scope="session")
def measure():
    data: dict = {}

    def _m(key: str, value) -> None:
        data[key] = value
    yield _m
    MEASUREMENTS.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


@pytest.fixture(scope="session")
def example_rows(measure):
    """Rows of the example check table: (example, server, status, verdict, fix)."""
    rows: list[dict] = []
    yield rows
    measure("example_check", rows)

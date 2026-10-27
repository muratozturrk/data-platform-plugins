import re

import pytest

from common.lakehouse import get_catalog, reset_namespace


@pytest.fixture(scope="session")
def catalog():
    return get_catalog()


@pytest.fixture(scope="module")
def ns(catalog, request):
    """One empty namespace per test module (tables purged before, kept after for inspection)."""
    name = re.sub(r"\W", "_", request.module.__name__.split(".")[-1])
    reset_namespace(catalog, name)
    return name

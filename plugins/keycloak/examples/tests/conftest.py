import importlib.util
import pathlib
import sys

import pytest

EXAMPLES = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"


def load(name):
    path = EXAMPLES / "automation" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def realm_audit():
    return load("realm_audit")


@pytest.fixture(scope="session")
def claims():
    return load("claims_contract_check")


@pytest.fixture(scope="session")
def tenant():
    return load("tenant_to_tfvars")


@pytest.fixture
def fixtures():
    return FIXTURES

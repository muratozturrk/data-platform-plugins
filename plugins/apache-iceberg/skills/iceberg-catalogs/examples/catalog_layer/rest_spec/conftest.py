import pytest

from common.iceberg_http import IcebergRest
from common.lakehouse import ROOT_CLIENT

POLARIS_CATALOG = "rest_spec"


@pytest.fixture(scope="module")
def _polaris_catalog(polaris_admin):
    polaris_admin.ensure_catalog(POLARIS_CATALOG)
    yield POLARIS_CATALOG
    polaris_admin.drop_catalog_cascade(POLARIS_CATALOG)


def make_client(server: str) -> IcebergRest:
    if server == "reference":
        return IcebergRest.reference()
    return IcebergRest.polaris(POLARIS_CATALOG, *ROOT_CLIENT)


@pytest.fixture(params=["reference", "polaris"])
def server(request, _polaris_catalog):
    """The same test body runs against two independent REST catalog implementations."""
    return request.param


@pytest.fixture
def ns(server, uid):
    client = make_client(server)
    name = f"rest_spec_{uid}"
    client.create_namespace(name)
    yield name
    for t in client.list_tables(name):
        client.drop_table(name, t, purge=True)
    for child in client.list_namespaces(parent=name):
        for t in client.list_tables(child):
            client.drop_table(child, t, purge=True)
        client.drop_namespace(child)
    client.drop_namespace(name)

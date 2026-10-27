"""One business definition, many tools and interfaces."""
from decimal import Decimal

import pytest

from common.federation import run
from federated_queries.federated import iceberg_catalog
from federation_requirements import rest_client
from federation_requirements.semantic_layer import (
    CUSTOMER_360, NET_REVENUE_V1, NET_REVENUE_V2, catalog_of_definitions, deploy, deploy_all)

TOTAL_SQL = "SELECT sum(net_revenue_usd) FROM lake.semantic.net_revenue_daily"


@pytest.fixture(scope="module", autouse=True)
def semantic_layer():
    deploy_all()
    yield
    deploy_all()       # leave v1 in place for later labs


def test_same_definition_same_answer_in_every_tool(record):
    bi = run(TOTAL_SQL, user="bi_dashboard", source="superset").rows[0][0]       # DB-API (BI tool)
    notebook = run(TOTAL_SQL, user="analyst_eu").rows[0][0]                       # notebook
    app = rest_client.execute(TOTAL_SQL, user="analyst_eu")                       # REST protocol
    assert bi == notebook == Decimal(app.rows[0][0])
    # The definition really applies the business rules (returns excluded, FX converted).
    raw = run("SELECT sum(amount) FROM lake.sales.orders").rows[0][0]
    assert bi != raw
    record(net_revenue_usd=str(bi), raw_amount=str(raw), rest_http_requests=app.http_requests)


def test_central_change_propagates_to_all_consumers(record):
    v1 = run(TOTAL_SQL, user="bi_dashboard").rows[0][0]
    deploy(NET_REVENUE_V2)
    v2_bi = run(TOTAL_SQL, user="bi_dashboard").rows[0][0]
    v2_app = Decimal(rest_client.execute(TOTAL_SQL, user="analyst_eu").rows[0][0])
    pending = run("SELECT sum(o.amount * f.rate_to_usd) FROM lake.sales.orders o "
                  "JOIN pg.public.fx_rates f ON f.currency = o.currency "
                  "LEFT JOIN pg.public.order_returns r ON r.order_id = o.order_id "
                  "WHERE r.order_id IS NULL AND o.status = 'P'").rows[0][0]
    assert v2_bi == v2_app == v1 - pending
    assert "v2" in catalog_of_definitions()["net_revenue_daily"]
    record(v1=str(v1), v2=str(v2_bi), pending_removed=str(pending))
    deploy(NET_REVENUE_V1)


def test_definitions_are_discoverable_and_portable(record):
    """Comments make the layer browsable; the views live in the Iceberg REST catalog,
    so other Iceberg engines see them too (here: PyIceberg)."""
    defs = catalog_of_definitions(user="analyst_eu")
    assert defs["net_revenue_daily"] == NET_REVENUE_V1.comment
    assert defs["customer_360"] == CUSTOMER_360.comment
    views = {name for _, name in iceberg_catalog().list_views("semantic")}
    assert {"net_revenue_daily", "customer_360"} <= views
    record(definitions=sorted(defs), iceberg_views=sorted(views))


def test_rest_protocol_and_dbapi_return_identical_rows(record):
    sql = "SELECT region, count(*) AS customers FROM lake.semantic.customer_360 GROUP BY region ORDER BY region"
    dbapi = run(sql, user="bi_dashboard").rows
    rest = rest_client.execute(sql, user="bi_dashboard")
    assert rest.columns == ["region", "customers"]
    assert [list(r) for r in dbapi] == rest.rows
    record(rows=len(rest.rows), http_requests=rest.http_requests)

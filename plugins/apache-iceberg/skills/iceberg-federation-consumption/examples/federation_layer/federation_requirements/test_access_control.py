"""Governed access. Rules live in trino/rules.json (file-based access control)."""
import pytest
from trino.exceptions import TrinoUserError

from common.federation import run
from federation_requirements.semantic_layer import deploy_all

C360 = "lake.semantic.customer_360"


@pytest.fixture(scope="module", autouse=True)
def semantic_layer():
    deploy_all()


def test_row_filter_and_column_mask_apply_per_user(record):
    eu = run(f"SELECT region, email FROM {C360}", user="analyst_eu").rows
    bi = run(f"SELECT region, email FROM {C360}", user="bi_dashboard").rows
    assert {r[0] for r in eu} == {"EUROPE"} and len(eu) == 300
    assert all(r[1].startswith("***@") for r in eu)
    assert len(bi) == 1500 and not any(r[1].startswith("***") for r in bi)
    record(analyst_eu_rows=len(eu), bi_rows=len(bi), analyst_email_sample=eu[0][1])


def test_definer_view_gives_access_without_exposing_sources():
    """analyst_eu reads the governed view, but not the tables it is built on."""
    assert run("SELECT count(*) FROM lake.semantic.net_revenue_daily", user="analyst_eu").rows[0][0] > 0


@pytest.mark.parametrize("sql, message", [
    ("SELECT count(*) FROM pg.public.customers", "Cannot access catalog pg"),
    ("SELECT count(*) FROM lake.sales.orders", "Cannot select from table lake.sales.orders"),
    ("SELECT * FROM TABLE(pg.system.query(query => 'SELECT email FROM customers'))",
     "Cannot execute function pg.system.query"),
    ("CREATE VIEW lake.semantic.my_metric AS SELECT 1 AS x", "Cannot create view"),
])
def test_analyst_is_denied_raw_and_write_access(sql, message):
    with pytest.raises(TrinoUserError) as err:
        run(sql, user="analyst_eu")
    assert err.value.error_name == "PERMISSION_DENIED"
    assert message in err.value.message

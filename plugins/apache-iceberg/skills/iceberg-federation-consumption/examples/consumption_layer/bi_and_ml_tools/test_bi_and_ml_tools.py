"""BI dashboards and ML jobs on one semantic layer, with governance at the SQL endpoint."""
import duckdb
import pytest
from trino.exceptions import TrinoUserError

from common.lakehouse import CATALOG, TRINO_CATALOG, add_spark_catalog, trino_query
from common.sales import MERCHANTS, day, load_sales, sales
from common.semantic import GOVERNED_VIEW, compile_query, drop_semantic_schema, publish_governed_view
from bi_and_ml_tools.bi import SALES_DASHBOARD, render_dbapi_style, render_superset_style, same_dashboard
from bi_and_ml_tools.ml import extract_training_set, fingerprint, train_linear

ROWS = 200_000
KPIS = ["net_revenue", "gross_revenue", "completed_orders", "avg_order_value", "refund_rate"]


@pytest.fixture(scope="module")
def table(spark, ns):
    t = f"{ns}.sales"
    load_sales(spark, t, ROWS)
    return t


@pytest.fixture(scope="module")
def semantic(ns, table):
    schema = f"{ns}_semantic"
    publish_governed_view(table, schema)
    yield schema
    drop_semantic_schema(schema)


def rows_close(a, b) -> bool:
    return same_dashboard({"t": a}, {"t": b})


def test_one_semantic_model_gives_the_same_kpis_on_three_engines(spark, catalog, table):
    trino = trino_query(compile_query(KPIS, ["region"], table))
    sparks = [tuple(r) for r in spark.sql(compile_query(KPIS, ["region"], table)).collect()]
    con = duckdb.connect()
    con.register("sales", catalog.load_table(table).scan().to_arrow())
    duck = con.execute(compile_query(KPIS, ["region"], "sales")).fetchall()
    assert rows_close(trino, sparks) and rows_close(trino, duck)
    assert len(trino) == 4


def test_two_bi_clients_render_the_same_governed_dashboard(semantic):
    superset = render_superset_style("analyst", semantic)
    dbapi = render_dbapi_style("analyst", semantic)
    assert same_dashboard(superset, dbapi)
    assert len(superset["Revenue by region"]) == 4 and len(superset["Category mix"]) == 6
    assert len(superset["KPI header"]) == 1


def test_merchants_see_only_their_rows_and_never_the_raw_table(table, semantic):
    tile = [t for t in SALES_DASHBOARD if t.title == "KPI header"]
    total = render_dbapi_style("admin", semantic, tile)["KPI header"][0][0]
    per_merchant = {}
    for m in MERCHANTS:
        rows = trino_query(f"SELECT DISTINCT merchant_id FROM {semantic}.{GOVERNED_VIEW}", user=m)
        assert rows == [(m,)]
        per_merchant[m] = render_dbapi_style(m, semantic, tile)["KPI header"][0][0]
        with pytest.raises(TrinoUserError, match="Access Denied"):
            trino_query(f"SELECT count(*) FROM {table}", user=m)
    print({m: round(v, 2) for m, v in per_merchant.items()}, round(total, 2))
    assert abs(sum(per_merchant.values()) - total) < 1e-6 * total


def test_analyst_sees_masked_pii_admin_sees_raw(semantic):
    sql = f"SELECT customer_email FROM {semantic}.{GOVERNED_VIEW} ORDER BY order_id LIMIT 3"
    assert trino_query(sql, user="analyst") == [("***",)] * 3
    assert all(e.endswith("@example.com") for (e,) in trino_query(sql, user="admin"))


def test_engine_level_rules_do_not_follow_the_data(spark, semantic):
    """The view is an Iceberg view in the REST catalog: Spark can read it, but Trino's rules stay in Trino."""
    add_spark_catalog(spark, TRINO_CATALOG)            # the view SQL references Trino's catalog name
    via_spark = spark.sql(f"SELECT count(DISTINCT merchant_id) n FROM {CATALOG}.{semantic}.{GOVERNED_VIEW}").first().n
    via_trino = trino_query(f"SELECT count(DISTINCT merchant_id) FROM {semantic}.{GOVERNED_VIEW}", user="m01")[0][0]
    assert (via_spark, via_trino) == (5, 1)


def test_dashboard_is_unchanged_after_the_physical_layout_changes(spark, table, semantic):
    before = render_superset_style("analyst", semantic)
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(4, merchant_id)")
    spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', strategy => 'sort', "
              "sort_order => 'merchant_id ASC NULLS LAST', options => map('rewrite-all', 'true'))")
    files = spark.sql(f"SELECT count(*) c FROM {table}.files").first().c
    after = render_superset_style("analyst", semantic)
    assert same_dashboard(before, after, tol=1e-9)
    assert files > 30                                   # 30 days x up to 4 buckets, no view or tile changed


def test_training_set_is_reproducible_at_a_snapshot(spark, catalog, table):
    features, pinned = extract_training_set(catalog, table)
    coef = train_linear(features)
    sales(spark, 20_000, start_id=ROWS, start=day(30), days=1).writeTo(table).append()   # ingestion continues

    again, _ = extract_training_set(catalog, table, snapshot_id=pinned)
    latest, latest_id = extract_training_set(catalog, table)
    assert fingerprint(again) == fingerprint(features) and (train_linear(again) == coef).all()
    assert latest_id != pinned and fingerprint(latest) != fingerprint(features)
    assert features.num_rows == 20_000 and features.schema.field("spend").type.bit_width == 64

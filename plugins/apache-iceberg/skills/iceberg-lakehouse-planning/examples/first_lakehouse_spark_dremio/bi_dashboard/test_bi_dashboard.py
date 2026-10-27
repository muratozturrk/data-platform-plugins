"""The 'Sales Analytics' dashboard data layer, checked against the source system."""
import os
from datetime import date

import pytest
import requests

from common.lakehouse import NESSIE_URI
from spark_ingestion.ingest import full_reload, read_source
from dremio_queries.dremio_lab import SOURCE, connect_dremio
from bi_dashboard.semantic import CHARTS, VIEWS, create_views, render, run_dashboard

OUT_DIR = os.path.join(os.path.dirname(__file__), "output")


@pytest.fixture(scope="module")
def dremio():
    return connect_dremio()


@pytest.fixture(scope="module")
def dataset(spark, ns):
    return f"{SOURCE}.{full_reload(spark, ns)}"


@pytest.fixture(scope="module")
def truth(spark):
    """Ground truth computed by PostgreSQL itself (the query is pushed down over JDBC)."""
    def q(sql):
        return [tuple(r) for r in read_source(spark, f"({sql}) AS q").collect()]
    return {
        "Sales by Day": q("SELECT sales_date, sum(sales_amount) FROM fashion_sales GROUP BY 1 ORDER BY 1"),
        "Sales by Category": q("SELECT category, sum(sales_amount) FROM fashion_sales GROUP BY 1 ORDER BY 1"),
        "regions": q("SELECT store_location, count(*), sum(sales_amount) FROM fashion_sales GROUP BY 1 ORDER BY 1"),
    }


@pytest.fixture(scope="module")
def results(dremio, dataset):
    return run_dashboard(dataset)


def test_chart_queries_match_the_source_database(results, truth):
    for chart in CHARTS:
        t = results[chart.name]
        got = list(zip(t.column(chart.dimension).to_pylist(), t.column("metric").to_pylist()))
        assert got == truth[chart.name], chart.name                  # exact DECIMAL(38,2) equality


def test_sales_by_day_covers_every_day_once(results):
    days = results["Sales by Day"].column("sales_date").to_pylist()
    assert days[0] == date(2024, 3, 1) and days[-1] == date(2024, 5, 31)
    assert len(days) == len(set(days)) == 92


def test_sales_by_category_is_a_valid_pie(results):
    t = results["Sales by Category"]
    assert t.column("category").to_pylist() == ["Bottoms", "Denim", "Dresses", "Footwear", "Outerwear", "Tops"]
    revenue = [float(v) for v in t.column("metric").to_pylist()]
    assert all(v > 0 for v in revenue)
    assert sum(v / sum(revenue) for v in revenue) == pytest.approx(1.0)


@pytest.fixture(scope="module")
def views(dremio, dataset):
    created = create_views(dremio, dataset, dataset.rsplit(".", 1)[0])
    yield created
    for view in created:                    # Spark's SHOW TABLES does not list views: drop them here
        dremio.sql(f"DROP VIEW {view}")


def test_curated_views_live_in_the_catalog_and_answer_from_iceberg(dremio, dataset, views, truth):
    schema = dataset.rsplit(".", 1)[0]
    assert views == [f"{schema}.{name}" for name in VIEWS]
    ns = schema.split(".")[1]
    entries = requests.get(f"{NESSIE_URI}/trees/main/entries").json()["entries"]
    kinds = {".".join(e["name"]["elements"]): e["type"] for e in entries}
    assert all(kinds[f"{ns}.{name}"] == "ICEBERG_VIEW" for name in VIEWS)   # stored in Nessie, not in Dremio
    region = dremio.sql(f"SELECT region, orders, revenue FROM {schema}.sales_by_region ORDER BY region")
    assert [(r["region"], r["orders"], float(r["revenue"])) for r in region] == \
        [(a, b, float(c)) for a, b, c in truth["regions"]]
    campaign = dremio.sql(f"SELECT sum(orders) AS n FROM {schema}.campaign_performance")
    assert campaign[0]["n"] == 3000


def test_dashboard_renders_to_a_static_png(results):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = render(results, os.path.join(OUT_DIR, "sales_analytics.png"))
    with open(path, "rb") as f:
        assert f.read(8) == b"\x89PNG\r\n\x1a\n"
    assert os.path.getsize(path) > 20_000

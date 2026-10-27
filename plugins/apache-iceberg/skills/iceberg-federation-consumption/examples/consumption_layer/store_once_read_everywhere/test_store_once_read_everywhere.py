"""Store once in Iceberg, consume from every tool, no copies, no drift."""
import pytest

from common.sales import day, load_sales, sales
from store_once_read_everywhere.readers import (
    export_departmental_copy, kpis_equal, read_everywhere, team_revenues, via_departmental_copy, via_spark)

ROWS = 300_000


@pytest.fixture(scope="module")
def table(spark, ns):
    t = f"{ns}.sales"
    load_sales(spark, t, ROWS)
    return t


def current_snapshot(spark, t) -> int:
    return spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id


def append_new_day(spark, t, n=10_000, start_id=ROWS):
    sales(spark, n, start_id=start_id, start=day(30), days=1).writeTo(t).append()


def test_six_consumers_get_identical_kpis(spark, catalog, table):
    results = read_everywhere(spark, catalog, table)
    reference = results["spark_sql"]
    assert set(reference) == {"NA", "EU", "APAC", "LATAM"}
    for engine, kpis in results.items():
        assert kpis_equal(kpis, reference), engine
    completed = spark.sql(f"SELECT count(*) c FROM {table} WHERE status = 'completed'").first().c
    assert sum(n for _, n in reference.values()) == completed


def test_engines_pinned_to_one_snapshot_agree_while_the_table_moves(spark, catalog, table):
    pinned = current_snapshot(spark, table)
    before = via_spark(spark, table)
    append_new_day(spark, table)

    at_pin = read_everywhere(spark, catalog, table, snapshot_id=pinned)
    latest = read_everywhere(spark, catalog, table)
    for engine in at_pin:
        assert kpis_equal(at_pin[engine], before), engine            # reproducible: old snapshot unchanged
        assert kpis_equal(latest[engine], latest["spark_sql"]), engine
        assert not kpis_equal(latest[engine], before), engine         # every engine sees the new commit


def test_departmental_copy_goes_stale_lakehouse_readers_do_not(spark, catalog, table, tmp_path):
    copy = str(tmp_path / "marketing_extract.parquet")
    copied_rows = export_departmental_copy(catalog, table, copy)
    assert kpis_equal(via_departmental_copy(copy), via_spark(spark, table))

    append_new_day(spark, table, n=5_000, start_id=ROWS + 10_000)    # new data lands in the lakehouse
    lakehouse = read_everywhere(spark, catalog, table)
    stale = via_departmental_copy(copy)
    assert not kpis_equal(stale, lakehouse["spark_sql"])
    assert sum(n for _, n in lakehouse["trino"].values()) > sum(n for _, n in stale.values())
    assert spark.table(table).count() == copied_rows + 5_000


def test_tool_specific_metric_definitions_drift(table):
    rev = team_revenues(table)
    drift = (rev["marketing"] - rev["finance"]) / rev["finance"]
    print(f"marketing={rev['marketing']:.2f} finance={rev['finance']:.2f} drift={drift:.1%}")
    assert drift > 0.10                    # same table, same day, two "revenue" numbers

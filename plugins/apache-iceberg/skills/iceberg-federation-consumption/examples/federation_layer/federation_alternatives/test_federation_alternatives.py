"""Live federation vs materialized acceleration vs storage-level (shortcut-style) access."""
import time

import pytest
from trino.exceptions import TrinoUserError

from common.federation import pg_execute, pg_table_reads, run, wait_for_pg_stats
from federation_requirements.pushdown import analyze_pushdown
from federation_requirements.semantic_layer import deploy_all
from federation_alternatives.acceleration import CUSTOMER_360_ACCEL as ACCEL

DASHBOARD = "SELECT region, segment, sum(lifetime_value) AS ltv, count(*) AS customers FROM {src} " \
            "WHERE status = 'active' GROUP BY region, segment ORDER BY region, segment"


@pytest.fixture(scope="module", autouse=True)
def accelerated():
    deploy_all()
    ACCEL.drop()
    ACCEL.refresh()
    yield
    ACCEL.drop()


def _timed(sql):
    t = time.perf_counter()
    res = run(sql, user="bi_dashboard", source="superset-prod")
    return res, time.perf_counter() - t


def test_acceleration_removes_source_load(record):
    runs = 5
    before = pg_table_reads("customers")
    live = [_timed(DASHBOARD.format(src="lake.semantic.customer_360")) for _ in range(runs)]
    mid = wait_for_pg_stats("customers", before)
    accel = [_timed(DASHBOARD.format(src=ACCEL.table)) for _ in range(runs)]
    time.sleep(1.5)                                    # let PostgreSQL flush any late counters
    after = pg_table_reads("customers")
    assert live[0][0].rows == accel[0][0].rows       # same answer while the copy is fresh
    assert mid[0] - before[0] >= runs                 # every live query scanned the source
    assert after == mid                               # the accelerated queries never touched it
    live_ms = sorted(x[1] for x in live)[runs // 2] * 1000
    accel_ms = sorted(x[1] for x in accel)[runs // 2] * 1000
    record(live_pg_scans=mid[0] - before[0], accel_pg_scans=after[0] - mid[0],
           live_processed_rows=live[0][0].processed_rows, accel_processed_rows=accel[0][0].processed_rows,
           live_median_ms=round(live_ms), accel_median_ms=round(accel_ms))


def test_acceleration_is_stale_until_refreshed(record):
    cid = 11
    snapshots_before = ACCEL.snapshots()
    first_refresh = ACCEL.refreshed_at()
    try:
        pg_execute("UPDATE customers SET status = 'suspended' WHERE customer_id = %s", (cid,))
        assert ACCEL.stale_rows() == 2                # old row in the copy, new row in the source
        live = run(DASHBOARD.format(src="lake.semantic.customer_360")).rows
        copy = run(DASHBOARD.format(src=ACCEL.table)).rows
        assert live != copy
        time.sleep(1)
        ACCEL.refresh()
        assert ACCEL.stale_rows() == 0
        assert ACCEL.refreshed_at() > first_refresh
        assert ACCEL.snapshots() > snapshots_before   # each refresh is an Iceberg snapshot
        record(stale_rows_before_refresh=2, snapshots_before=snapshots_before,
               snapshots_after=ACCEL.snapshots())
    finally:
        pg_execute("UPDATE customers SET status = 'active' WHERE customer_id = %s", (cid,))
        ACCEL.refresh()


def test_trino_materialized_views_are_not_available_on_a_rest_catalog():
    with pytest.raises(TrinoUserError, match="createMaterializedView is not supported for Iceberg REST catalog"):
        run("CREATE MATERIALIZED VIEW lake.accel.mv_customer_360 AS SELECT * FROM lake.semantic.customer_360")


def test_storage_level_access_pushes_filters_but_not_aggregations(record):
    """OneLake-style shortcuts: only filters and projections reach the storage layer; the engine
    computes joins and aggregates. A database connector can push the whole aggregation."""
    run("CREATE OR REPLACE TABLE lake.accel.customers_files AS SELECT * FROM pg.public.customers")
    q = "SELECT count(*), sum(credit_limit) FROM {t} WHERE region = 'EUROPE'"
    files = analyze_pushdown(q.format(t="lake.accel.customers_files"))
    database = analyze_pushdown(q.format(t="pg.public.customers"))
    assert "aggregation" in files.in_engine
    assert "aggregation" in database.pushed and "aggregation" not in database.in_engine
    assert database.rows_from_source == 1                 # PostgreSQL returned the aggregate
    assert files.rows_from_source >= 300                  # rows (filtered at best) reach the engine
    run("DROP TABLE lake.accel.customers_files")
    record(storage_rows=files.rows_from_source, database_rows=database.rows_from_source)


def test_materialized_copy_does_not_inherit_view_row_filters(record):
    """Access rules attach to objects, not to data: the row filter on customer_360 does not
    follow the rows into the accelerated copy. Each copy needs its own rule."""
    view_rows = run("SELECT count(*) FROM lake.semantic.customer_360", user="analyst_eu").rows[0][0]
    copy_rows = run(f"SELECT count(*) FROM {ACCEL.table}", user="analyst_eu").rows[0][0]
    assert view_rows == 300
    assert copy_rows == 1500
    record(view_rows=view_rows, copy_rows=copy_rows)

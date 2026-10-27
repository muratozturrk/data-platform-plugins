"""The Trino engine behind the federation layer."""
import pytest

from common.federation import run, scalar
from federated_queries.federated import REVENUE_BY_REGION_SQL, iceberg_catalog
from trino_engine.engine_inspect import (
    catalogs, join_distributions, plan_fragments, run_with_dynamic_filtering, stage_tree)

PROMO_SQL = """
SELECT p.promo_day, count(*) AS orders, sum(o.amount) AS revenue
FROM lake.sales.orders o
JOIN pg.public.promotions p ON o.order_date = p.promo_day
WHERE p.campaign = 'black_friday'
GROUP BY p.promo_day ORDER BY p.promo_day
"""


def test_each_source_is_a_catalog_backed_by_a_connector(record):
    cats = catalogs()
    assert cats["lake"] == "iceberg"
    assert cats["pg"] == "postgresql"
    assert cats["tpch"] == "tpch"
    assert cats["system"] == "system"
    record(catalogs=cats)


def test_query_runs_as_stages_tasks_and_splits(record):
    res = run(REVENUE_BY_REGION_SQL)
    stages = stage_tree(res.stats)
    data_files = scalar('SELECT count(*) FROM lake.sales."orders$files"')
    assert len(stages) >= 3
    assert res.splits >= data_files                 # at least one split per Iceberg data file
    assert plan_fragments(REVENUE_BY_REGION_SQL).count("SOURCE") >= 2   # one source stage per connector
    record(stages=len(stages), total_splits=res.splits, iceberg_data_files=data_files,
           stage_tree=stages)


def test_cost_based_optimizer_broadcasts_the_small_dimension():
    assert join_distributions(REVENUE_BY_REGION_SQL) == ["REPLICATED"]
    forced = {"join_distribution_type": "PARTITIONED"}
    assert join_distributions(REVENUE_BY_REGION_SQL, session_properties=forced) == ["PARTITIONED"]
    assert run(REVENUE_BY_REGION_SQL, session_properties=forced).rows == run(REVENUE_BY_REGION_SQL).rows


def test_dynamic_filtering_prunes_the_iceberg_scan(record):
    """Trino's counterpart of Dremio's runtime filtering: the promotion days from PostgreSQL
    become a filter on the Iceberg scan before the large side is read."""
    on = run_with_dynamic_filtering(PROMO_SQL, enabled=True)
    off = run_with_dynamic_filtering(PROMO_SQL, enabled=False)
    assert on.result.rows == off.result.rows
    assert off.iceberg_rows_read == 15000
    assert on.iceberg_rows_read < off.iceberg_rows_read / 10
    assert on.result.splits < off.result.splits
    record(iceberg_rows_with_df=on.iceberg_rows_read, iceberg_rows_without_df=off.iceberg_rows_read,
           splits_with_df=on.result.splits, splits_without_df=off.result.splits,
           physical_bytes_with_df=on.result.stats["physicalInputBytes"],
           physical_bytes_without_df=off.result.stats["physicalInputBytes"])


@pytest.fixture
def scratch_table():
    name = "lake.sales.order_events_trino"
    run(f"DROP TABLE IF EXISTS {name}")
    yield name
    run(f"DROP TABLE IF EXISTS {name}")


def test_trino_writes_and_maintains_iceberg_tables(scratch_table, record):
    """Trino is not read-only on Iceberg: DDL, INSERT, MERGE and compaction (optimize) all work,
    and the result is a standard Iceberg table another engine (PyIceberg) can read."""
    run(f"CREATE TABLE {scratch_table} (order_id bigint, status varchar, amount decimal(12,2)) "
        "WITH (format_version = 2)")
    for batch in range(4):
        run(f"INSERT INTO {scratch_table} SELECT order_id, status, amount FROM lake.sales.orders "
            f"WHERE order_id % 4 = {batch}")
    run(f"""MERGE INTO {scratch_table} t USING (SELECT order_id FROM lake.sales.orders WHERE status = 'P') s
            ON t.order_id = s.order_id WHEN MATCHED THEN UPDATE SET status = 'F'""")
    files_before = scalar(f'SELECT count(*) FROM lake.sales."order_events_trino$files" WHERE content = 0')
    run(f"ALTER TABLE {scratch_table} EXECUTE optimize")
    files_after = scalar(f'SELECT count(*) FROM lake.sales."order_events_trino$files" WHERE content = 0')
    deletes_after = scalar(f'SELECT count(*) FROM lake.sales."order_events_trino$files" WHERE content > 0')
    snapshots = scalar(f'SELECT count(*) FROM lake.sales."order_events_trino$snapshots"')
    assert files_after < files_before and deletes_after == 0
    arrow = iceberg_catalog().load_table("sales.order_events_trino").scan().to_arrow()
    assert arrow.num_rows == 15000
    assert "P" not in set(arrow.column("status").to_pylist())
    record(data_files_before_optimize=files_before, data_files_after_optimize=files_after,
           snapshots=snapshots, pyiceberg_rows=arrow.num_rows)


def test_native_query_passthrough_runs_source_dialect(record):
    """Like Dremio's external query: SQL in PostgreSQL's own dialect, executed by PostgreSQL."""
    rows = run("""SELECT * FROM TABLE(pg.system.query(query =>
        'SELECT region, percentile_cont(0.5) WITHIN GROUP (ORDER BY credit_limit) AS median_limit
         FROM customers GROUP BY region ORDER BY region'))""").rows
    assert len(rows) == 5 and all(r[1] > 0 for r in rows)
    record(median_limits={r[0]: r[1] for r in rows})

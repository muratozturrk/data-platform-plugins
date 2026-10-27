"""Turn audit requirements (latency, freshness, concurrency, cost) into measurements."""
from datetime import datetime, timedelta, timezone

import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from common.sales import day, load_sales, sales
from consumption_requirements.requirements import (
    Requirement, build_rollup, concurrent_sessions, consumption_report, freshness_seconds, p95, plan_scan,
    trino_stats)

ROWS, DAYS = 300_000, 30
ONE_DAY_TRINO = "order_ts >= TIMESTAMP '2025-03-05 00:00:00 UTC' AND order_ts < TIMESTAMP '2025-03-06 00:00:00 UTC'"
ONE_DAY_PYICEBERG = And(GreaterThanOrEqual("order_ts", "2025-03-05T00:00:00+00:00"),
                        LessThan("order_ts", "2025-03-06T00:00:00+00:00"))
ALL_COLUMNS = ("order_id, merchant_id, customer_id, customer_email, region, category, "
               "quantity, unit_price, discount, status, order_ts")
DASHBOARD_RAW = """
SELECT region, round(sum(CASE WHEN status = 'completed' THEN quantity * unit_price * (1 - discount) END), 2)
FROM {t} WHERE order_ts >= TIMESTAMP '2025-03-24 00:00:00 UTC' GROUP BY region ORDER BY region"""
DASHBOARD_ROLLUP = """
SELECT region, round(sum(net_revenue), 2) FROM {t}
WHERE order_date >= DATE '2025-03-24' GROUP BY region ORDER BY region"""


@pytest.fixture(scope="module")
def table(spark, ns):
    t = f"{ns}.sales"
    load_sales(spark, t, ROWS, days=DAYS)
    return t


def test_partition_pruning_cuts_files_and_bytes(catalog, table):
    full, one_day = plan_scan(catalog, table), plan_scan(catalog, table, ONE_DAY_PYICEBERG)
    assert (full.files, one_day.files) == (DAYS, 1)

    t_full = trino_stats(f"SELECT count(*), sum(quantity) FROM {table}")
    t_day = trino_stats(f"SELECT count(*), sum(quantity) FROM {table} WHERE {ONE_DAY_TRINO}")
    print(f"pyiceberg files {full.files}->{one_day.files}, bytes {full.file_bytes}->{one_day.file_bytes}; "
          f"trino input bytes {t_full.physical_input_bytes}->{t_day.physical_input_bytes}, "
          f"rows {t_full.processed_rows}->{t_day.processed_rows}")
    assert t_day.rows[0][0] == ROWS // DAYS
    assert t_day.processed_rows == ROWS // DAYS                       # only one partition was read
    assert t_day.physical_input_bytes * 10 < t_full.physical_input_bytes
    # A bare count(*) over whole partitions is answered from manifest row counts: no data file is read.
    assert trino_stats(f"SELECT count(*) FROM {table} WHERE {ONE_DAY_TRINO}").physical_input_bytes == 0


def test_projection_reads_only_the_needed_columns(catalog, table):
    narrow = plan_scan(catalog, table, columns=("region", "quantity"))
    wide = plan_scan(catalog, table)
    assert narrow.files == wide.files and narrow.column_bytes * 3 < wide.column_bytes

    narrow_sql = f"SELECT region, sum(quantity) FROM {table} GROUP BY region"
    wide_sql = f"SELECT checksum(row({ALL_COLUMNS})) FROM {table}"
    # Default: Trino reads Parquet files below parquet_small_file_threshold (3MB) in one request.
    d_narrow, d_wide = trino_stats(narrow_sql), trino_stats(wide_sql)
    assert d_narrow.physical_input_bytes == d_wide.physical_input_bytes
    # Threshold lowered: only the selected column chunks are fetched.
    per_column = {"iceberg.parquet_small_file_threshold": "1kB"}
    t_narrow, t_wide = trino_stats(narrow_sql, session=per_column), trino_stats(wide_sql, session=per_column)
    print(f"column bytes 2 cols={narrow.column_bytes} all={wide.column_bytes}; trino input bytes default "
          f"{d_narrow.physical_input_bytes}/{d_wide.physical_input_bytes}, threshold 1kB "
          f"{t_narrow.physical_input_bytes}/{t_wide.physical_input_bytes}")
    assert t_narrow.processed_rows == t_wide.processed_rows == ROWS   # same rows, fewer column chunks
    assert t_narrow.physical_input_bytes * 3 < t_wide.physical_input_bytes


def test_pre_aggregated_table_answers_the_dashboard_with_far_fewer_rows(spark, table):
    rollup = f"{table}_daily_rollup"
    rollup_rows = build_rollup(spark, table, rollup)
    raw = trino_stats(DASHBOARD_RAW.format(t=table))
    pre = trino_stats(DASHBOARD_ROLLUP.format(t=rollup))
    print(f"rollup table rows={rollup_rows}; dashboard read raw={raw.processed_rows} rows/"
          f"{raw.physical_input_bytes} B, rollup={pre.processed_rows} rows/{pre.physical_input_bytes} B")
    assert [r[0] for r in raw.rows] == [r[0] for r in pre.rows]
    assert all(abs(a[1] - b[1]) <= 0.02 for a, b in zip(raw.rows, pre.rows))
    assert pre.processed_rows * 10 < raw.processed_rows
    assert pre.physical_input_bytes < raw.physical_input_bytes


def test_freshness_is_the_age_of_the_last_commit(spark, catalog, table):
    sales(spark, 1_000, start_id=ROWS, start=day(DAYS), days=1).writeTo(table).append()
    age = freshness_seconds(catalog, table)
    later = freshness_seconds(catalog, table, now=datetime.now(timezone.utc) + timedelta(hours=2))
    print(f"freshness right after commit: {age:.1f} s")
    assert 0 <= age < 120
    assert later > 7_200 - 1


def test_concurrent_dashboard_sessions_get_identical_answers(table):
    sql = f"SELECT region, count(*) FROM {table} WHERE {ONE_DAY_TRINO} GROUP BY region ORDER BY region"
    latencies, results = concurrent_sessions(sql, ["admin"] * 8)
    print(f"8 sessions: max {max(latencies):.2f} s, p95 {p95(latencies):.2f} s")
    assert len(results) == 8 and all(r == results[0] for r in results)
    assert sum(n for _, n in results[0]) == ROWS // DAYS


def test_consumption_report_turns_audit_limits_into_pass_fail(catalog, table):
    day_query = trino_stats(f"SELECT count(*), sum(unit_price) FROM {table} WHERE {ONE_DAY_TRINO}")
    full_query = trino_stats(f"SELECT count(*), sum(unit_price) FROM {table}")
    reqs = [
        Requirement("performance and latency", "dashboard query seconds", day_query.elapsed_ms / 1000, 30),
        Requirement("cost control", "bytes per one-day query", day_query.physical_input_bytes, 1_000_000),
        Requirement("cost control", "bytes per full-history query", full_query.physical_input_bytes, 1_000_000),
        Requirement("freshness", "seconds since last commit", freshness_seconds(catalog, table), 86_400),
    ]
    report = consumption_report(reqs)
    print("\n".join(report))
    assert [r.split()[0] for r in report] == ["PASS", "PASS", "FAIL", "PASS"]

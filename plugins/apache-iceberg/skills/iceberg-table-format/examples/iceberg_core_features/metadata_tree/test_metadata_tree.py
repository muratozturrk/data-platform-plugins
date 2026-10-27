"""Read the metadata layers directly and reproduce the engine's pruning decisions."""
from datetime import date, datetime, timedelta

import pytest
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan, LessThanOrEqual

from common.lakehouse import scan_metrics
from common.orders import create_orders_table, orders
from metadata_tree.metadata_tree import day_ordinal, decode_long, plan, walk

DAYS = [date(2025, 1, d) for d in range(1, 6)]
ROWS_PER_DAY = 8_000
TABLE = "orders_tree"


@pytest.fixture(scope="module")
def table(spark, ns):
    """Five daily loads (one commit each), rows range-distributed by order_id inside a day."""
    t = f"{ns}.{TABLE}"
    create_orders_table(spark, t)
    spark.sql(f"ALTER TABLE {t} WRITE ORDERED BY order_id")
    for i, d in enumerate(DAYS):
        orders(spark, ROWS_PER_DAY, i * ROWS_PER_DAY, datetime(d.year, d.month, d.day)).writeTo(t).append()
    return t


@pytest.fixture(scope="module")
def tree(table, ns):
    return walk(ns, TABLE)


def test_catalog_points_to_the_latest_metadata_json(spark, table, tree):
    latest = spark.sql(f"SELECT file FROM {table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").first().file
    assert tree.metadata_location == latest
    assert tree.metadata_location.endswith(".metadata.json")
    md = tree.metadata
    assert md["format-version"] == 2
    assert len(md["snapshots"]) == len(DAYS)
    spec = next(s for s in md["partition-specs"] if s["spec-id"] == md["default-spec-id"])
    assert [(f["name"], f["transform"]) for f in spec["fields"]] == [("order_ts_day", "day")]
    assert md["sort-orders"][-1]["fields"][0]["transform"] == "identity"


def test_tree_matches_spark_metadata_tables(spark, table, tree):
    spark_files = {r.file_path for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}
    assert {f.path for f in tree.data_files} == spark_files
    assert sum(f.record_count for f in tree.data_files) == spark.table(table).count() == 40_000
    assert len(tree.manifests) == spark.sql(f"SELECT count(*) c FROM {table}.manifests").first().c == len(DAYS)
    current = spark.sql(f"SELECT manifest_list FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    assert tree.manifest_list == current.manifest_list
    assert len(tree.data_files) > len(DAYS)                       # several files per day


def test_binary_bounds_decode_to_the_real_column_range(spark, table, tree):
    fid = tree.field_id("order_id")
    ours = {f.path: (decode_long(f.lower[fid]), decode_long(f.upper[fid])) for f in tree.data_files}
    theirs = {r.file_path: (r.lo, r.hi) for r in spark.sql(f"""
        SELECT file_path, readable_metrics.order_id.lower_bound lo, readable_metrics.order_id.upper_bound hi
        FROM {table}.files""").collect()}
    assert ours == theirs


def test_manifest_list_summaries_skip_whole_manifests(tree):
    p = plan(tree, DAYS[2])
    assert (p["manifests_total"], p["manifests_scanned"]) == (5, 1)
    day3 = [f for f in tree.data_files if f.partition == {"order_ts_day": day_ordinal(DAYS[2])}]
    assert len(day3) == 4                                          # range distribution: 4 files per day
    assert p["files"] == sorted(f.path for f in day3)


def test_hand_planner_agrees_with_pyiceberg_and_spark(spark, catalog, table, tree):
    day = DAYS[2]
    lo_id = 2 * ROWS_PER_DAY + 100                                 # a narrow id range inside day 3
    hi_id = lo_id + 50
    ours = plan(tree, day, (lo_id, hi_id))

    start = datetime(day.year, day.month, day.day)
    end = start + timedelta(days=1)
    pyice = catalog.load_table(table).scan(row_filter=And(
        GreaterThanOrEqual("order_ts", start.isoformat() + "+00:00"), LessThan("order_ts", end.isoformat() + "+00:00"),
        GreaterThanOrEqual("order_id", lo_id), LessThanOrEqual("order_id", hi_id))).plan_files()
    assert sorted(t.file.file_path for t in pyice) == ours["files"]

    m = scan_metrics(spark.sql(f"""SELECT * FROM {table}
        WHERE order_ts >= TIMESTAMP '{start}' AND order_ts < TIMESTAMP '{end}'
          AND order_id BETWEEN {lo_id} AND {hi_id}"""))
    assert m["resultDataFiles"] == len(ours["files"]) == 1
    # Spark's Iceberg scan metrics show the same two-level pruning as the hand planner.
    assert (m["totalDataManifest"], m["scannedDataManifests"], m["skippedDataManifests"]) == (5, 1, 4)
    assert m["skippedDataFiles"] == 3                              # counted inside scanned manifests only

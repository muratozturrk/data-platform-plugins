from datetime import date

import pyarrow.compute as pc
from pyiceberg.expressions import EqualTo, GreaterThan

from common.trips import create_trips_table, make_trips
from pyiceberg_lifecycle.lifecycle import (add_borough_column, bucket_by_vendor, day_range,
                                         delete_refunds, expensive_trips, overwrite_day,
                                         planned_files, snapshot_log, total)

DAY2 = date(2023, 1, 2)


def test_create_and_append_writes_one_file_per_partition(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips", rows=5000, days=5)
    assert t.scan().to_arrow().num_rows == 5000
    assert planned_files(t) == 5                       # one Parquet file per pickup_date
    assert [op for _, op, _ in snapshot_log(t)] == ["append"]
    t.append(make_trips(500, days=5, first_id=10_001, seed=8))
    assert [(op, n) for _, op, n in snapshot_log(t)] == [("append", 5000), ("append", 5500)]
    assert planned_files(t) == 10                      # appends add files, never rewrite


def test_overwrite_replaces_exactly_one_partition(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_ow")
    before = t.scan().to_arrow()
    other_days_before = before.filter(pc.not_equal(before["pickup_date"], DAY2))
    fresh = make_trips(10, days=1, first_id=90_001, seed=99, start=DAY2)
    overwrite_day(t, DAY2, fresh)
    after = t.scan().to_arrow()
    day2 = after.filter(pc.equal(after["pickup_date"], DAY2))
    assert sorted(day2["trip_id"].to_pylist()) == list(range(90_001, 90_011))
    others = after.filter(pc.not_equal(after["pickup_date"], DAY2))
    assert others.num_rows == other_days_before.num_rows and total(others) == total(other_days_before)
    # A partition-aligned overwrite is a delete snapshot plus an append snapshot.
    assert [op for _, op, _ in snapshot_log(t)] == ["append", "delete", "append"]


def test_delete_by_predicate_is_copy_on_write(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_del")
    cheap = t.scan(row_filter="total_amount < 10").to_arrow().num_rows
    assert cheap > 0
    delete_refunds(t, 10.0)
    assert t.scan(row_filter="total_amount < 10").to_arrow().num_rows == 0
    assert t.scan().to_arrow().num_rows == 5000 - cheap
    last = t.metadata.snapshots[-1].summary
    assert last.operation.value == "overwrite"         # files rewritten, no delete files
    assert int(last["total-delete-files"]) == 0


def test_schema_evolution_keeps_old_rows_readable(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_schema")
    old_files = {task.file.file_path for task in t.scan().plan_files()}
    add_borough_column(t)
    new = make_trips(3, first_id=20_001).append_column("pickup_borough", __import__("pyarrow").array(["Queens"] * 3))
    t.append(new)
    rows = t.scan().to_arrow()
    assert rows.schema.field("pickup_borough").type == "large_string"   # 0.9.1 reads strings as large_string
    assert rows.filter(pc.is_null(rows["pickup_borough"])).num_rows == 5000   # old rows: null
    assert rows.filter(pc.equal(rows["pickup_borough"], "Queens")).num_rows == 3
    assert old_files <= {task.file.file_path for task in t.scan().plan_files()}  # nothing rewritten
    assert len(t.metadata.schemas) == 2


def test_partition_evolution_applies_to_new_files_only(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_spec")
    bucket_by_vendor(t)
    t.append(make_trips(1000, first_id=30_001, seed=3))
    files = {}
    for task in t.scan().plan_files():
        files.setdefault(task.file.spec_id, []).append(task.file.partition)
    assert len(files[0]) == 5                            # old layout untouched: 1 file per day
    assert all(len(p) == 1 for p in files[0])            # (pickup_date)
    assert all(len(p) == 2 for p in files[1])            # (pickup_date, vendor_bucket)
    assert len(files[1]) == 5 * len({p[1] for p in files[1]})   # one file per day x bucket
    assert t.scan(row_filter=EqualTo("vendor_id", 1)).to_arrow().num_rows == \
        pc.sum(pc.equal(t.scan().to_arrow()["vendor_id"], 1)).as_py()


def test_time_travel_reads_any_snapshot(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_tt")
    first = t.current_snapshot().snapshot_id
    t.append(make_trips(100, first_id=40_001, seed=4))
    overwrite_day(t, DAY2, make_trips(1, days=1, first_id=99_999, start=DAY2))
    history = snapshot_log(t)
    assert [n for _, _, n in history] == [5000, 5100, 5100 - 1020, 4081]
    assert t.scan(snapshot_id=first).to_arrow().num_rows == 5000
    assert t.scan(snapshot_id=history[1][0]).to_arrow().num_rows == 5100
    assert t.scan().to_arrow().num_rows == 4081
    # Rolling back is just pointing readers at an older snapshot id: no data is copied.
    assert planned_files(t, snapshot_id=first) == 5


def test_scan_planning_prunes_files_with_partition_and_column_stats(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_plan", rows=5000, days=5)
    assert planned_files(t) == 5
    # Partition pruning: 2 of 5 days.
    assert planned_files(t, day_range(date(2023, 1, 2), date(2023, 1, 4))) == 2
    # Column stats pruning: no file has total_amount above its max.
    max_amount = pc.max(t.scan().to_arrow()["total_amount"]).as_py()
    assert planned_files(t, GreaterThan("total_amount", max_amount)) == 0
    # A filter only on values present in every file prunes nothing at file level.
    assert planned_files(t, GreaterThan("total_amount", 100)) == 5
    rich = expensive_trips(t)
    assert rich.column_names == ["trip_id", "pickup_date", "total_amount"]
    assert rich.num_rows > 0 and pc.min(rich["total_amount"]).as_py() > 100

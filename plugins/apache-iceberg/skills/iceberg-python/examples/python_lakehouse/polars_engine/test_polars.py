from datetime import date

import polars as pl
import pyarrow.compute as pc
import pytest

from common.trips import TRIPS_SCHEMA, create_trips_table
from polars_engine.polars_ops import append, avg_distance_by_passengers, files_in_plan, scan


@pytest.fixture(scope="module")
def trips(catalog, ns):
    return create_trips_table(catalog, f"{ns}.trips")


def test_scan_without_storage_options_goes_to_aws(trips):
    # The PyIceberg table carries s3.endpoint, but the native Polars reader ignores it.
    with pytest.raises(OSError, match="amazonaws.com"):
        pl.scan_iceberg(trips).collect()
    assert scan(trips).collect().height == 5000


def test_scan_from_metadata_json(trips):
    # pl.scan_iceberg("s3://.../metadata.json", storage_options=...) works once the endpoint is passed.
    assert scan(trips.metadata_location).collect().height == 5000


def test_aggregation_matches_pyarrow(trips):
    summary = avg_distance_by_passengers(scan(trips))
    raw = trips.scan().to_arrow()
    expected = {}
    for k in sorted(set(raw["passenger_count"].to_pylist()) - {1}):
        sel = raw.filter(pc.equal(raw["passenger_count"], k))
        expected[k] = pc.mean(sel["trip_distance"]).as_py()
    got = dict(zip(summary["passenger_count"], summary["avg_distance"]))
    assert got.keys() == expected.keys() == {2, 3, 4}
    assert all(abs(got[k] - expected[k]) < 1e-9 for k in got)


def test_lazy_filters_prune_iceberg_files(trips):
    lf = scan(trips)
    assert files_in_plan(lf) == 5
    assert files_in_plan(lf.filter(pl.col("pickup_date") == date(2023, 1, 2))) == 1    # partition
    assert files_in_plan(lf.filter(pl.col("total_amount") > 10_000)) == 0             # column stats


def test_write_iceberg_by_name(catalog, ns):
    # write_iceberg("ns.table") calls load_catalog() with no name: the "default" catalog must be
    # configured. The runner sets PYICEBERG_CATALOG__DEFAULT__* in docker-compose.yml (PyIceberg
    # reads them once, at import time; ~/.pyiceberg.yaml works the same way).
    t = catalog.create_table(f"{ns}.trips_table", schema=TRIPS_SCHEMA)   # unpartitioned
    new_data = pl.DataFrame({"trip_id": [101, 102, 103], "passenger_count": [1, 2, 3],
                             "trip_distance": [3.5, 5.1, 2.9]})
    with pytest.raises(ValueError, match="Mismatch in fields"):
        new_data.write_iceberg(f"{ns}.trips_table", mode="append")     # Int64 vs Iceberg int
    new_data.with_columns(pl.col("passenger_count").cast(pl.Int32)) \
        .write_iceberg(f"{ns}.trips_table", mode="append")             # missing columns are fine
    t.refresh()
    rows = t.scan().to_arrow()
    assert rows.num_rows == 3 and rows["pickup_date"].null_count == 3


def test_overwrite_mode_replaces_the_whole_table(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_ow")
    first = t.current_snapshot().snapshot_id
    small = scan(t).filter(pl.col("pickup_date") == date(2023, 1, 1)).head(2).collect()
    small.write_iceberg(t, mode="overwrite")          # no filter argument exists
    t.refresh()
    assert scan(t).collect().height == 2               # all 5 days replaced, not just one
    assert scan(t, snapshot_id=first).collect().height == 5000   # time travel still has it


def test_append_helper_casts_python_ints(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips_app")
    row = {"trip_id": [9_000_001], "vendor_id": [1], "passenger_count": [2],
           "trip_distance": [1.5], "total_amount": [9.9]}
    with pytest.raises(KeyError, match="pickup_date"):     # partition source column is mandatory
        append(t, pl.DataFrame(row))
    append(t, pl.DataFrame({**row, "pickup_date": [date(2023, 1, 9)]}))
    t.refresh()
    assert scan(t).collect().height == 5001
    assert [s.summary.operation.value for s in t.metadata.snapshots] == ["append", "append"]

"""Spice AI: the open-source runtime over the REST catalog, plus a CSV source."""
import time
from importlib.metadata import metadata

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from common.seed_spice import NS
from common.trips import make_trips
from duckdb_engine.duckdb_ops import connect
from platform_sdks.spice_client import query, wait_ready


@pytest.fixture(scope="module")
def trips(catalog):
    t = catalog.load_table(f"{NS}.trips")
    t.overwrite(make_trips(5000))                 # same table object Spice resolved at start-up
    rw = catalog.load_table(f"{NS}.trips_rw")
    rw.overwrite(make_trips(10, days=1, first_id=1))
    wait_ready({"trips", "trips_rw", "driver_ratings"})
    return t


def test_pypi_spicepy_is_not_the_spice_sdk():
    meta = metadata("spicepy")
    assert meta["Summary"] == "Circuit simulator written in Python"
    import spicepy
    assert not hasattr(spicepy, "Client")         # the documented `from spicepy import Client` fails


def test_aggregate_query_over_iceberg(trips):
    got = query("""SELECT passenger_count, AVG(total_amount) AS avg_fare, COUNT(*) AS n
                   FROM trips WHERE total_amount > 50
                   GROUP BY passenger_count ORDER BY avg_fare DESC""").to_pylist()
    raw = trips.scan(row_filter="total_amount > 50").to_arrow()
    assert sum(r["n"] for r in got) == raw.num_rows
    for r in got:
        sel = raw.filter(pc.equal(raw["passenger_count"], r["passenger_count"]))
        assert abs(r["avg_fare"] - pc.mean(sel["total_amount"]).as_py()) < 1e-9


def test_federated_join_iceberg_with_csv(trips):
    joined = query("""SELECT i.trip_id, i.total_amount, p.driver_rating
                      FROM trips AS i JOIN driver_ratings AS p ON i.driver_id = p.driver_id
                      WHERE i.total_amount > 100""")
    assert joined.num_rows == trips.scan(row_filter="total_amount > 100").to_arrow().num_rows
    assert set(joined["driver_rating"].to_pylist()) <= {4.9, 4.7, 4.2, 3.8, 4.5}


def test_reads_follow_new_snapshots_and_push_filters(trips):
    before = query("SELECT count(*) AS n FROM trips").to_pylist()[0]["n"]
    trips.append(make_trips(7, first_id=50_001, seed=5))
    start, seen = time.time(), before
    while seen != before + 7 and time.time() - start < 30:
        time.sleep(0.5)
        seen = query("SELECT count(*) AS n FROM trips").to_pylist()[0]["n"]
    assert seen == before + 7                      # no refresh call needed, just a short lag
    print(f"spice saw the new snapshot after {time.time() - start:.1f} s")
    plan = query("EXPLAIN SELECT count(*) FROM trips WHERE total_amount > 100").to_pylist()
    physical = next(p["plan"] for p in plan if p["plan_type"] == "physical_plan")
    assert "IcebergTableScan" in physical and "predicate:[total_amount > 100]" in physical


def test_default_datasets_are_read_only(trips):
    with pytest.raises(pa.ArrowInvalid, match="read-only dataset"):
        query("INSERT INTO trips (trip_id) VALUES (1)")


def test_read_write_dataset_writes_equality_deletes(catalog, trips):
    assert query("INSERT INTO trips_rw (trip_id, total_amount) VALUES (99, 9.0)").to_pylist() == [{"count": 1}]
    assert query("DELETE FROM trips_rw WHERE trip_id = 1").to_pylist() == [{"count": 1}]
    rw = catalog.load_table(f"{NS}.trips_rw")
    assert int(rw.current_snapshot().summary["added-equality-deletes"]) == 1
    with pytest.raises(ValueError, match="equality deletes"):
        rw.scan().to_arrow()                       # PyIceberg 0.9.1 (and Polars, Daft) cannot read it
    con = connect()
    ids = sorted(r[0] for r in con.execute(
        f"SELECT trip_id FROM iceberg_scan('{rw.metadata_location}')").fetchall())
    assert ids == list(range(2, 11)) + [99]        # DuckDB applies the equality delete
    assert query("SELECT count(*) AS n FROM trips_rw").to_pylist()[0]["n"] == 10

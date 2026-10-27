from datetime import date

import duckdb
import pytest

from common.trips import create_trips_table, make_trips
from duckdb_engine.duckdb_ops import (attach_rest_catalog, connect, top_trips_via_arrow,
                                       top_trips_via_iceberg_scan)


@pytest.fixture(scope="module")
def trips(catalog, ns):
    t = create_trips_table(catalog, f"{ns}.trips")
    t.append(make_trips(1000, first_id=10_001, seed=11))
    return t


@pytest.fixture
def con():
    c = connect()
    yield c
    c.close()


def test_extension_comes_from_pypi_wheel(con):
    row = con.execute("SELECT loaded, installed_from FROM duckdb_extensions() "
                      "WHERE extension_name = 'iceberg'").fetchone()
    assert row[0] is True
    assert "duckdb_extension_iceberg" in row[1]          # local wheel path, not the extension repo


def test_arrow_route_and_iceberg_scan_agree(con, trips):
    via_arrow = top_trips_via_arrow(con, trips)
    via_ext = top_trips_via_iceberg_scan(con, trips.metadata_location)
    assert len(via_arrow) == 10 and via_arrow == via_ext


def test_snapshot_id_parameter_fails_snapshot_from_id_works(con, trips):
    first = trips.metadata.snapshots[0].snapshot_id
    with pytest.raises(duckdb.BinderException, match='Invalid named parameter "snapshot_id"'):
        con.execute(f"SELECT COUNT(*) FROM iceberg_scan('{trips.metadata_location}', "
                    f"snapshot_id := '{first}')")
    n = con.execute(f"SELECT COUNT(*) FROM iceberg_scan('{trips.metadata_location}', "
                    f"snapshot_from_id={first})").fetchone()[0]
    assert n == 5000
    assert top_trips_via_iceberg_scan(con, trips.metadata_location, first) == \
        top_trips_via_arrow(con, trips, first)


def test_attached_catalog_ctas_and_insert(con, catalog, ns, trips):
    attach_rest_catalog(con)
    tables = con.execute(f"SHOW TABLES FROM iceberg_catalog.{ns}").fetchall()
    assert ("trips",) in tables
    con.execute(f"""
        CREATE TABLE iceberg_catalog.{ns}.daily_trips AS
        SELECT trip_id, trip_distance, total_amount FROM iceberg_catalog.{ns}.trips
        WHERE pickup_date = DATE '2023-01-01'""")
    con.execute(f"INSERT INTO iceberg_catalog.{ns}.daily_trips VALUES (1001, 3.2, 15.75), (1002, 7.1, 28.10)")
    # The tables DuckDB wrote are ordinary Iceberg tables for every other engine.
    daily = catalog.load_table(f"{ns}.daily_trips")
    assert daily.scan().to_arrow().num_rows == 1200 + 2   # 1,000 + 200 rows on day 1, plus 2
    assert [s.summary.operation.value for s in daily.metadata.snapshots] == ["append", "append"]


def test_attached_catalog_delete_update_and_time_travel(con, catalog, ns, trips):
    attach_rest_catalog(con)
    con.execute(f"CREATE TABLE iceberg_catalog.{ns}.dml AS SELECT * FROM iceberg_catalog.{ns}.trips "
                f"WHERE pickup_date = DATE '2023-01-03'")
    t = catalog.load_table(f"{ns}.dml")
    created = t.current_snapshot().snapshot_id
    deleted = con.execute(f"DELETE FROM iceberg_catalog.{ns}.dml WHERE total_amount < 20").fetchone()[0]
    con.execute(f"UPDATE iceberg_catalog.{ns}.dml SET passenger_count = 9 WHERE trip_id % 100 = 3")
    t.refresh()
    rows = t.scan().to_arrow()
    assert deleted > 0 and rows.num_rows == 1200 - deleted
    assert 9 in rows["passenger_count"].to_pylist()
    # DuckDB writes merge-on-read position deletes that PyIceberg applies on read.
    summary = t.current_snapshot().summary
    assert [s.summary.operation.value for s in t.metadata.snapshots] == ["append", "delete", "overwrite"]
    assert int(summary["total-position-deletes"]) > 0 and summary.get("total-equality-deletes") is None
    assert int(summary["total-delete-files"]) == 2
    # total-records counts physical rows (deleted rows included), not what a reader returns.
    assert int(summary["total-records"]) > rows.num_rows
    old = con.execute(f"SELECT count(*) FROM iceberg_catalog.{ns}.dml AT (VERSION => {created})").fetchone()[0]
    assert old == 1200

"""Measure, per library, which Iceberg operations actually work with the pinned versions.

Each probe runs the operation for real on a scratch table and reports True/False; an operation
the library has no API for is False. The result backs the "which library for which job" table.
"""
import inspect
from datetime import date

import daft
import polars as pl
from pyiceberg.expressions import EqualTo
from pyiceberg.types import StringType

from common.lakehouse import S3_PROPS
from common.trips import create_trips_table, make_trips

OPERATIONS = ["time_travel_read", "append", "partition_overwrite", "row_delete",
              "schema_evolution", "read_equality_deletes"]
DAY1 = date(2023, 1, 1)


def _ok(fn) -> bool:
    try:
        return fn() is not False
    except Exception:
        return False


def _fresh(catalog, ident):
    return create_trips_table(catalog, ident, rows=500)


def probe_pyiceberg(catalog, ns, eq_table):
    t = _fresh(catalog, f"{ns}.cap_pyiceberg")
    first = t.current_snapshot().snapshot_id
    return {
        "time_travel_read": _ok(lambda: t.scan(snapshot_id=first).to_arrow().num_rows == 500),
        "append": _ok(lambda: t.append(make_trips(5, first_id=9001))),
        "partition_overwrite": _ok(lambda: t.overwrite(make_trips(1, days=1, first_id=9101),
                                                       overwrite_filter=EqualTo("pickup_date", DAY1.isoformat()))),
        "row_delete": _ok(lambda: t.delete(delete_filter="total_amount < 20")),
        "schema_evolution": _ok(lambda: t.update_schema().add_column("zone", StringType()).commit()),
        "read_equality_deletes": _ok(lambda: eq_table.scan().to_arrow()),
    }


def probe_polars(catalog, ns, eq_table):
    t = _fresh(catalog, f"{ns}.cap_polars")
    first = t.current_snapshot().snapshot_id
    write_params = inspect.signature(pl.DataFrame.write_iceberg).parameters
    return {
        "time_travel_read": _ok(lambda: pl.scan_iceberg(t, snapshot_id=first, storage_options=S3_PROPS)
                                .collect().height == 500),
        "append": _ok(lambda: pl.from_arrow(make_trips(5, first_id=9001)).write_iceberg(t, mode="append")),
        # mode="overwrite" replaces the whole table; there is no filter parameter.
        "partition_overwrite": "overwrite_filter" in write_params,
        "row_delete": hasattr(pl.DataFrame, "delete_iceberg"),
        "schema_evolution": False,       # no Polars API; done through PyIceberg
        "read_equality_deletes": _ok(lambda: pl.scan_iceberg(eq_table, storage_options=S3_PROPS).collect()),
    }


def probe_duckdb(catalog, ns, eq_table, con):
    t = _fresh(catalog, f"{ns}.cap_duckdb")
    first = t.current_snapshot().snapshot_id
    name = f"iceberg_catalog.{ns}.cap_duckdb"
    return {
        "time_travel_read": _ok(lambda: con.execute(
            f"SELECT count(*) FROM {name} AT (VERSION => {first})").fetchone()[0] == 500),
        "append": _ok(lambda: con.execute(f"INSERT INTO {name} (trip_id, pickup_date) VALUES (9001, DATE '2023-01-02')")),
        "partition_overwrite": _ok(lambda: con.execute(
            f"BEGIN; DELETE FROM {name} WHERE pickup_date = DATE '2023-01-01'; "
            f"INSERT INTO {name} (trip_id, pickup_date) VALUES (9101, DATE '2023-01-01'); COMMIT")),
        "row_delete": _ok(lambda: con.execute(f"DELETE FROM {name} WHERE total_amount < 20")),
        "schema_evolution": _ok(lambda: con.execute(f"ALTER TABLE {name} ADD COLUMN zone VARCHAR")),
        "read_equality_deletes": _ok(lambda: con.execute(
            f"SELECT count(*) FROM iceberg_scan('{eq_table.metadata_location}')").fetchone()),
    }


def probe_daft(catalog, ns, eq_table):
    t = _fresh(catalog, f"{ns}.cap_daft")
    first = t.current_snapshot().snapshot_id
    return {
        "time_travel_read": _ok(lambda: daft.read_iceberg(t, snapshot_id=first).count_rows() == 500),
        "append": _ok(lambda: daft.from_arrow(make_trips(5, first_id=9001)).write_iceberg(t, mode="append")),
        "partition_overwrite": _ok(lambda: daft.from_arrow(make_trips(1, days=1, first_id=9101)).write_iceberg(
            t, mode="overwrite", overwrite_filter=daft.col("pickup_date") == DAY1)),
        "row_delete": hasattr(daft.DataFrame, "delete_iceberg"),
        "schema_evolution": False,       # no Daft API; done through PyIceberg
        "read_equality_deletes": _ok(lambda: daft.read_iceberg(eq_table).to_arrow()),
    }

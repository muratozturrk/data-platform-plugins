"""One snapshot, many engines: the same KPI query through every Python library in this environment."""
import daft
import duckdb
import polars as pl
import pyarrow as pa

from common.lakehouse import S3_PROPS

KPI_SQL = """SELECT passenger_count, count(*) AS trips, sum(total_amount) AS revenue,
                    avg(trip_distance) AS avg_distance
             FROM {source} GROUP BY passenger_count ORDER BY passenger_count"""


def _rows(table: pa.Table) -> list[tuple]:
    t = table.sort_by("passenger_count")
    return [(int(p), int(n), float(r), float(d)) for p, n, r, d in
            zip(*(t[c].to_pylist() for c in ("passenger_count", "trips", "revenue", "avg_distance")))]


def via_pyiceberg(table, snapshot_id) -> list[tuple]:
    arrow = table.scan(snapshot_id=snapshot_id).to_arrow()
    g = arrow.group_by("passenger_count").aggregate(
        [("trip_id", "count"), ("total_amount", "sum"), ("trip_distance", "mean")])
    names = {"trip_id_count": "trips", "total_amount_sum": "revenue", "trip_distance_mean": "avg_distance"}
    return _rows(g.rename_columns([names.get(c, c) for c in g.column_names]))


def via_polars(table, snapshot_id) -> list[tuple]:
    df = (pl.scan_iceberg(table, snapshot_id=snapshot_id, storage_options=S3_PROPS)
          .group_by("passenger_count")
          .agg(pl.len().alias("trips"), pl.col("total_amount").sum().alias("revenue"),
               pl.col("trip_distance").mean().alias("avg_distance"))
          .collect())
    return _rows(df.to_arrow())


def via_duckdb_arrow(table, snapshot_id) -> list[tuple]:
    con = duckdb.connect()
    con.register("t", table.scan(snapshot_id=snapshot_id).to_arrow())
    return _rows(con.execute(KPI_SQL.format(source="t")).fetch_arrow_table())


def via_duckdb_iceberg_scan(con, table, snapshot_id) -> list[tuple]:
    src = f"iceberg_scan('{table.metadata_location}', snapshot_from_id={snapshot_id})"
    return _rows(con.execute(KPI_SQL.format(source=src)).fetch_arrow_table())


def via_duckdb_catalog(con, identifier: str, snapshot_id) -> list[tuple]:
    src = f"iceberg_catalog.{identifier} AT (VERSION => {snapshot_id})"
    return _rows(con.execute(KPI_SQL.format(source=src)).fetch_arrow_table())


def via_daft(table, snapshot_id) -> list[tuple]:
    df = (daft.read_iceberg(table, snapshot_id=snapshot_id)
          .groupby("passenger_count")
          .agg(daft.col("trip_id").count().alias("trips"),
               daft.col("total_amount").sum().alias("revenue"),
               daft.col("trip_distance").mean().alias("avg_distance")))
    return _rows(df.to_arrow())


def via_spice(query) -> list[tuple]:
    """Spice always reads the current snapshot (no time-travel syntax over Flight here)."""
    return _rows(query(KPI_SQL.format(source="trips")))


def same(a: list[tuple], b: list[tuple], rel: float = 1e-9) -> bool:
    """Equal keys and counts; float sums equal up to summation order."""
    return len(a) == len(b) and all(
        x[:2] == y[:2] and all(abs(u - v) <= rel * max(1.0, abs(u)) for u, v in zip(x[2:], y[2:]))
        for x, y in zip(a, b))

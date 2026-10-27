"""One Iceberg table, six consumers: the "tools come to the data" model.

Each reader computes the same KPI (net revenue and completed orders per region) through a
different consumption path, optionally pinned to one snapshot:
  Spark SQL, PyIceberg -> Arrow compute, pandas, Polars, DuckDB, Trino (SQL endpoint for BI tools).
PyIceberg API: https://py.iceberg.apache.org/api/ ; Trino time travel:
https://trino.io/docs/current/connector/iceberg.html#time-travel-queries
"""
import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from common.lakehouse import trino_query

Kpis = dict[str, tuple[float, int]]          # region -> (net_revenue, completed_orders)
KPI_COLUMNS = ("region", "quantity", "unit_price", "discount", "status")
KPI_SQL = """
SELECT region, sum(quantity * unit_price * (1 - discount)) AS net_revenue, count(*) AS orders
FROM {source} {version} WHERE status = 'completed' GROUP BY region
"""


def _kpis(rows) -> Kpis:
    return {r[0]: (float(r[1]), int(r[2])) for r in rows}


def scan_arrow(catalog, table: str, snapshot_id: int | None = None) -> pa.Table:
    """Projection + filter pushed into the Iceberg scan; only needed columns are read."""
    return catalog.load_table(table).scan(row_filter="status == 'completed'", selected_fields=KPI_COLUMNS,
                                          snapshot_id=snapshot_id).to_arrow()


def via_spark(spark, table: str, snapshot_id: int | None = None) -> Kpis:
    version = f"VERSION AS OF {snapshot_id}" if snapshot_id else ""
    return _kpis(spark.sql(KPI_SQL.format(source=table, version=version)).collect())


def via_trino(table: str, snapshot_id: int | None = None) -> Kpis:
    version = f"FOR VERSION AS OF {snapshot_id}" if snapshot_id else ""
    return _kpis(trino_query(KPI_SQL.format(source=table, version=version)))


def via_arrow(catalog, table: str, snapshot_id: int | None = None) -> Kpis:
    t = scan_arrow(catalog, table, snapshot_id)
    net = pc.multiply(pc.multiply(t["quantity"], t["unit_price"]), pc.subtract(1.0, t["discount"]))
    out = pa.table({"region": t["region"], "net": net}).group_by("region").aggregate([("net", "sum"), ("net", "count")])
    return _kpis(zip(out["region"].to_pylist(), out["net_sum"].to_pylist(), out["net_count"].to_pylist()))


def via_pandas(catalog, table: str, snapshot_id: int | None = None) -> Kpis:
    df = scan_arrow(catalog, table, snapshot_id).to_pandas()
    df["net"] = df.quantity * df.unit_price * (1 - df.discount)
    g = df.groupby("region")["net"].agg(["sum", "count"])
    return _kpis(zip(g.index, g["sum"], g["count"]))


def via_polars(catalog, table: str, snapshot_id: int | None = None) -> Kpis:
    df = pl.from_arrow(scan_arrow(catalog, table, snapshot_id))
    g = df.group_by("region").agg(
        (pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount"))).sum().alias("net"),
        pl.len().alias("n"))
    return _kpis(g.iter_rows())


def via_duckdb(catalog, table: str, snapshot_id: int | None = None) -> Kpis:
    con = duckdb.connect()
    con.register("sales", scan_arrow(catalog, table, snapshot_id))
    return _kpis(con.execute(KPI_SQL.format(source="sales", version="")).fetchall())


def read_everywhere(spark, catalog, table: str, snapshot_id: int | None = None) -> dict[str, Kpis]:
    return {
        "spark_sql": via_spark(spark, table, snapshot_id),
        "trino": via_trino(table, snapshot_id),
        "pyiceberg_arrow": via_arrow(catalog, table, snapshot_id),
        "pandas": via_pandas(catalog, table, snapshot_id),
        "polars": via_polars(catalog, table, snapshot_id),
        "duckdb": via_duckdb(catalog, table, snapshot_id),
    }


def kpis_equal(a: Kpis, b: Kpis, tol: float = 1e-6) -> bool:
    """Same regions, same counts, revenue equal up to float summation order."""
    return a.keys() == b.keys() and all(
        a[k][1] == b[k][1] and abs(a[k][0] - b[k][0]) <= tol * max(1.0, abs(a[k][0])) for k in a)


# --- the traditional alternative: copy data into a departmental store ------------------------

def export_departmental_copy(catalog, table: str, path: str) -> int:
    """The traditional pattern: a nightly extract into a team-owned file/warehouse."""
    t = catalog.load_table(table).scan().to_arrow()
    pq.write_table(t, path)
    return t.num_rows


def via_departmental_copy(path: str) -> Kpis:
    con = duckdb.connect()
    return _kpis(con.execute(KPI_SQL.format(source=f"read_parquet('{path}')", version="")).fetchall())


# --- the semantic problem: every tool defines "revenue" its own way --------------------------

TEAM_REVENUE_DEFINITIONS = {
    # marketing dashboard: everything not cancelled, list price
    "marketing": "SELECT sum(quantity * unit_price) FROM {t} WHERE status <> 'cancelled'",
    # finance report: completed only, after discount
    "finance": "SELECT sum(quantity * unit_price * (1 - discount)) FROM {t} WHERE status = 'completed'",
}


def team_revenues(table: str) -> dict[str, float]:
    return {team: float(trino_query(sql.format(t=table))[0][0]) for team, sql in TEAM_REVENUE_DEFINITIONS.items()}

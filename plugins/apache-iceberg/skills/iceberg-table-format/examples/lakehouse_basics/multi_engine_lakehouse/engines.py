"""One copy of the data, three engines: Spark (JVM, batch ETL), PyIceberg (Python service), DuckDB (local SQL).

All three go through the same REST catalog and read the same Parquet files on S3. Nothing is exported,
loaded or replicated per tool: that is the "single canonical copy" claim of the lakehouse.
"""
import pyarrow as pa
from pyiceberg.expressions import EqualTo

REVENUE_SQL = """
    SELECT country, round(sum(amount), 2) AS revenue, count(*) AS orders
    FROM {table} WHERE status <> 'returned'
    GROUP BY country ORDER BY country
"""


def revenue_spark(spark, table: str) -> list[tuple]:
    return [tuple(r) for r in spark.sql(REVENUE_SQL.format(table=table)).collect()]


def revenue_duckdb(iceberg_table) -> list[tuple]:
    """DuckDB queries the Arrow batches PyIceberg reads from the table's own data files."""
    con = iceberg_table.scan(selected_fields=("country", "amount", "status")).to_duckdb(table_name="orders")
    return con.execute(REVENUE_SQL.format(table="orders")).fetchall()


def files_read_by_pyiceberg(iceberg_table) -> set[str]:
    return {t.file.file_path for t in iceberg_table.scan().plan_files()}


def append_from_python_service(iceberg_table, batch: pa.Table) -> int:
    """A Python microservice writes to the same table without Spark. Returns the new snapshot id."""
    iceberg_table.append(batch)
    return iceberg_table.current_snapshot().snapshot_id


def lookup_order(iceberg_table, order_id: int) -> dict | None:
    rows = iceberg_table.scan(row_filter=EqualTo("order_id", order_id)).to_arrow().to_pylist()
    return rows[0] if rows else None

"""Extract fashion_sales from PostgreSQL with Spark JDBC and load it into an Iceberg table.

Two load modes, matching the batch-vs-incremental question every ingestion design must answer:
- full_reload(): `writeTo(...).createOrReplace()` (one atomic snapshot per run)
- incremental_append(): high-water mark on the SERIAL id, appends only new source rows
"""
import os

from pyspark.sql import DataFrame, SparkSession, functions as F

JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/mydb")
JDBC_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER", "myuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "mypassword"),   # local dev default
    "driver": "org.postgresql.Driver",
}
SOURCE_TABLE = "fashion_sales"


def read_source(spark: SparkSession, query: str = SOURCE_TABLE) -> DataFrame:
    """spark.read.jdbc; `query` may be a table name or a parenthesised subquery."""
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPERTIES)


def full_reload(spark: SparkSession, namespace: str, table: str = SOURCE_TABLE) -> str:
    target = f"{namespace}.{table}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    (read_source(spark)
     .writeTo(target)
     .tableProperty("format-version", "2")
     .createOrReplace())
    return target


def incremental_append(spark: SparkSession, target: str) -> int:
    """Append source rows whose id is above the table's current high-water mark. Returns rows added."""
    high_water = spark.table(target).agg(F.coalesce(F.max("id"), F.lit(0))).first()[0]
    # The predicate runs inside PostgreSQL (subquery pushdown), so only new rows cross the wire.
    new_rows = read_source(spark, f"(SELECT * FROM {SOURCE_TABLE} WHERE id > {int(high_water)}) AS delta")
    n = new_rows.count()
    if n:
        new_rows.writeTo(target).append()
    return n

"""One order table workload, written by Spark at any format version.

Covers what the spec has rules for: several column types (bounds encoding, string truncation), two
partition transforms, a sort order, schema evolution, appends and a row-level delete.
"""
from pyspark.sql import SparkSession

DDL = """
CREATE TABLE {table} (
    order_id   BIGINT,
    customer   STRING,
    amount     DECIMAL(10, 2),
    discount   DOUBLE,
    express    BOOLEAN,
    order_date DATE,
    order_ts   TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(order_ts), bucket(4, order_id))
TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read',
               'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'{extra})
"""

INSERT = """
INSERT INTO {table}
SELECT id,
       concat('customer-with-a-long-name-', lpad(cast(id % 37 AS STRING), 3, '0')),
       CAST(id * 1.25 AS DECIMAL(10, 2)),
       IF(id % 5 = 0, NULL, id / 100.0),
       id % 2 = 0,
       date_add(DATE'2025-03-01', CAST(id % 3 AS INT)),
       timestamp_seconds(1740787200 + (id % 3) * 86400 + id)
FROM range({start}, {end})
"""


def create_orders(spark: SparkSession, table: str, version: int, **props: str) -> None:
    extra = "".join(f", '{k}'='{v}'" for k, v in props.items())
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(DDL.format(table=table, version=version, extra=extra))
    spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY order_id")


def insert_orders(spark: SparkSession, table: str, start: int, end: int) -> None:
    spark.sql(INSERT.format(table=table, start=start, end=end))


def run_workload(spark: SparkSession, table: str, version: int) -> None:
    """create, 2 appends, schema evolution, append, row-level delete (v2+)."""
    create_orders(spark, table, version)
    insert_orders(spark, table, 0, 300)
    insert_orders(spark, table, 300, 600)
    spark.sql(f"ALTER TABLE {table} ADD COLUMN channel STRING")
    spark.sql(f"INSERT INTO {table} SELECT order_id + 1000, customer, amount, discount, express, order_date, order_ts, 'web' "
              f"FROM {table} WHERE order_id < 30")
    if version >= 2:
        spark.sql(f"DELETE FROM {table} WHERE order_id % 50 = 7")

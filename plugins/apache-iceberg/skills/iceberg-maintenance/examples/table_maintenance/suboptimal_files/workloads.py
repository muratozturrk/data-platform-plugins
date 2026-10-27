"""Write workloads that degrade an Iceberg table, as they do in production.

- streaming / micro-batch appends -> small files, one snapshot + manifest per commit
- payment refund corrections via MERGE -> COW rewrites vs MOR delete files
"""
from pyspark.sql import SparkSession

from common.clickstream import create_events_table, micro_batch_ingest


def build_streaming_table(spark: SparkSession, table: str, batches: int, rows_per_batch: int,
                          day, files_per_batch: int = 4, **props: str) -> None:
    create_events_table(spark, table, **props)
    micro_batch_ingest(spark, table, batches, rows_per_batch, day, files_per_batch)


def set_row_level_mode(spark: SparkSession, table: str, mode: str) -> None:
    """mode: 'copy-on-write' or 'merge-on-read' for UPDATE, DELETE and MERGE."""
    spark.sql(f"""
        ALTER TABLE {table} SET TBLPROPERTIES (
          'write.update.mode' = '{mode}',
          'write.delete.mode' = '{mode}',
          'write.merge.mode'  = '{mode}'
        )
    """)


def apply_refunds(spark: SparkSession, table: str, every_nth_checkout: int, refund_tag: int = 0) -> int:
    """Refund corrections from a payments system: zero the amount of selected checkouts.

    Touches a small fraction of rows scattered across many files: the classic
    "sparse update" where MOR beats COW on write cost.
    Returns the number of corrected rows.
    """
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW refunds AS
        SELECT event_id FROM {table}
        WHERE event_type = 'checkout' AND amount > 0
          AND pmod(event_id + {refund_tag}, {every_nth_checkout}) = 0
    """)
    n = spark.table("refunds").count()
    spark.sql(f"""
        MERGE INTO {table} t
        USING refunds r
        ON t.event_id = r.event_id
        WHEN MATCHED THEN UPDATE SET t.amount = 0.0
    """)
    return n

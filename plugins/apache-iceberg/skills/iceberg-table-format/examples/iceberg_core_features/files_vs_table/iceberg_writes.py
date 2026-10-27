"""The same write patterns against an Iceberg table: failed jobs, concurrent jobs, stale writers."""
import io
from concurrent.futures import ThreadPoolExecutor

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from common.lakehouse import read_object


def append_that_fails(spark: SparkSession, table: str, df: DataFrame, poison_order_id: int) -> None:
    """Append where one Spark task throws after the other tasks already wrote their files.

    local[N] runs tasks once (spark.task.maxFailures=1), so the job aborts like a real
    job that hits bad data or loses an executor.
    """
    @F.udf(T.DoubleType())
    def validate(order_id, amount):
        if order_id == poison_order_id:
            raise ValueError(f"bad record {order_id}")
        return amount

    df.repartition(4).withColumn("amount", validate("order_id", "amount")).writeTo(table).append()


def concurrent_appends(spark: SparkSession, table: str, batches: list[DataFrame]) -> None:
    """Independent jobs appending to one table at the same time (one Spark job per thread).

    Each commit is an optimistic compare-and-swap on the catalog pointer; on conflict the
    Iceberg client re-reads the new metadata and retries (commit.retry.num-retries, default 4).
    """
    with ThreadPoolExecutor(max_workers=len(batches)) as pool:
        list(pool.map(lambda b: b.writeTo(table).append(), batches))


def parquet_footer_stats(uri: str, column: str) -> tuple:
    """(min, max, compressed bytes) of one column, read from a Parquet footer only."""
    md = pq.ParquetFile(io.BytesIO(read_object(uri))).metadata
    idx = md.schema.names.index(column)
    mins, maxs, size = [], [], 0
    for rg in range(md.num_row_groups):
        col = md.row_group(rg).column(idx)
        mins.append(col.statistics.min)
        maxs.append(col.statistics.max)
        size += col.total_compressed_size
    return min(mins), max(maxs), size

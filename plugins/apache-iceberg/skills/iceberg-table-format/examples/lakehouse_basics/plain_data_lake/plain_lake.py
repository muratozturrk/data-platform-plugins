"""A Hadoop-era data lake: a directory of Parquet files, coordinated only by convention.

The same three operations run against a plain directory and against an Iceberg table:
  1. a multi-file batch that crashes half way (readers must not see half a batch),
  2. an overwrite job that fails on a bad record (the previous data must survive),
  3. an upstream schema change (it must be caught at write time, not by a reader months later).
"""
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession, functions as F

from common.lakehouse import s3_client, split_uri, table_location


class WriterCrashed(RuntimeError):
    pass


def write_batch_files(directory: str, batch: pa.Table, n_files: int, crash_after: int | None = None) -> list[str]:
    """Write one logical batch as n Parquet files, the way a sink connector or a job's tasks do.

    The plain lake has no commit step: each file becomes visible to readers the moment it lands.
    """
    os.makedirs(directory, exist_ok=True)
    paths, rows_per_file = [], -(-batch.num_rows // n_files)
    for i in range(n_files):
        if crash_after is not None and i == crash_after:
            raise WriterCrashed(f"writer died after {i} of {n_files} files")
        path = f"{directory}/part-{i:05d}-{uuid.uuid4().hex}.parquet"
        pq.write_table(batch.slice(i * rows_per_file, rows_per_file), path)
        paths.append(path)
    return paths


def upload_uncommitted_files(spark: SparkSession, table: str, batch: pa.Table, n_files: int) -> list[str]:
    """Simulate the Iceberg equivalent of the crash: data files are already in object storage
    under the table's data/ prefix, but the writer died before the catalog commit."""
    bucket, prefix = split_uri(table_location(spark, table) + "/data/")
    keys, rows_per_file = [], -(-batch.num_rows // n_files)
    for i in range(n_files):
        sink = pa.BufferOutputStream()
        pq.write_table(batch.slice(i * rows_per_file, rows_per_file), sink)
        key = f"{prefix}crashed-{i:05d}-{uuid.uuid4().hex}.parquet"
        s3_client().put_object(Bucket=bucket, Key=key, Body=sink.getvalue().to_pybytes())
        keys.append(f"s3://{bucket}/{key}")
    return keys


def poison(df: DataFrame, bad_order_id: int) -> DataFrame:
    """A transformation that fails at run time on one bad record (raise_error, Spark 3.1+)."""
    return df.withColumn(
        "amount",
        F.when(F.col("order_id") == bad_order_id, F.raise_error(F.lit("bad record: negative amount")))
         .otherwise(F.col("amount")))


def overwrite_plain(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def overwrite_iceberg(df: DataFrame, table: str) -> None:
    # Dynamic overwrite: for an unpartitioned table this replaces the whole table in ONE commit.
    df.writeTo(table).overwritePartitions()

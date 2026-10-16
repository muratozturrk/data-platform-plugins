"""coalesce_write: size output files from data volume instead of a fixed coalesce(100).

sparkopt fires when partitions > 2000 and avg rows/partition < 10_000 (heuristic).
- cheap upstream    -> coalesce(n)     (no shuffle, but upstream runs with n tasks)
- heavy upstream or partitionBy -> repartition(...) (one shuffle, parallel upstream)

Run: python coalesce_write.py   (PySpark 3.3+)
"""
import glob
import math
import tempfile

from pyspark.sql import DataFrame, SparkSession, functions as F

TARGET_FILE_BYTES = 256 * 1024**2


def files_for(est_bytes: int, target: int = TARGET_FILE_BYTES) -> int:
    return max(1, math.ceil(est_bytes / target))


def write_sized(df: DataFrame, out: str, est_bytes: int, heavy_upstream: bool = False) -> None:
    n = files_for(est_bytes)
    shaped = df.repartition(n) if heavy_upstream else df.coalesce(n)
    shaped.write.mode("overwrite").parquet(out)


def write_partitioned(df: DataFrame, out: str, part_col: str, max_records: int = 5_000_000) -> None:
    """One task per partition value -> one (or a few, capped) file per directory."""
    (df.repartition(part_col)
       .write.option("maxRecordsPerFile", max_records)
       .partitionBy(part_col).mode("overwrite").parquet(out))


if __name__ == "__main__":
    assert files_for(0) == 1 and files_for(10 * 1024**3) == 40
    spark = SparkSession.builder.master("local[2]").appName("coalesce_write").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # keep the 2500 partitions for the demo
    base = tempfile.mkdtemp()
    df = spark.range(0, 50_000, numPartitions=2500).withColumn("dt", F.col("id") % 3)

    write_sized(df, f"{base}/flat", est_bytes=50_000 * 16)
    flat_files = glob.glob(f"{base}/flat/part-*")
    write_partitioned(df, f"{base}/by_dt", "dt")
    dt_files = glob.glob(f"{base}/by_dt/dt=*/part-*")
    print("flat files:", len(flat_files), "partitioned files:", len(dt_files))
    assert len(flat_files) == 1 and len(dt_files) == 3
    spark.stop()

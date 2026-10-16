"""Multi-stage pipeline: where to cache, where to checkpoint (sparkopt case: staged_materialization).

Persistence plan
  raw          -> no persist: cheap scan + filter, read once
  enriched     -> no persist: single consumer (the next stage)
  ranked       -> checkpoint(eager=True) to a reliable dir: after 3 shuffles the expensive,
                  long-lineage point; downstream failures restart from here, not from source
  summary      -> MEMORY_AND_DISK + count(): forks into two writes (persist before fork)
  eu / non_eu  -> nothing: cheap filters of a cached frame. Checkpointing them before the
                  write (what sparkopt generates) writes the same data twice for no gain.

Write atomicity comes from the output committer or the table format (Delta/Iceberg),
not from checkpoint().

Corrections vs. the sparkopt template: setCheckpointDir set once at startup to a durable path;
no persist on single-consumer frames; pre-write checkpoints removed; approx_count_distinct rsd
stated (default 0.05 = ~5% relative std. deviation). (Original sparkopt also lacked the
Window/F imports and used MEMORY_ONLY_SER; the bundled copy fixes both.)

Run: spark-submit staged_materialization.py --facts s3a://bucket/events/ \
       --dim-a s3a://bucket/users/ --dim-b s3a://bucket/products/ \
       --out s3a://bucket/marts/ --checkpoint-dir s3a://bucket/_ckpt/staged/
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main(a: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("staged-materialization").getOrCreate()
    spark.sparkContext.setCheckpointDir(a.checkpoint_dir)  # once, durable storage

    raw = spark.read.parquet(a.facts).where(F.col("is_valid"))
    dim_a = spark.read.parquet(a.dim_a).select("user_id", "region")
    dim_b = spark.read.parquet(a.dim_b).select("product_id", "category")

    # Shuffles 1-2: two joins (sort-merge if dims are large; AQE may convert to broadcast).
    enriched = (
        raw.join(dim_a, "user_id", "left")
        .join(dim_b, "product_id", "inner")
    )
    # Shuffle 3: window.
    w = Window.partitionBy("category").orderBy(F.desc("revenue"))
    ranked = enriched.withColumn("rank_in_category", F.rank().over(w))

    # Reliable checkpoint: truncates lineage and survives executor loss. Costs one write
    # to the checkpoint dir; Spark does not delete it (cleanup: see below).
    ranked = ranked.checkpoint(eager=True)

    summary = (
        ranked.groupBy("category", "region")
        .agg(
            F.sum("revenue").alias("total"),
            F.approx_count_distinct("user_id", rsd=0.05).alias("uniq_users"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    summary.count()  # materialize before the fork

    summary.where(F.col("region") == "EU").write.mode("overwrite").parquet(f"{a.out}/eu")
    (summary.where(F.col("region") != "EU").withColumn("flag", F.lit("non-EU"))
     .write.mode("overwrite").parquet(f"{a.out}/non_eu"))

    summary.unpersist()
    # Reliable checkpoint files outlive the job unless
    # spark.cleaner.referenceTracking.cleanCheckpoints=true (and the RDD was GC'd); add a
    # lifecycle rule on the checkpoint prefix.
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--facts", required=True)
    p.add_argument("--dim-a", required=True)
    p.add_argument("--dim-b", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--checkpoint-dir", required=True, help="HDFS/S3/ABFS path")
    main(p.parse_args())

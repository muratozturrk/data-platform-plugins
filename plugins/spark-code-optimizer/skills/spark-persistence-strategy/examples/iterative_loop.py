"""iterative_loop: truncate the plan with localCheckpoint every N iterations (PageRank-style loop).

Notes vs sparkopt's generated code:
  * no setCheckpointDir: localCheckpoint() does not use it;
  * no prev.unpersist() on a never-persisted DataFrame (it is a no-op). Old local checkpoint
    blocks are freed by ContextCleaner once the reference is dropped. (Original sparkopt emitted
    both; the bundled copy dropped them.)
  * loop-invariant input (links) is persisted once and released after the loop.
Set FAULT_TOLERANT=1 to use reliable checkpoint() instead (needs SPARK_CHECKPOINT_DIR on a cluster).

Run: spark-submit iterative_loop.py
"""
import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CHECKPOINT_INTERVAL = 5   # sparkopt default (IterativeLoopCase.checkpoint_interval)
MAX_ITER = 20


def plan_size(df):
    """Rough plan size: length of the logical plan string (grows without checkpoints)."""
    return len(df._jdf.queryExecution().logical().toString())


def main():
    spark = SparkSession.builder.master("local[2]").appName("iterative_loop").getOrCreate()
    fault_tolerant = os.environ.get("FAULT_TOLERANT") == "1"
    if fault_tolerant:
        spark.sparkContext.setCheckpointDir(
            os.environ.get("SPARK_CHECKPOINT_DIR", "file:///tmp/spark-ckpt"))

    def truncate(df):
        return df.checkpoint(eager=True) if fault_tolerant else df.localCheckpoint(eager=True)

    links = (spark.range(10_000)
             .select(F.col("id").alias("src"), ((F.col("id") * 7 + 3) % 10_000).alias("dst"))
             .persist(StorageLevel.MEMORY_AND_DISK))
    links.count()                                   # eager, reused every iteration

    ranks = truncate(spark.range(10_000).select(F.col("id").alias("page"), F.lit(1.0).alias("rank")))
    try:
        for i in range(MAX_ITER):
            contrib = (links.join(ranks, links.src == ranks.page)
                       .groupBy("dst").agg(F.sum("rank").alias("c")))
            ranks = contrib.select(F.col("dst").alias("page"),
                                   (F.lit(0.15) + F.lit(0.85) * F.col("c")).alias("rank"))
            if (i + 1) % CHECKPOINT_INTERVAL == 0:
                ranks = truncate(ranks)             # rebinding drops the old checkpoint reference
                print(f"iteration {i + 1}: checkpoint, plan size {plan_size(ranks)}")
        print(ranks.orderBy(F.desc("rank")).limit(5).collect())
    finally:
        links.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

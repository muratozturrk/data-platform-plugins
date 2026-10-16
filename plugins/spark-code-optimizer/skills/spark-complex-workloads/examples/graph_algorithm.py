"""Iterative PageRank on DataFrames with localCheckpoint every N iterations
(sparkopt case: graph_algorithm).

Persistence plan
  edges (+ out_degree) -> MEMORY_AND_DISK, read every iteration, never changes
  vertices             -> MEMORY_AND_DISK, small, read every iteration
  ranks                -> localCheckpoint every N iterations (truncates the plan that
                          otherwise grows by one join+aggregate per iteration)

Corrections vs. the sparkopt template: no MEMORY_ONLY_SER; no setCheckpointDir (not needed
for localCheckpoint); join keys fixed (src/dst vs id); out_degree computed; teleport term
uses (1-d)/N to match the 1/N start; convergence uses the L1
delta between two materialized rank vectors instead of the near-constant rank sum;
first checkpoint at (i+1) % N == 0, not at iteration 0.

ponytail: dangling-node rank mass is not redistributed (same as Spark's bundled PageRank
example). Add self-loops for sink vertices up front if you need ranks to sum to 1.

Run: spark-submit graph_algorithm.py --edges s3a://bucket/edges/ --out s3a://bucket/pagerank/
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DAMPING = 0.85


def pagerank(spark: SparkSession, edges_raw: DataFrame, max_iter: int, ckpt_every: int,
             tol: float) -> DataFrame:
    edges_raw = edges_raw.select("src", "dst").distinct()
    vertices = (
        edges_raw.select(F.col("src").alias("id"))
        .union(edges_raw.select(F.col("dst").alias("id")))
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    n = vertices.count()  # one action, reused below (do not call count() inside lit())

    out_deg = edges_raw.groupBy("src").agg(F.count("*").alias("out_degree"))
    edges = edges_raw.join(out_deg, "src").persist(StorageLevel.MEMORY_AND_DISK)
    edges.count()

    # localCheckpoint: executor-local blocks, no checkpoint dir required, plan truncated.
    ranks = vertices.withColumn("rank", F.lit(1.0 / n)).localCheckpoint(eager=True)
    last_ckpt = ranks

    for i in range(max_iter):
        contribs = (
            ranks.join(edges, ranks.id == edges.src)
            .groupBy(F.col("dst").alias("id"))
            .agg(F.sum(F.col("rank") / F.col("out_degree")).alias("incoming"))
        )
        # ranks is referenced exactly ONCE per iteration. Referencing it twice (e.g. a
        # separate dangling-mass aggregate) doubles the un-checkpointed plan every
        # iteration: 2^N subtrees between checkpoints.
        ranks = vertices.join(contribs, "id", "left").select(
            "id",
            ((1 - DAMPING) / n + DAMPING * F.coalesce("incoming", F.lit(0.0))).alias("rank"),
        )

        if (i + 1) % ckpt_every == 0:
            ranks = ranks.localCheckpoint(eager=True)
            # Both vectors are materialized, so this check reads blocks, not lineage.
            delta = (
                ranks.alias("c").join(last_ckpt.alias("p"), "id")
                .agg(F.sum(F.abs(F.col("c.rank") - F.col("p.rank"))))
                .first()[0]
            )
            print(f"iter={i + 1} L1 delta over last {ckpt_every} iters={delta:.3e}")
            # ponytail: DataFrame.unpersist() does not free localCheckpoint blocks; dropping
            # the reference lets the ContextCleaner reclaim them on GC.
            last_ckpt = ranks
            if delta < tol:
                break

    vertices.unpersist()
    edges.unpersist()
    return ranks


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--edges", required=True, help="parquet with columns src, dst")
    p.add_argument("--out", required=True)
    p.add_argument("--max-iter", type=int, default=30)
    p.add_argument("--checkpoint-every", type=int, default=5)
    p.add_argument("--tol", type=float, default=1e-6)
    a = p.parse_args()

    spark = SparkSession.builder.appName("pagerank-df").getOrCreate()
    ranks = pagerank(spark, spark.read.parquet(a.edges), a.max_iter, a.checkpoint_every, a.tol)
    ranks.write.mode("overwrite").parquet(a.out)
    spark.stop()


if __name__ == "__main__":
    main()

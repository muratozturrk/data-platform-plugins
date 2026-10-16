"""Incremental upsert with Delta MERGE (sparkopt case: incremental_etl).

Persistence plan
  target table   -> NOT persisted. MERGE prunes it to the files that contain matching keys;
                    caching the whole table (what sparkopt generates) costs a full scan.
  updates (delta)-> MEMORY_AND_DISK + count(): small, deduplicated, reused by the audit
                    count and by MERGE (which reads its source more than once)
  merge result   -> no checkpoint. Delta commits atomically through its transaction log;
                    a Spark checkpoint gives no write atomicity.

Corrections vs. the sparkopt template: no *_SER levels; no persist of the full target;
no overwrite of the path being read (AnalysisException, and non-atomic on parquet);
no F.hash(*cols) over two aliased frames (ambiguous columns); deletes come from an explicit
`op` flag, not from "key missing in this increment" (an increment only holds changes).

Requires delta-spark matching your Spark version, e.g.
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    incremental_etl.py --source s3a://bucket/cdc/date=2026-10-15/ --target s3a://bucket/customers_delta/
"""
import argparse

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

KEY = "customer_id"
VALUE_COLS = ["name", "email", "tier", "country"]


def main(source: str, target: str) -> None:
    spark = SparkSession.builder.appName("incremental-merge").getOrCreate()

    # CDC increment: one row per change with op in (I, U, D) and a change timestamp.
    raw = spark.read.parquet(source)

    # MERGE fails if several source rows match one target row -> keep the latest per key.
    latest = Window.partitionBy(KEY).orderBy(F.col("changed_at").desc())
    updates = (
        raw.withColumn("_rn", F.row_number().over(latest))
        .where("_rn = 1")
        .drop("_rn")
        .select(KEY, *VALUE_COLS, "op", "changed_at")
    )
    # Persist the small, deterministic source: reused by the count and by MERGE.
    updates.persist(StorageLevel.MEMORY_AND_DISK)
    n_changes = updates.count()
    if n_changes == 0:
        print("no changes")
        return

    if not DeltaTable.isDeltaTable(spark, target):
        (updates.where("op != 'D'").drop("op")
         .write.format("delta").partitionBy("country").save(target))
        updates.unpersist()
        return

    # Null-safe "something changed" check: skips no-op updates, which would otherwise
    # rewrite files for nothing.
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in VALUE_COLS)
    set_cols = {c: f"s.{c}" for c in VALUE_COLS} | {"changed_at": "s.changed_at"}

    # Add a partition/date predicate to the ON clause when you can (e.g. t.country IN (...))
    # so MERGE prunes target files instead of scanning the whole table.
    (
        DeltaTable.forPath(spark, target).alias("t")
        .merge(updates.alias("s"), f"t.{KEY} = s.{KEY}")
        .whenMatchedDelete(condition="s.op = 'D'")
        .whenMatchedUpdate(condition=f"s.op != 'D' AND ({changed})", set=set_cols)
        .whenNotMatchedInsert(condition="s.op != 'D'", values={KEY: f"s.{KEY}", **set_cols})
        .execute()
    )

    metrics = (DeltaTable.forPath(spark, target).history(1)
               .select("operationMetrics").first()[0])
    print(f"changes={n_changes} merge metrics={metrics}")

    updates.unpersist()
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True)
    a = p.parse_args()
    main(a.source, a.target)

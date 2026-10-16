"""Small, frequently joined, slowly changing lookup: cache + broadcast + refresh on change
(sparkopt case: cached_broadcast).

Persistence plan
  lookup  -> MEMORY_AND_DISK + count(), refreshed only when its Delta version changes
             (or after max_age_s as a fallback). Cache saves the source read + transforms
             for every batch; F.broadcast() avoids shuffling the large side.
  batch   -> no persist: each micro-batch is joined and written once.

What cache does NOT do: Spark re-collects and re-broadcasts the lookup for every query /
micro-batch; there is no cross-query broadcast reuse. Cache only makes that collect cheap.

Corrections vs. the sparkopt template: no MEMORY_ONLY_SER; `batch_stream` replaced by a real
foreachBatch stream; TTL-by-call-count replaced by version check; writes go to one table
instead of one directory per batch.

Run: spark-submit --packages io.delta:delta-spark_2.12:3.2.0 ... cached_broadcast.py \
       --events s3a://bucket/events_delta/ --lookup s3a://bucket/products_delta/ \
       --out s3a://bucket/enriched/ --checkpoint s3a://bucket/_chk/enriched/
"""
import argparse
import time

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class VersionedLookup:
    """Holds one cached snapshot of a Delta lookup table; reloads when the table changes."""

    def __init__(self, spark: SparkSession, path: str, max_age_s: int = 3600):
        self.spark, self.path, self.max_age_s = spark, path, max_age_s
        self.df, self.version, self.loaded_at = None, None, 0.0

    def _current_version(self) -> int:
        return DeltaTable.forPath(self.spark, self.path).history(1).select("version").first()[0]

    def get(self) -> DataFrame:
        version = self._current_version()
        stale = time.time() - self.loaded_at > self.max_age_s
        if self.df is None or version != self.version or stale:
            fresh = (
                self.spark.read.format("delta").option("versionAsOf", version).load(self.path)
                .select("product_id", "category", "list_price")
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            fresh.count()  # materialize the new snapshot before dropping the old one
            if self.df is not None:
                self.df.unpersist()
            self.df, self.version, self.loaded_at = fresh, version, time.time()
            print(f"lookup reloaded: version={version}")
        return self.df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--events", required=True)
    p.add_argument("--lookup", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--checkpoint", required=True, help="durable path: HDFS/S3/ABFS")
    a = p.parse_args()

    spark = SparkSession.builder.appName("cached-broadcast-lookup").getOrCreate()
    lookup = VersionedLookup(spark, a.lookup)

    def enrich(batch_df: DataFrame, batch_id: int) -> None:
        dim = lookup.get()
        # Explicit hint: the cached size estimate may exceed autoBroadcastJoinThreshold (10MB).
        (batch_df.join(F.broadcast(dim), "product_id", "left")
         .withColumn("batch_id", F.lit(batch_id))
         .write.format("delta").mode("append").save(a.out))

    (spark.readStream.format("delta").load(a.events)
     .writeStream.foreachBatch(enrich)
     .option("checkpointLocation", a.checkpoint)
     .trigger(processingTime="1 minute")
     .start()
     .awaitTermination())


if __name__ == "__main__":
    main()

"""hybrid_checkpoint: stream x static join with the right persistence for each part.

* static lookup: persist + count() once, broadcast into the stream join (frozen snapshot;
  do not cache if the table must stay fresh);
* streaming DataFrame: cannot be cached/localCheckpointed; per-batch reuse goes in foreachBatch;
* query progress: checkpointLocation, one stable directory per query, never deleted while the
  query may restart.
Uses the built-in `rate` source so it runs without Kafka. For Kafka swap in
spark.readStream.format("kafka").option("kafka.bootstrap.servers", ...).option("subscribe", ...).

Run: spark-submit hybrid_checkpoint.py [base_dir]
"""
import sys
import tempfile

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(base_dir):
    spark = SparkSession.builder.master("local[2]").appName("hybrid_checkpoint").getOrCreate()

    lookup = (spark.range(100)
              .select(F.col("id").alias("product_id"),
                      F.concat(F.lit("product-"), F.col("id").cast("string")).alias("name"))
              .persist(StorageLevel.MEMORY_AND_DISK))       # original sparkopt: MEMORY_ONLY_SER
    lookup.count()                                           # eager, before the stream starts

    events = (spark.readStream.format("rate").option("rowsPerSecond", 50).load()
              .select(F.col("timestamp").alias("event_time"),
                      (F.col("value") % 100).alias("product_id"),
                      (F.col("value") % 1500).alias("amount")))

    enriched = events.join(F.broadcast(lookup), "product_id", "left")

    def write_batch(batch_df, batch_id):
        batch_df.persist(StorageLevel.MEMORY_AND_DISK)       # batch_df is a static DataFrame
        try:
            batch_df.write.mode("append").parquet(f"{base_dir}/events")
            batch_df.filter("amount > 1000").write.mode("append").parquet(f"{base_dir}/alerts")
        finally:
            batch_df.unpersist()

    query = (enriched.writeStream
             .foreachBatch(write_batch)
             .option("checkpointLocation", f"{base_dir}/_checkpoints/enrich_events")
             .trigger(processingTime="5 seconds")
             .start())
    try:
        query.awaitTermination(20)                           # demo: run ~20 seconds
    finally:
        query.stop()
        lookup.unpersist()
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="hybrid_"))

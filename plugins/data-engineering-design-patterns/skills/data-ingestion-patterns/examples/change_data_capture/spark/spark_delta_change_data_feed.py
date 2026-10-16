"""Change Data Capture with Delta Lake Change Data Feed.

Lake-native CDC: no Debezium, no replication slot. A table created with
``delta.enableChangeDataFeed = true`` records every row change; a reader with ``readChangeFeed`` gets the rows
plus ``_change_type`` (insert, update_preimage, update_postimage, delete), ``_commit_version`` and
``_commit_timestamp``.

This job consumes the feed of ``events`` in Structured Streaming and keeps a downstream table in sync:
  * ``update_preimage`` rows are dropped (old values, only useful for auditing);
  * inside a micro-batch only the latest change per key survives (highest ``_commit_version``);
  * MERGE applies inserts/updates and physical deletes (the hard-delete support the Incremental Loader lacks).

Throughput is bounded with ``maxFilesPerTrigger`` (or ``maxBytesPerTrigger``); ``startingVersion`` picks the
first table version to read. The CDF only contains changes made after it was enabled: bootstrap older rows
with a Full Loader first.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_delta_change_data_feed.py \
      --source /lake/events --target /lake/events_replica --checkpoint /chk/events_cdf [--create-source]
"""
import argparse
import logging
import sys

log = logging.getLogger("delta_cdf")
KEY = "visit_id"


def build_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("delta-cdf-consumer")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # session-wide alternative to the table property
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
            .getOrCreate())


def create_source(spark, path: str) -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{path}` (
        visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING)
        USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")


def ensure_target(spark, path: str) -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{path}` (
        visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        _source_version LONG) USING DELTA""")


def build_upsert(target_path: str):
    def upsert(changes, batch_id: int) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql import Window, functions as F

        latest = (changes.where(F.col("_change_type") != "update_preimage")
                  .withColumn("_rn", F.row_number().over(
                      Window.partitionBy(KEY).orderBy(F.col("_commit_version").desc())))
                  .where("_rn = 1").drop("_rn"))
        target = DeltaTable.forPath(changes.sparkSession, target_path)
        (target.alias("t").merge(latest.alias("s"), f"t.{KEY} = s.{KEY}")
         # replayed batch after a failure: never apply an older version over a newer one
         .whenMatchedDelete(condition="s._change_type = 'delete' AND s._commit_version >= t._source_version")
         .whenMatchedUpdate(condition="s._change_type != 'delete' AND s._commit_version >= t._source_version",
                            set={"event_time": "s.event_time", "user_id": "s.user_id", "page": "s.page",
                                 "_source_version": "s._commit_version"})
         .whenNotMatchedInsert(condition="s._change_type != 'delete'",
                               values={KEY: f"s.{KEY}", "event_time": "s.event_time", "user_id": "s.user_id",
                                       "page": "s.page", "_source_version": "s._commit_version"})
         .execute())
        log.info("batch %d applied", batch_id)
    return upsert


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--starting-version", default="0")
    p.add_argument("--max-files-per-trigger", default="4")
    p.add_argument("--create-source", action="store_true", help="create the demo source table with CDF on")
    args = p.parse_args(argv)

    spark = build_spark()
    if args.create_source:
        create_source(spark, args.source)
    ensure_target(spark, args.target)

    events = (spark.readStream.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", args.starting_version)
              .option("maxFilesPerTrigger", args.max_files_per_trigger)
              .load(args.source))

    query = (events.writeStream
             .option("checkpointLocation", args.checkpoint)
             .foreachBatch(build_upsert(args.target))
             .trigger(processingTime="30 seconds")       # 30 s latency budget
             .start())
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())

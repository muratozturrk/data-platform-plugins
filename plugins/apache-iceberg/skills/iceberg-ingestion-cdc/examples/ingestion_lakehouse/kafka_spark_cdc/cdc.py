"""Kafka (Debezium envelopes) -> Spark Structured Streaming -> Iceberg MERGE.

Per micro-batch: drop tombstones, keep the latest change per key (by source LSN), then one
MERGE INTO with an LSN guard so replays and late, older events never overwrite newer state.
Pattern: Spark foreachBatch + Iceberg MERGE, https://iceberg.apache.org/docs/latest/spark-writes/
"""
from pyspark.sql import DataFrame, Window, functions as F

from common.lakehouse import read_kafka

ROW = "id BIGINT, name STRING, email STRING, tier STRING"
ROW_STRUCT = "STRUCT<id: BIGINT, name: STRING, email: STRING, tier: STRING>"
ENVELOPE = (f"before {ROW_STRUCT}, after {ROW_STRUCT}, op STRING, ts_ms BIGINT, "
            "source STRUCT<db: STRING, table: STRING, lsn: BIGINT, ts_ms: BIGINT>")


def create_customers(spark, table) -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} ({ROW}, _lsn BIGINT, _source_ts TIMESTAMP)
                  USING iceberg PARTITIONED BY (bucket(4, id))
                  TBLPROPERTIES ('format-version'='2',
                                 'write.merge.mode'='merge-on-read',
                                 'write.update.mode'='merge-on-read',
                                 'write.delete.mode'='merge-on-read')""")


def latest_changes(batch: DataFrame) -> DataFrame:
    changes = (batch.where("value IS NOT NULL")              # tombstones carry no change
               .select(F.from_json(F.col("value").cast("string"), ENVELOPE).alias("e"))
               .select(F.coalesce("e.after.id", "e.before.id").alias("id"),
                       F.col("e.op").alias("op"),
                       "e.after.name", "e.after.email", "e.after.tier",
                       F.col("e.source.lsn").alias("_lsn"),
                       F.timestamp_millis("e.source.ts_ms").alias("_source_ts")))
    newest_first = Window.partitionBy("id").orderBy(F.col("_lsn").desc())
    return (changes.withColumn("rn", F.row_number().over(newest_first))
            .where("rn = 1").drop("rn"))


def cdc_merge_handler(target, metrics=None):
    def handle(batch: DataFrame, epoch_id: int) -> None:
        spark = batch.sparkSession
        latest_changes(batch).createOrReplaceTempView("cdc_changes")
        spark.sql(f"""
            MERGE INTO {target} t USING cdc_changes s ON t.id = s.id
            WHEN MATCHED AND s._lsn > t._lsn AND s.op = 'd' THEN DELETE
            WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET
                 t.name = s.name, t.email = s.email, t.tier = s.tier,
                 t._lsn = s._lsn, t._source_ts = s._source_ts
            WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
                 (id, name, email, tier, _lsn, _source_ts)
                 VALUES (s.id, s.name, s.email, s.tier, s._lsn, s._source_ts)""")
        if metrics is not None:
            metrics.append({"epoch_id": epoch_id, "kafka_records": batch.count()})
    return handle


def start_cdc(spark, topic, target, checkpoint, metrics=None, **kafka_opts):
    return (read_kafka(spark, topic, **kafka_opts).writeStream
            .foreachBatch(cdc_merge_handler(target, metrics))
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start())


def table_state(spark, table) -> dict[int, tuple]:
    return {r.id: (r.id, r.name, r.email, r.tier)
            for r in spark.table(table).select("id", "name", "email", "tier").collect()}


# --- Append-only event stream into a partitioned table: fanout writer ----------------------
def create_events(spark, table) -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table}
                  (event_id BIGINT, country STRING, event_ts TIMESTAMP)
                  USING iceberg PARTITIONED BY (country, days(event_ts))""")


def start_partitioned_append(spark, topic, table, checkpoint, distribution: str, fanout: bool):
    """distribution: Iceberg write distribution-mode ('hash' shuffles rows by partition before
    writing, 'none' writes them in arrival order). fanout keeps one open file per partition per
    task, so unclustered input needs no shuffle or sort."""
    events = (read_kafka(spark, topic)
              .select(F.from_json(F.col("value").cast("string"),
                                  "event_id BIGINT, country STRING, event_ts TIMESTAMP").alias("e"))
              .select("e.*"))
    return (events.writeStream.format("iceberg").outputMode("append")
            .option("distribution-mode", distribution)
            .option("fanout-enabled", str(fanout).lower())
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(table))

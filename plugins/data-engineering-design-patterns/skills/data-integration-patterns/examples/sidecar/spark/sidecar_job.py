"""Sidecar - many inputs and many outputs in one Structured Streaming job (PySpark + Delta).

Input side (fan-in):
  * two Kafka topics with DIFFERENT field names are harmonized to one schema BEFORE
    unionByName - no CASE WHEN src = ... branches later;
  * a heartbeat `rate-micro-batch` source (filtered to zero rows) keeps micro-batches
    running even when the topics are idle.
Output side (fan-out, Parallel Split semantics):
  * foreachBatch writes the raw enriched rows AND an aggregate to two Delta tables;
  * persist() so the input is computed once, not once per writer;
  * txnAppId/txnVersion make each write idempotent when a failed batch is retried
.

Run:  spark-submit --packages io.delta:delta-spark_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 sidecar_job.py
Env:  KAFKA_BOOTSTRAP, OUTPUT_DIR
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/tmp/dp/sidecar")
APP_ID = "visits-sidecar-v1"  # change only together with the checkpoint location


def read_topic(spark: SparkSession, topic: str, schema: str) -> DataFrame:
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic).load()
            .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*"))


def harmonized_input(spark: SparkSession) -> DataFrame:
    web = (read_topic(spark, "visits-web", "visit_id STRING, event_ts TIMESTAMP, device STRING")
           .select("visit_id", F.col("event_ts").alias("event_time"), F.col("device").alias("type"),
                   F.lit("web").alias("source")))
    mobile = (read_topic(spark, "visits-mobile", "id STRING, event_timestamp TIMESTAMP, os STRING")
              .select(F.col("id").alias("visit_id"), F.col("event_timestamp").alias("event_time"),
                      F.col("os").alias("type"), F.lit("mobile").alias("source")))
    heartbeat = (spark.readStream.format("rate-micro-batch").options(rowsPerBatch=1, numPartitions=1)
                 .load().filter("false").select(F.lit(None).cast("string").alias("visit_id")))
    return web.unionByName(mobile).unionByName(heartbeat, allowMissingColumns=True)


def write_enriched_visits(visits: DataFrame, batch_number: int) -> None:
    visits.persist()
    options = {"txnVersion": str(batch_number), "txnAppId": APP_ID}
    visits.write.format("delta").mode("append").options(**options).save(f"{OUTPUT_DIR}/visits_raw")
    devices_stats = (visits.groupBy("source", "type").count().withColumn("batch_id", F.lit(batch_number)))
    devices_stats.write.format("delta").mode("append").options(**options).save(f"{OUTPUT_DIR}/devices_stats")
    visits.unpersist()


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("visits_sidecar")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    (harmonized_input(spark).writeStream.foreachBatch(write_enriched_visits)
     .option("checkpointLocation", f"{OUTPUT_DIR}/_checkpoint").trigger(processingTime="1 minute")
     .start().awaitTermination())

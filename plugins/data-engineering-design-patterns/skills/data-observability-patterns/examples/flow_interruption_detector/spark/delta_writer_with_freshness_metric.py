"""Flow Interruption Detector - producer-side freshness metric (PySpark + Delta Lake).

A Structured Streaming job copies Kafka records into a Delta table. After every
micro-batch that ACTUALLY wrote rows, it pushes `visits_last_update_time` to a
Prometheus Pushgateway. Alert rule (Prometheus/Grafana):

    time() - visits_last_update_time > 3600   # no new data for 1 hour

`last_data_change_time()` is the storage/metadata-layer alternative: it reads the
Delta history and ignores housekeeping commits (OPTIMIZE, VACUUM, schema changes)
which would otherwise hide an interruption (false "activity").

Run:
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 delta_writer_with_freshness_metric.py
Env: KAFKA_BOOTSTRAP, PUSHGATEWAY_URL, DELTA_TABLE_PATH, CHECKPOINT_DIR
Deps: pyspark, delta-spark, prometheus-client
"""
import os
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "localhost:9091")
TABLE_PATH = os.environ.get("DELTA_TABLE_PATH", "/tmp/dp/visits_delta")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/dp/checkpoints/visits_delta")

# Commits that change data. Anything else (OPTIMIZE, VACUUM, SET TBLPROPERTIES...) is housekeeping.
DATA_OPERATIONS = ("WRITE", "STREAMING UPDATE", "MERGE", "UPDATE", "DELETE", "CREATE TABLE AS SELECT")


def push_last_update_time() -> None:
    registry = CollectorRegistry()
    gauge = Gauge("visits_last_update_time", "Last data write time of the visits Delta table",
                  registry=registry)
    gauge.set_to_current_time()
    push_to_gateway(PUSHGATEWAY_URL, job="visits_table_ingestor", registry=registry)


def write_and_report(batch: DataFrame, batch_id: int) -> None:
    batch.persist()
    # The silent failure scenario: the job runs, but writes nothing. Only real writes refresh the metric.
    if batch.count() > 0:
        batch.write.format("delta").mode("append").save(TABLE_PATH)
        push_last_update_time()
    batch.unpersist()


def last_data_change_time(spark: SparkSession, table_path: str) -> datetime | None:
    """Metadata-layer check: last commit that really changed data (compaction excluded)."""
    history = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
    row = (history.filter(F.col("operation").isin(*DATA_OPERATIONS))
           .agg(F.max("timestamp").alias("last_change")).collect()[0])
    return row.last_change


def main() -> None:
    spark = (SparkSession.builder.appName("visits_delta_ingestor")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())

    visits = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribe", "visits").option("startingOffsets", "earliest").load()
              .selectExpr("CAST(key AS STRING) AS visit_id", "CAST(value AS STRING) AS payload",
                          "timestamp AS ingestion_time"))

    query = (visits.writeStream.foreachBatch(write_and_report)
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime="1 minute").start())

    try:
        query.awaitTermination()
    finally:
        last = last_data_change_time(spark, TABLE_PATH)
        lag = (datetime.now(timezone.utc).replace(tzinfo=None) - last) if last else None
        print(f"Last data change: {last}, age: {lag}")


if __name__ == "__main__":
    main()

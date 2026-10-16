"""Hybrid Source - backfill from the lakehouse, then switch to Kafka (PySpark).

Spark has no HybridSource class (Flink does), so the chain is two queries in one driver:
  1. historical: Delta table, CLOSED boundary (explicit ingestion runs), availableNow +
     maxFilesPerTrigger -> incremental, checkpointed, retry-safe;
  2. real-time: Kafka from the switch timestamp (`startingTimestamp`) so the two
     datasets do not overlap (no interleaved/duplicated records).
Both phases share `to_common_schema` (schema harmonization) and the same sink.

Limitation: state is NOT carried between the two queries. For stateful logic (sessions)
either use Flink's HybridSource, or make the sink idempotent (MERGE) and accept that
sessions spanning the switch are rebuilt (see the reference file).

Run:  spark-submit --packages io.delta:delta-spark_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 hybrid_source_job.py
Env:  KAFKA_BOOTSTRAP, SWITCH_TIMESTAMP_MS (= end of the historical dataset), OUTPUT_DIR
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
SWITCH_TIMESTAMP_MS = os.environ.get("SWITCH_TIMESTAMP_MS", "1762144261000")
HISTORICAL_RUNS = ("run1", "run2", "run3")  # technical boundary, not a business filter
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/tmp/dp/sessions_input")
CHECKPOINTS = "/tmp/dp/checkpoints/hybrid"
VISIT_SCHEMA = "visit_id STRING, user_id STRING, page STRING, event_time TIMESTAMP"


def to_common_schema(df: DataFrame, origin: str) -> DataFrame:
    return df.select("visit_id", "user_id", "page", "event_time", F.lit(origin).alias("origin"))


def run_historical(spark: SparkSession) -> None:
    historical = (spark.readStream.format("delta").option("maxFilesPerTrigger", 100).table("visits")
                  .filter(F.col("ingestion_run").isin(*HISTORICAL_RUNS)))
    (to_common_schema(historical, "historical").writeStream.format("parquet")
     .option("path", OUTPUT_DIR).option("checkpointLocation", f"{CHECKPOINTS}/historical")
     .trigger(availableNow=True).start().awaitTermination())


def run_real_time(spark: SparkSession) -> None:
    kafka = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("subscribe", "visits").option("startingTimestamp", SWITCH_TIMESTAMP_MS).load()
             .select(F.from_json(F.col("value").cast("string"), VISIT_SCHEMA).alias("v")).select("v.*"))
    (to_common_schema(kafka, "real_time").writeStream.format("parquet")
     .option("path", OUTPUT_DIR).option("checkpointLocation", f"{CHECKPOINTS}/real_time")
     .trigger(processingTime="30 seconds").start().awaitTermination())


if __name__ == "__main__":
    session = (SparkSession.builder.appName("visits_hybrid_source")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .getOrCreate())
    run_historical(session)  # a restart re-runs this quickly: the checkpoint says "nothing left"
    run_real_time(session)

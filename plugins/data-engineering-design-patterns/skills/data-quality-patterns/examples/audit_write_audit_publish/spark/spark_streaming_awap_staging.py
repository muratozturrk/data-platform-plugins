"""Audit-Write-Audit-Publish (AWAP) - staging-based streaming variant, PySpark + Delta Lake.

Job 1 (`staging`): Kafka visits -> Delta staging table, no logic change.
Job 2 (`audit`):  stream the staging table, add an `is_valid` row expression, then in
foreachBatch apply a dataset-level audit and dispatch rows to the final or errors table.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      spark_streaming_awap_staging.py staging|audit
Env: KAFKA_BOOTSTRAP (default localhost:9094), BASE_DIR (default /tmp/dp/awap).
"""
import os
import sys

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
BASE_DIR = os.environ.get("BASE_DIR", "/tmp/dp/awap")
STAGING, OUTPUT, ERRORS = (f"{BASE_DIR}/{n}" for n in ("staging_visits", "visits", "visits_errors"))
MAX_INVALID_RATIO = 0.2  # dataset-level rule: fail the micro-batch above this ratio

VISIT_SCHEMA = StructType([
    StructField("visit_id", StringType()), StructField("event_time", TimestampType()),
    StructField("user_id", StringType()), StructField("page", StringType()),
])

spark = (SparkSession.builder.appName("awap-staging")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())


def run_staging_job():
    visits = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP).option("subscribe", "visits")
              .option("startingOffsets", "EARLIEST").option("maxOffsetsPerTrigger", "50").load()
              .select(F.from_json(F.col("value").cast("string"), VISIT_SCHEMA).alias("visit"))
              .selectExpr("visit.*"))
    return (visits.writeStream.format("delta").trigger(processingTime="15 seconds")
            .option("checkpointLocation", f"{BASE_DIR}/checkpoint_staging").start(STAGING))


row_validation_expression = (
    F.col("visit_id").isNotNull() & F.col("user_id").isNotNull() & F.col("page").isNotNull()
    & F.col("event_time").isNotNull() & (F.col("event_time") <= F.current_timestamp())
)


def audit_dataset_and_write_to_output_table(batch: DataFrame, batch_id: int) -> None:
    batch.persist()
    total = batch.count()
    if total == 0:
        batch.unpersist()
        return
    invalid = batch.filter(~F.col("is_valid")).count()
    if invalid / total > MAX_INVALID_RATIO:
        batch.unpersist()
        # fail strategy: stop the stream; checkpoint not committed -> batch retried after fix
        raise RuntimeError(f"Batch {batch_id}: {invalid}/{total} invalid rows > {MAX_INVALID_RATIO:.0%}")
    # dispatch strategy: valid rows published, invalid rows kept aside
    # txnAppId/txnVersion make the Delta writes idempotent on batch retry
    for path, cond in ((OUTPUT, F.col("is_valid")), (ERRORS, ~F.col("is_valid"))):
        (batch.filter(cond).drop("is_valid").write.format("delta").mode("append")
         .option("txnAppId", f"awap-{os.path.basename(path)}").option("txnVersion", batch_id).save(path))
    batch.unpersist()


def run_audit_job():
    visits = (spark.readStream.format("delta").option("maxBytesPerTrigger", 20_000_000).load(STAGING)
              .withColumn("is_valid", F.coalesce(row_validation_expression, F.lit(False))))
    return (visits.writeStream.trigger(processingTime="30 seconds")
            .option("checkpointLocation", f"{BASE_DIR}/checkpoint_audit")
            .foreachBatch(audit_dataset_and_write_to_output_table).start())


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "audit"
    (run_staging_job() if mode == "staging" else run_audit_job()).awaitTermination()

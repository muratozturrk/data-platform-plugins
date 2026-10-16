"""Batch Reprocessor - shared business logic, decoupled from inputs and outputs.

The transformation lives here, so the real-time job
(Kafka -> Kafka) and the batch reprocessor (lakehouse -> Kafka) run exactly the same
code. Only I/O and the origin decoration differ (Template-style: shared steps +
one varying decoration step).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VISIT_SCHEMA = "visit_id STRING, user_id STRING, page STRING, event_time TIMESTAMP"


def extract_columns(raw_json_values: DataFrame) -> DataFrame:
    """Input: a `value` column with the raw JSON (Kafka value or raw lakehouse copy)."""
    return (raw_json_values.select(F.from_json(F.col("value").cast("string"), VISIT_SCHEMA).alias("v"))
            .select("v.*").filter(F.col("visit_id").isNotNull())
            .withColumn("page", F.lower(F.trim("page"))))


def to_kafka_records(visits: DataFrame, origin: str) -> DataFrame:
    """Metadata Decorator: origin goes to a header, the payload stays identical."""
    return visits.select(
        F.col("visit_id").alias("key"), F.to_json(F.struct("*")).alias("value"),
        F.array(F.struct(F.lit("origin").alias("key"), F.lit(origin).cast("binary").alias("value"))).alias("headers"))

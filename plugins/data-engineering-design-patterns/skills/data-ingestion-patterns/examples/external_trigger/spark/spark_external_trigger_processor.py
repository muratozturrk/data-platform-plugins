# spark_external_trigger_processor.py
#
# External Trigger processor: reads trigger events from a PostgreSQL
# trigger queue table, processes the triggered dataset with Spark,
# and marks the trigger as acknowledged only after success.
#
# Trigger queue pattern:
#   - External system inserts a row into trigger_queue
#   - This job polls trigger_queue for pending triggers
#   - Processes the triggered dataset
#   - Updates trigger status to 'completed' or 'failed'
#
# Run:
#   spark-submit \
#     --packages io.delta:delta-core_2.12:2.4.0 \
#     spark_external_trigger_processor.py
#
# Requirements: delta-spark, pyspark, psycopg2-binary

import os
import uuid
import logging
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional
import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

DB_CONN = dict(
    host="state-db", dbname="pipeline_state",
    user="pipeline", password=os.environ["DB_PASSWORD"]
)

BRONZE_PATH     = "/data/raw/orders"
SILVER_PATH     = "/data/silver/orders"

# DDL — run once to create the trigger queue:
TRIGGER_QUEUE_DDL = """
CREATE TABLE IF NOT EXISTS trigger_queue (
    id              SERIAL PRIMARY KEY,
    trigger_id      TEXT         NOT NULL UNIQUE,
    event_type      TEXT         NOT NULL,  -- e.g. 'file_arrived', 'partition_ready'
    source_system   TEXT         NOT NULL,  -- e.g. 'orders_service', 's3_bucket'
    payload         JSONB        NOT NULL,  -- event-specific metadata
    status          TEXT         NOT NULL DEFAULT 'pending',
                                            -- pending | processing | completed | failed
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    claimed_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error_message   TEXT,
    retry_count     INT          NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_trigger_queue_status
    ON trigger_queue(status, created_at);
"""


@dataclass
class TriggerEvent:
    id:            int
    trigger_id:    str
    event_type:    str
    source_system: str
    payload:       dict
    retry_count:   int


# ── Trigger queue helpers ──────────────────────────────────────────────────────

def claim_next_trigger(conn) -> Optional[TriggerEvent]:
    """
    Atomically claims the next pending trigger using SELECT FOR UPDATE SKIP LOCKED.
    This pattern allows multiple workers to claim triggers concurrently
    without one blocking another — each worker takes a different row.
    Returns None if no pending triggers exist.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            UPDATE trigger_queue
            SET    status     = 'processing',
                   claimed_at = NOW()
            WHERE  id = (
                SELECT id FROM trigger_queue
                WHERE  status = 'pending'
                   AND retry_count < 3
                ORDER  BY created_at ASC
                LIMIT  1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, trigger_id, event_type, source_system,
                      payload, retry_count
        """)
        row = cur.fetchone()
    conn.commit()

    if row is None:
        return None

    return TriggerEvent(
        id            = row["id"],
        trigger_id    = row["trigger_id"],
        event_type    = row["event_type"],
        source_system = row["source_system"],
        payload       = row["payload"],
        retry_count   = row["retry_count"],
    )


def mark_trigger_completed(conn, trigger_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trigger_queue
            SET status       = 'completed',
                completed_at = NOW()
            WHERE id = %s
        """, (trigger_id,))
    conn.commit()
    log.info("Trigger %d marked completed.", trigger_id)


def mark_trigger_failed(conn, trigger_id: int, error: str):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trigger_queue
            SET status        = 'failed',
                completed_at  = NOW(),
                error_message = %s,
                retry_count   = retry_count + 1
            WHERE id = %s
        """, (error[:2000], trigger_id))
    conn.commit()
    log.warning("Trigger %d marked failed: %s", trigger_id, error)


# ── Spark processing logic ─────────────────────────────────────────────────────

def process_partition_ready_trigger(
    spark: SparkSession,
    payload: dict,
) -> int:
    """
    Handles a 'partition_ready' trigger — reads the Bronze partition
    identified in the payload and writes it to Silver.
    Returns the number of rows written.
    """
    partition_date = payload["partition_date"]
    source_table   = payload.get("source_table", "orders")

    log.info("Processing trigger: source=%s date=%s",
             source_table, partition_date)

    # Read the Bronze partition identified by the trigger
    bronze_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("_ingestion_date") == partition_date)
    )

    row_count = bronze_df.count()
    if row_count == 0:
        raise ValueError(
            f"Bronze partition empty for date={partition_date}. "
            f"Trigger may have fired prematurely."
        )

    # Apply Silver transformations (abbreviated)
    silver_df = (
        bronze_df
        .withColumn("id",          F.col("id").cast(IntegerType()))
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
        .withColumn("amount_usd",
            (F.col("ord_amt").cast(DoubleType()) / 100))
        .withColumn("updated_at",
            F.to_timestamp(
                (F.col("upd_ts").cast("long") / 1000).cast(TimestampType())
            ))
        .withColumn("_processed_at",  F.current_timestamp())
        .withColumn("_trigger_date",  F.lit(partition_date))
    )

    # Idempotent write using MERGE
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (
            DeltaTable.forPath(spark, SILVER_PATH).alias("target")
            .merge(silver_df.alias("source"), "target.id = source.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

    log.info("Silver write complete: %d rows", row_count)
    return row_count


def process_file_arrived_trigger(
    spark: SparkSession,
    payload: dict,
) -> int:
    """
    Handles a 'file_arrived' trigger — reads a specific file path
    delivered by an external system (e.g. partner file drop on S3).
    """
    file_path  = payload["file_path"]
    file_format = payload.get("format", "csv")
    dataset    = payload.get("dataset", "unknown")

    log.info("Processing file trigger: path=%s format=%s", file_path, file_format)

    # Read the specific file identified by the trigger
    if file_format == "csv":
        df = spark.read.option("header", "true").csv(file_path)
    elif file_format == "parquet":
        df = spark.read.parquet(file_path)
    elif file_format == "json":
        df = spark.read.json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    row_count = df.count()

    # Add ingestion envelope and write to Bronze raw layer
    enriched_df = (
        df
        .withColumn("_ingestion_ts",   F.current_timestamp())
        .withColumn("_source_file",    F.lit(file_path))
        .withColumn("_source_system",  F.lit(dataset))
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_batch_id",       F.lit(str(uuid.uuid4())))
    )

    (
        enriched_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"/data/raw/{dataset}")
    )

    log.info("File ingested to Bronze: %d rows", row_count)
    return row_count


# ── Trigger router ─────────────────────────────────────────────────────────────

EVENT_HANDLERS = {
    "partition_ready": process_partition_ready_trigger,
    "file_arrived":    process_file_arrived_trigger,
}


def process_trigger(spark: SparkSession, trigger: TriggerEvent) -> int:
    """Routes a trigger event to the appropriate handler."""
    handler = EVENT_HANDLERS.get(trigger.event_type)
    if handler is None:
        raise ValueError(
            f"Unknown event type: {trigger.event_type}. "
            f"Registered handlers: {list(EVENT_HANDLERS.keys())}"
        )
    return handler(spark, trigger.payload)


# ── Main polling loop ──────────────────────────────────────────────────────────

def main():
    spark = SparkSession.builder \
        .appName("ExternalTriggerProcessor") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    conn = psycopg2.connect(**DB_CONN)

    # Ensure trigger queue table exists
    with conn.cursor() as cur:
        cur.execute(TRIGGER_QUEUE_DDL)
    conn.commit()

    log.info("External trigger processor started. Polling for triggers...")

    try:
        while True:
            trigger = claim_next_trigger(conn)

            if trigger is None:
                log.debug("No pending triggers. Sleeping 30s...")
                import time
                time.sleep(30)
                continue

            log.info(
                "Claimed trigger | id=%d | type=%s | source=%s | payload=%s",
                trigger.id, trigger.event_type,
                trigger.source_system, trigger.payload,
            )

            try:
                rows_written = process_trigger(spark, trigger)
                mark_trigger_completed(conn, trigger.id)
                log.info(
                    "Trigger %d completed | rows_written=%d",
                    trigger.id, rows_written,
                )
            except Exception as e:
                log.error("Trigger %d failed: %s", trigger.id, e,
                          exc_info=True)
                mark_trigger_failed(conn, trigger.id, str(e))
                # Continue polling — one failed trigger does not stop the processor

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        conn.close()
        spark.stop()


if __name__ == "__main__":
    main()
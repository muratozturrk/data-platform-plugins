# spark_s3_file_trigger.py
#
# S3 File Arrival Trigger: listens for S3 event notifications published
# to an SQS queue (or equivalent) and triggers Spark processing
# immediately when a new file arrives in the landing zone.
#
# S3 → S3 Event Notification → SQS queue → This processor → Spark job
#
# Requirements:
#   pip install pyspark delta-spark boto3 psycopg2-binary

import os
import json
import logging
import boto3
import psycopg2
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

SQS_QUEUE_URL   = "https://sqs.us-east-1.amazonaws.com/123456789/file-arrivals"
AWS_REGION      = "us-east-1"
LANDING_PREFIX  = "landing/orders/"     # only process files under this prefix
RAW_DELTA_PATH  = "/data/raw/orders"
DB_CONN         = dict(
    host="state-db", dbname="pipeline_state",
    user="pipeline", password=os.environ["DB_PASSWORD"]
)


# ── Deduplication store ────────────────────────────────────────────────────────
# S3 event notifications are at-least-once — the same file arrival event
# can be delivered multiple times. We deduplicate using an S3 key tracking table.

def ensure_processed_files_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_s3_files (
                s3_key       TEXT PRIMARY KEY,
                bucket       TEXT NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                row_count    BIGINT,
                batch_id     TEXT
            )
        """)
    conn.commit()


def is_already_processed(conn, s3_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM processed_s3_files WHERE s3_key = %s",
            (s3_key,)
        )
        return cur.fetchone() is not None


def mark_file_processed(conn, s3_key: str, bucket: str,
                         row_count: int, batch_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO processed_s3_files
                (s3_key, bucket, row_count, batch_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (s3_key) DO NOTHING
        """, (s3_key, bucket, row_count, batch_id))
    conn.commit()


# ── S3 event parsing ───────────────────────────────────────────────────────────

def parse_s3_event(sqs_message_body: str) -> list[dict]:
    """
    Parses an SQS message containing an S3 event notification.
    Returns a list of {bucket, key} dicts for each file that arrived.
    """
    body = json.loads(sqs_message_body)

    # Handle SNS-wrapped S3 events (common pattern)
    if "Message" in body:
        body = json.loads(body["Message"])

    if "Records" not in body:
        return []

    files = []
    for record in body["Records"]:
        if record.get("eventName", "").startswith("ObjectCreated"):
            bucket = record["s3"]["bucket"]["name"]
            key    = record["s3"]["object"]["key"]

            # Only process files under the expected prefix
            if key.startswith(LANDING_PREFIX) and not key.endswith("/"):
                files.append({"bucket": bucket, "key": key})

    return files


# ── Spark processing ───────────────────────────────────────────────────────────

def ingest_file_to_bronze(
    spark:     SparkSession,
    bucket:    str,
    key:       str,
) -> int:
    """
    Reads a single file from S3 and appends it to the Bronze Delta table.
    Detects file format from the key extension.
    """
    s3_path  = f"s3a://{bucket}/{key}"
    batch_id = str(uuid.uuid4())

    # Detect format from file extension
    if key.endswith(".csv"):
        df = spark.read.option("header", "true").csv(s3_path)
    elif key.endswith(".parquet"):
        df = spark.read.parquet(s3_path)
    elif key.endswith(".json") or key.endswith(".jsonl"):
        df = spark.read.json(s3_path)
    else:
        raise ValueError(f"Unsupported file extension: {key}")

    row_count = df.count()
    if row_count == 0:
        log.warning("Empty file: %s — skipping.", key)
        return 0

    # Add Bronze envelope metadata
    enriched_df = (
        df
        .withColumn("_ingestion_ts",   F.current_timestamp())
        .withColumn("_source_file",    F.lit(s3_path))
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_batch_id",       F.lit(batch_id))
    )

    (
        enriched_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(RAW_DELTA_PATH)
    )

    log.info("Ingested %s → Bronze: %d rows | batch_id=%s",
             key, row_count, batch_id)
    return row_count


# ── SQS polling loop ───────────────────────────────────────────────────────────

def run():
    sqs  = boto3.client("sqs", region_name=AWS_REGION)
    conn = psycopg2.connect(**DB_CONN)
    ensure_processed_files_table(conn)

    spark = SparkSession.builder \
        .appName("S3FileTriggerProcessor") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    log.info("S3 file trigger processor started. Listening on SQS...")

    try:
        while True:
            # Long poll for up to 20 seconds — reduces empty responses
            response = sqs.receive_message(
                QueueUrl            = SQS_QUEUE_URL,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds     = 20,    # long polling
            )

            messages = response.get("Messages", [])
            if not messages:
                continue

            for sqs_msg in messages:
                receipt_handle = sqs_msg["ReceiptHandle"]
                files = parse_s3_event(sqs_msg["Body"])

                for file_info in files:
                    bucket = file_info["bucket"]
                    key    = file_info["key"]

                    # Deduplication: skip if already processed
                    if is_already_processed(conn, key):
                        log.info("Already processed: %s — skipping.", key)
                        continue

                    try:
                        row_count = ingest_file_to_bronze(spark, bucket, key)
                        mark_file_processed(
                            conn, key, bucket, row_count,
                            str(uuid.uuid4())
                        )
                    except Exception as e:
                        log.error("Failed to process %s: %s", key, e,
                                  exc_info=True)
                        # Do NOT delete the SQS message — let it retry
                        # SQS will redeliver after visibility timeout
                        continue

                # Delete SQS message only after all files in it are processed
                sqs.delete_message(
                    QueueUrl      = SQS_QUEUE_URL,
                    ReceiptHandle = receipt_handle,
                )
                log.info("SQS message deleted: processed %d file(s)",
                         len(files))

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        conn.close()
        spark.stop()


if __name__ == "__main__":
    run()
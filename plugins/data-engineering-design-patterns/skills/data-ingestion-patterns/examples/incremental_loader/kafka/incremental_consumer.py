# incremental_consumer.py
# Reads from the topic that Kafka Connect populates.
# Usage: python incremental_consumer.py
#
# Requirements: pip install kafka-python psycopg2-binary

import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer, TopicPartition
from kafka import OffsetAndMetadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BROKERS   = ["localhost:9092"]
TOPIC           = "incremental_orders"   # matches topic.prefix + table name
CONSUMER_GROUP  = "incremental-loader-orders"

TARGET_DB = dict(
    host="localhost", dbname="mydb", user="user", password=os.environ["DB_PASSWORD"]
)

BATCH_SIZE = 500   # commit offsets every N messages


# ── Target DB helpers ─────────────────────────────────────────────────────────

def get_target_conn():
    return psycopg2.connect(**TARGET_DB)


def ensure_target_table(conn):
    """Create the target table if it does not exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders_dwh (
                id              INT PRIMARY KEY,
                customer_id     INT,
                amount          NUMERIC(10,2),
                status          VARCHAR(50),
                updated_at      TIMESTAMPTZ,
                kafka_ingested_at BIGINT,
                loaded_at       TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()


def upsert_batch(conn, records: list[dict]):
    """
    UPSERT a batch of order records into the target table.
    ON CONFLICT DO UPDATE handles rows that Kafka Connect re-published
    due to the look-back buffer — the latest version always wins.
    """
    if not records:
        return

    with conn.cursor() as cur:
        for rec in records:
            cur.execute("""
                INSERT INTO orders_dwh
                    (id, customer_id, amount, status, updated_at, kafka_ingested_at)
                VALUES
                    (%(id)s, %(customer_id)s, %(amount)s,
                     %(status)s, %(updated_at)s, %(kafka_ingested_at)s)
                ON CONFLICT (id) DO UPDATE SET
                    customer_id       = EXCLUDED.customer_id,
                    amount            = EXCLUDED.amount,
                    status            = EXCLUDED.status,
                    updated_at        = EXCLUDED.updated_at,
                    kafka_ingested_at = EXCLUDED.kafka_ingested_at,
                    loaded_at         = NOW()
            """, rec)
    conn.commit()
    log.info("Upserted %d records into orders_dwh", len(records))


# ── Consumer loop ─────────────────────────────────────────────────────────────

def run_consumer():
    target_conn = get_target_conn()
    ensure_target_table(target_conn)

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS,

        # Consumer group: Kafka tracks the committed offset per group.
        # If this consumer restarts, it resumes from the last committed offset —
        # this is the Kafka equivalent of a watermark.
        group_id=CONSUMER_GROUP,

        # Manual commit: we control exactly when the offset advances.
        # With auto-commit, a crash between receiving and processing a message
        # would silently skip it — manual commit prevents that.
        enable_auto_commit=False,

        # If this group has never committed an offset (first run),
        # start from the very beginning of the topic.
        auto_offset_reset="earliest",

        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,

        # Maximum records per poll call — controls batch size
        max_poll_records=BATCH_SIZE,

        # Heartbeat and session settings — prevent the broker from
        # thinking this consumer is dead during a slow DB upsert
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        max_poll_interval_ms=300_000,
    )

    consumer.subscribe([TOPIC])
    log.info("Subscribed to topic: %s (group: %s)", TOPIC, CONSUMER_GROUP)

    try:
        while True:
            # poll() returns a dict: {TopicPartition → [ConsumerRecord, ...]}
            poll_result = consumer.poll(timeout_ms=5_000)

            if not poll_result:
                log.debug("No new messages. Waiting...")
                continue

            for partition, messages in poll_result.items():
                records = [msg.value for msg in messages]

                log.info(
                    "Partition %d: received %d messages (offsets %d → %d)",
                    partition.partition,
                    len(messages),
                    messages[0].offset,
                    messages[-1].offset,
                )

                # ── Process the batch ──────────────────────────────────────
                upsert_batch(target_conn, records)

                # ── Commit offset AFTER successful upsert ──────────────────
                # Committing offset = advancing the watermark.
                # We only do this after the DB write succeeds.
                # If upsert_batch() raises an exception, we never reach here,
                # so the offset stays at the previous committed position and
                # the same messages are redelivered on the next poll — free retry.
                consumer.commit({
                    partition: OffsetAndMetadata(
                        messages[-1].offset + 1,  # next offset to read
                        None
                    )
                })
                log.info(
                    "Committed offset %d for partition %d",
                    messages[-1].offset + 1,
                    partition.partition,
                )

    except KeyboardInterrupt:
        log.info("Shutting down consumer gracefully...")
    except Exception as e:
        log.error("Consumer error: %s", e, exc_info=True)
        raise
    finally:
        consumer.close()
        target_conn.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    run_consumer()
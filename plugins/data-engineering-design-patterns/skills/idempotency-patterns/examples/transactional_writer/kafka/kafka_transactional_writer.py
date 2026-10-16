# kafka_transactional_writer.py
#
# Transactional Writer for Kafka pipelines.
# Uses Kafka's native transaction API:
#   initTransactions -> beginTransaction -> send -> commitTransaction
#                                               OR abortTransaction
#
# Guarantees:
#   - All messages in one transaction are visible to consumers atomically
#   - Consumers with isolation.level=read_committed see ONLY committed messages
#   - Aborted transaction messages are never delivered to read_committed consumers
#   - Exactly-once: transactional_id is stable per producer instance;
#     broker fences zombie producers with the same transactional_id
#
# Limitation: transactions span one producer session.
# Cross-topic atomicity (write topic A + topic B in one transaction) IS
# supported natively by Kafka transactions.
#
# Usage: python kafka_transactional_writer.py
# Requirements: pip install confluent-kafka
# (kafka-python does not support Kafka transactions -- use confluent-kafka)

import json
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterator

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException, TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# -- Config ------------------------------------------------------------------
KAFKA_BROKERS       = os.environ.get("KAFKA_BROKERS", "localhost:9092")
SOURCE_TOPIC        = os.environ.get("SOURCE_TOPIC", "silver.orders")
OUTPUT_TOPIC_ORDERS = os.environ.get("OUTPUT_TOPIC_ORDERS", "gold.orders")
OUTPUT_TOPIC_EVENTS = os.environ.get("OUTPUT_TOPIC_EVENTS", "gold.order_events")
CONSUMER_GROUP      = os.environ.get("CONSUMER_GROUP", "transactional-writer")
# transactional_id must be stable and unique per producer instance.
# Using a fixed string here -- in production, use a per-partition
# transactional_id to allow parallel transactional producers.
TRANSACTIONAL_ID    = os.environ.get("TRANSACTIONAL_ID", "transactional-writer-orders-p0")
# A transaction left open longer than this is aborted by the broker. Keep it
# above the worst-case batch duration (collect window + produce + commit); the
# same applies to Flink, where the timeout must exceed the
# checkpoint duration. Must be <= broker transaction.max.timeout.ms.
TRANSACTION_TIMEOUT_MS = int(os.environ.get("TRANSACTION_TIMEOUT_MS", "60000"))
# read_committed: consumer sees only committed transactional messages
ISOLATION_LEVEL     = "read_committed"


# -- Transactional producer --------------------------------------------------
def make_transactional_producer() -> Producer:
    """
    Creates a Kafka producer with transactional support.
    transactional.id must be stable per logical producer instance.
    acks=all and enable.idempotence=true are required by transactions.
    """
    return Producer({
        "bootstrap.servers":        KAFKA_BROKERS,
        "transactional.id":         TRANSACTIONAL_ID,
        "transaction.timeout.ms":   TRANSACTION_TIMEOUT_MS,
        "enable.idempotence":       True,
        "acks":                     "all",
        "retries":                  10,
        "max.in.flight.requests.per.connection": 1,
    })


# -- Read-committed consumer -------------------------------------------------
def make_read_committed_consumer() -> Consumer:
    """
    isolation.level=read_committed: only messages from committed
    transactions are returned to the application.
    Aborted transaction messages are filtered by the broker.
    """
    return Consumer({
        "bootstrap.servers":  KAFKA_BROKERS,
        "group.id":           CONSUMER_GROUP,
        "isolation.level":    ISOLATION_LEVEL,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })


# -- Transformation logic ----------------------------------------------------
def transform_order(record: dict) -> tuple[dict, dict]:
    """
    Transforms one order record into:
      1. Enriched order (for gold.orders)
      2. Event record (for gold.order_events)
    Both are written atomically in one Kafka transaction.
    """
    enriched_order = {
        **record,
        "_transformed_at": datetime.now(tz=timezone.utc).isoformat(),
        "_version":        1,
    }
    event_record = {
        "order_id":   record.get("order_id"),
        "event_type": record.get("event_type", "unknown"),
        "event_time": record.get("event_time"),
        "amount":     record.get("amount"),
        "emitted_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    return enriched_order, event_record


def rewind_to_batch_start(consumer: Consumer, messages: list) -> None:
    """Seek every partition of an aborted batch back to its first offset."""
    first: dict[int, int] = {}
    for msg in messages:
        first[msg.partition()] = min(first.get(msg.partition(), msg.offset()),
                                     msg.offset())
    for partition, offset in first.items():
        consumer.seek(TopicPartition(SOURCE_TOPIC, partition, offset))
    log.warning("Rewound %d partition(s) to the start of the aborted batch",
                len(first))


# -- Transactional write loop ------------------------------------------------
def run():
    producer = make_transactional_producer()
    consumer = make_read_committed_consumer()

    # initTransactions: registers this transactional_id with the broker.
    # Any zombie producer with the same transactional_id is fenced off.
    producer.init_transactions()
    log.info(
        "Transactional producer initialised | transactional_id=%s",
        TRANSACTIONAL_ID,
    )

    consumer.subscribe([SOURCE_TOPIC])
    stats    = defaultdict(int)
    last_log = time.time()

    log.info(
        "Transactional writer started | %s -> %s, %s",
        SOURCE_TOPIC, OUTPUT_TOPIC_ORDERS, OUTPUT_TOPIC_EVENTS,
    )

    try:
        while True:
            # Poll a batch of messages
            messages = []
            deadline = time.time() + 5.0   # collect for up to 5 seconds

            while time.time() < deadline:
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise KafkaException(msg.error())
                messages.append(msg)

            if not messages:
                continue

            # -- BEGIN TRANSACTION ------------------------------------------
            producer.begin_transaction()
            log.debug(
                "Transaction BEGIN | messages=%d", len(messages)
            )

            try:
                for msg in messages:
                    try:
                        record = json.loads(msg.value().decode("utf-8"))
                    except (json.JSONDecodeError, AttributeError):
                        stats["parse_errors"] += 1
                        continue

                    order_id = str(record.get("order_id") or "")
                    if not order_id:
                        stats["no_key"] += 1
                        continue

                    enriched_order, event_record = transform_order(record)

                    # Send to both output topics within the same transaction
                    producer.produce(
                        topic     = OUTPUT_TOPIC_ORDERS,
                        key       = order_id.encode("utf-8"),
                        value     = json.dumps(enriched_order).encode("utf-8"),
                    )
                    producer.produce(
                        topic     = OUTPUT_TOPIC_EVENTS,
                        key       = order_id.encode("utf-8"),
                        value     = json.dumps(event_record).encode("utf-8"),
                    )
                    stats["produced"] += 1

                # Send consumed offsets to transaction -- atomic offset commit
                # Offsets committed only if transaction commits.
                # On abort: offsets NOT committed; consumer re-reads messages.
                consumer_positions: dict[int, int] = {}
                for msg in messages:
                    consumer_positions[msg.partition()] = max(
                        consumer_positions.get(msg.partition(), 0),
                        msg.offset() + 1,
                    )
                offsets = [
                    TopicPartition(SOURCE_TOPIC, p, o)
                    for p, o in consumer_positions.items()
                ]
                producer.send_offsets_to_transaction(
                    offsets,
                    consumer.consumer_group_metadata(),
                )

                # -- COMMIT TRANSACTION -------------------------------------
                producer.commit_transaction()
                stats["transactions_committed"] += 1
                log.debug(
                    "Transaction COMMIT | produced=%d", stats["produced"]
                )

            except Exception as exc:
                # -- ABORT TRANSACTION --------------------------------------
                log.error(
                    "Transaction ABORT | reason=%s", str(exc)
                )
                try:
                    producer.abort_transaction()
                except Exception as abort_exc:
                    log.error(
                        "abort_transaction failed: %s. "
                        "Producer may need reinitialisation.",
                        str(abort_exc),
                    )
                stats["transactions_aborted"] += 1
                # The group offsets were not committed, but this consumer's
                # in-memory position already moved past the batch. Without a
                # seek the next poll would skip it and the data would be lost.
                rewind_to_batch_start(consumer, messages)
                if (isinstance(exc, KafkaException) and exc.args
                        and isinstance(exc.args[0], KafkaError)
                        and exc.args[0].fatal()):
                    # e.g. fenced by a newer instance with the same
                    # transactional.id: this producer can never commit again.
                    raise
                # Do not re-raise -- retry the same batch in the next loop

            if time.time() - last_log > 60:
                log.info(
                    "Stats | produced=%d committed_txns=%d "
                    "aborted_txns=%d parse_errors=%d no_key=%d",
                    stats["produced"],
                    stats["transactions_committed"],
                    stats["transactions_aborted"],
                    stats["parse_errors"],
                    stats["no_key"],
                )
                last_log = time.time()

    except KeyboardInterrupt:
        log.info("Shutting down.")
    finally:
        consumer.close()
        # No explicit producer.close() needed -- init_transactions handles cleanup


if __name__ == "__main__":
    run()
# kafka_pipeline_trigger.py
#
# Kafka External Trigger: upstream job publishes a pipeline completion
# event to a dedicated topic. Downstream job subscribes and starts
# processing only when the expected event arrives.
#
# Two components in one file:
#   TriggerPublisher  — called by upstream jobs on completion
#   TriggerConsumer   — runs downstream, reacts to completion events
#
# Usage: python kafka_pipeline_trigger.py
# Requirements: pip install kafka-python

import json
import logging
import time
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, Callable
from kafka import KafkaProducer, KafkaConsumer
from kafka import OffsetAndMetadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKERS     = ["localhost:9092"]
TRIGGER_TOPIC     = "pipeline.triggers"
DLQ_TOPIC         = "pipeline.triggers.dlq"    # dead-letter queue


@dataclass
class PipelineTriggerEvent:
    """
    Standardised trigger event schema.
    Every upstream job publishes this when it completes successfully.
    """
    pipeline_name:  str           # e.g. "passthrough_replicator_orders"
    dataset:        str           # e.g. "orders_bronze"
    partition_date: str           # e.g. "2024-01-10"
    batch_id:       str           # unique run ID
    row_count:      int           # rows written by upstream
    schema_hash:    str           # column hash for drift detection
    published_at:   str           # UTC ISO timestamp
    extra:          Optional[dict] = None


# ── Publisher ──────────────────────────────────────────────────────────────────

class TriggerPublisher:
    """
    Published by upstream Spark jobs on successful completion.
    Import this in any job that wants to trigger downstream pipelines.
    """

    def __init__(self):
        self._producer = KafkaProducer(
            bootstrap_servers = KAFKA_BROKERS,
            value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
            key_serializer    = lambda k: k.encode("utf-8"),
            acks              = "all",
            retries           = 5,
            max_in_flight_requests_per_connection = 1,
        )

    def publish(self, event: PipelineTriggerEvent):
        """
        Publishes a trigger event. Key = dataset:partition_date
        ensures log compaction retains only the latest trigger per
        dataset per partition.
        """
        key = f"{event.dataset}:{event.partition_date}"

        future = self._producer.send(
            topic = TRIGGER_TOPIC,
            key   = key,
            value = asdict(event),
        )
        self._producer.flush()
        meta = future.get(timeout=10)

        log.info(
            "Trigger published | pipeline=%s | dataset=%s | date=%s | "
            "rows=%d | offset=%d",
            event.pipeline_name, event.dataset,
            event.partition_date, event.row_count, meta.offset,
        )

    def close(self):
        self._producer.close()


# ── Consumer ───────────────────────────────────────────────────────────────────

class TriggerConsumer:
    """
    Subscribes to the trigger topic and calls handler functions
    when matching trigger events arrive.

    Supports:
      - Filtering by dataset name
      - At-least-once processing with manual offset commit
      - Dead-letter queue for events that fail after max_retries
    """

    def __init__(self, consumer_group: str, max_retries: int = 3):
        self._consumer_group = consumer_group
        self._max_retries    = max_retries
        self._handlers: dict[str, Callable] = {}

        self._consumer = KafkaConsumer(
            TRIGGER_TOPIC,
            bootstrap_servers  = KAFKA_BROKERS,
            group_id           = consumer_group,
            enable_auto_commit = False,
            auto_offset_reset  = "earliest",
            value_deserializer = lambda v: json.loads(v.decode("utf-8")),
            key_deserializer   = lambda k: k.decode("utf-8") if k else None,
            max_poll_records   = 10,
            session_timeout_ms = 30_000,
        )

        self._dlq_producer = KafkaProducer(
            bootstrap_servers = KAFKA_BROKERS,
            value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
            acks              = "all",
        )

    def register_handler(
        self,
        dataset:  str,
        handler:  Callable[[PipelineTriggerEvent], None],
    ):
        """
        Registers a handler function for a specific dataset.
        When a trigger event for that dataset arrives, handler is called.
        """
        self._handlers[dataset] = handler
        log.info("Handler registered for dataset: %s", dataset)

    def _send_to_dlq(self, event_dict: dict, error: str):
        """Sends an unprocessable event to the dead-letter queue."""
        dlq_event = {**event_dict, "_dlq_error": error,
                     "_dlq_ts": datetime.now(tz=timezone.utc).isoformat()}
        self._dlq_producer.send(
            DLQ_TOPIC,
            value=json.dumps(dlq_event).encode("utf-8")
        )
        self._dlq_producer.flush()
        log.warning("Event sent to DLQ: %s", error)

    def run(self):
        """
        Starts the consumer loop. Blocks until KeyboardInterrupt.
        """
        log.info(
            "Trigger consumer started | group=%s | handlers=%s",
            self._consumer_group, list(self._handlers.keys())
        )

        try:
            while True:
                poll_result = self._consumer.poll(timeout_ms=5_000)

                if not poll_result:
                    continue

                for partition, messages in poll_result.items():
                    for msg in messages:
                        event_dict = msg.value
                        dataset    = event_dict.get("dataset")

                        handler = self._handlers.get(dataset)
                        if handler is None:
                            # No handler for this dataset — skip silently
                            # and commit (we don't want to block on events
                            # this consumer doesn't care about)
                            self._consumer.commit({
                                partition: OffsetAndMetadata(
                                    msg.offset + 1, None)
                            })
                            continue

                        event = PipelineTriggerEvent(**event_dict)

                        # Retry loop with backoff
                        success = False
                        for attempt in range(1, self._max_retries + 1):
                            try:
                                handler(event)
                                success = True
                                break
                            except Exception as e:
                                log.warning(
                                    "Handler failed attempt %d/%d for %s: %s",
                                    attempt, self._max_retries,
                                    event.dataset, e
                                )
                                if attempt < self._max_retries:
                                    time.sleep(2 ** attempt)  # exponential backoff

                        if not success:
                            self._send_to_dlq(
                                event_dict,
                                f"Failed after {self._max_retries} attempts"
                            )

                        # Commit regardless of success/DLQ
                        # — we have handled the event (either processed or DLQ'd)
                        self._consumer.commit({
                            partition: OffsetAndMetadata(
                                msg.offset + 1, None)
                        })

        except KeyboardInterrupt:
            log.info("Shutting down consumer...")
        finally:
            self._consumer.close()
            self._dlq_producer.close()


# ── Example wiring ─────────────────────────────────────────────────────────────

def handle_orders_bronze_ready(event: PipelineTriggerEvent):
    """
    Called when the passthrough replicator signals orders_bronze is ready.
    Triggers the Silver transformation for that partition.
    """
    log.info(
        "Trigger received: orders_bronze ready | date=%s | rows=%d",
        event.partition_date, event.row_count,
    )
    # In production: submit Spark job or call Airflow REST API here
    # For example:
    #   submit_spark_job("transformation_replicator", event.partition_date)
    #   or
    #   trigger_airflow_dag("transformation_replicator_orders",
    #                        {"partition_date": event.partition_date})
    print(f"Starting Silver transformation for {event.partition_date}...")


if __name__ == "__main__":
    # ── Publisher example (run by upstream job) ────────────────────────
    publisher = TriggerPublisher()
    publisher.publish(PipelineTriggerEvent(
        pipeline_name  = "passthrough_replicator_orders",
        dataset        = "orders_bronze",
        partition_date = "2024-01-10",
        batch_id       = "abc-123",
        row_count      = 94821,
        schema_hash    = "a1b2c3d4",
        published_at   = datetime.now(tz=timezone.utc).isoformat(),
    ))
    publisher.close()

    # ── Consumer example (run by downstream job) ───────────────────────
    consumer = TriggerConsumer(consumer_group="silver-transformation-trigger")
    consumer.register_handler("orders_bronze", handle_orders_bronze_ready)
    consumer.run()
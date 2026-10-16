# kafka_readiness_marker.py
#
# Kafka Readiness Marker: publishes a readiness event to a dedicated
# pipeline.readiness topic when a dataset partition is complete.
# Downstream consumers subscribe to this topic and start processing
# only after receiving the readiness event for their expected dataset.
#
# Usage: import and call publish_readiness_event() from any pipeline job.
# Requirements: pip install kafka-python

import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from kafka import KafkaProducer, KafkaConsumer
from kafka import OffsetAndMetadata

log = logging.getLogger(__name__)

KAFKA_BROKERS     = ["localhost:9092"]
READINESS_TOPIC   = "pipeline.readiness"


@dataclass
class ReadinessEvent:
    dataset:        str
    partition_date: str
    batch_id:       str
    row_count:      int
    schema_hash:    str
    produced_at:    str
    extra:          Optional[dict] = None


# ── Publisher ──────────────────────────────────────────────────────────────────

def publish_readiness_event(
    dataset:        str,
    partition_date: str,
    batch_id:       str,
    row_count:      int,
    schema_hash:    str,
    extra:          Optional[dict] = None,
) -> ReadinessEvent:
    """
    Publishes a readiness event to the pipeline.readiness Kafka topic.
    Use dataset+partition_date as the message key so:
      - All readiness events for the same partition go to the same partition
      - Log compaction retains only the latest readiness event per key
        (previous failed attempts' markers are automatically cleaned up)
    """
    event = ReadinessEvent(
        dataset        = dataset,
        partition_date = partition_date,
        batch_id       = batch_id,
        row_count      = row_count,
        schema_hash    = schema_hash,
        produced_at    = datetime.now(tz=timezone.utc).isoformat(),
        extra          = extra or {},
    )

    # Key = dataset:partition_date
    # This key ensures log compaction keeps only the latest readiness
    # event per dataset per partition — stale events are cleaned up automatically
    message_key = f"{dataset}:{partition_date}"

    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BROKERS,
        value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer    = lambda k: k.encode("utf-8"),
        acks              = "all",    # all replicas confirm — no signal loss
        retries           = 5,
    )

    future = producer.send(
        topic = READINESS_TOPIC,
        key   = message_key,
        value = asdict(event),
    )
    producer.flush()
    record_metadata = future.get(timeout=10)

    log.info(
        "Readiness event published | dataset=%s | date=%s | "
        "rows=%d | offset=%d",
        dataset, partition_date, row_count, record_metadata.offset,
    )
    producer.close()
    return event


# ── Consumer (event-driven downstream trigger) ─────────────────────────────────

def wait_for_readiness(
    expected_dataset:   str,
    expected_date:      str,
    consumer_group:     str,
    timeout_seconds:    int = 3600,
) -> ReadinessEvent:
    """
    Blocks until the readiness event for the expected dataset+date arrives
    on the pipeline.readiness topic. Returns the event when found.
    Raises TimeoutError if the event does not arrive within timeout_seconds.

    Use this in event-driven downstream jobs that are triggered by Kafka
    rather than scheduled by Airflow.
    """
    consumer = KafkaConsumer(
        READINESS_TOPIC,
        bootstrap_servers   = KAFKA_BROKERS,
        group_id            = consumer_group,
        enable_auto_commit  = False,
        auto_offset_reset   = "earliest",
        value_deserializer  = lambda v: json.loads(v.decode("utf-8")),
        key_deserializer    = lambda k: k.decode("utf-8") if k else None,
    )

    expected_key  = f"{expected_dataset}:{expected_date}"
    elapsed       = 0
    poll_interval = 5   # seconds

    log.info(
        "Waiting for readiness event | dataset=%s | date=%s | "
        "timeout=%ds",
        expected_dataset, expected_date, timeout_seconds,
    )

    try:
        while elapsed < timeout_seconds:
            poll_result = consumer.poll(timeout_ms=poll_interval * 1000)

            for partition, messages in poll_result.items():
                for msg in messages:
                    if msg.key == expected_key:
                        event = ReadinessEvent(**msg.value)
                        log.info(
                            "Readiness received | dataset=%s | date=%s | rows=%d",
                            event.dataset, event.partition_date, event.row_count,
                        )
                        # Commit offset — signal we processed this readiness event
                        consumer.commit({
                            partition: OffsetAndMetadata(msg.offset + 1, None)
                        })
                        return event

            elapsed += poll_interval
            log.debug("Still waiting... elapsed=%ds", elapsed)

    finally:
        consumer.close()

    raise TimeoutError(
        f"Readiness event not received within {timeout_seconds}s "
        f"for dataset={expected_dataset} date={expected_date}"
    )
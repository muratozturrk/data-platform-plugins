"""Offline Observer - standalone Kafka observation consumer, confluent-kafka.

Runs in its own consumer group, completely separate from the processing app, so it
never steals messages or commits the producer's offsets. Every WINDOW_SECONDS it:
  * emits metrics for the records seen (count, NULL/empty counts per field,
    unknown fields not part of the expected schema),
  * computes the lag of the observed processing group (`OBSERVED_GROUP`):
    high watermark - committed offset, per partition.
Observations go to stdout and the `visits_observations` topic.

Run: pip install confluent-kafka; python kafka_offline_observer.py
Env: KAFKA_BOOTSTRAP (localhost:9094), OBSERVED_GROUP (visits-processor).
"""
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
OBSERVED_GROUP = os.environ.get("OBSERVED_GROUP", "visits-processor")
TOPIC, WINDOW_SECONDS = "visits", 60
EXPECTED_FIELDS = {"visit_id", "event_time", "user_id", "page", "context"}


def observe(records: list) -> dict:
    nulls, unknown = Counter(), Counter()
    for r in records:
        for field in EXPECTED_FIELDS:
            if r.get(field) in (None, ""):
                nulls[field] += 1
        unknown.update(set(r) - EXPECTED_FIELDS)  # new upstream fields we don't process yet
    return {"all_rows": len(records), "nulls": dict(nulls), "unknown_fields": dict(unknown)}


def processing_lag(consumer: Consumer) -> dict:
    """Lag of another group: read its committed offsets via a throwaway consumer."""
    partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    observed = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": OBSERVED_GROUP,
                         "enable.auto.commit": False})
    try:
        committed = observed.committed([TopicPartition(TOPIC, p) for p in partitions], timeout=10)
        lag = {}
        for tp in committed:
            high = consumer.get_watermark_offsets(tp, timeout=10)[1]
            lag[tp.partition] = high - tp.offset if tp.offset >= 0 else high  # no commit yet
        return lag
    finally:
        observed.close()  # never commits: auto commit off, no assignment


def main():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "visits-offline-observer",
                         "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC])
    records, window_start = [], time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                try:
                    records.append(json.loads(msg.value()))
                except json.JSONDecodeError:
                    records.append({"_invalid_json": True})
            if time.monotonic() - window_start < WINDOW_SECONDS:
                continue
            observation = {"@timestamp": datetime.now(timezone.utc).isoformat(), **observe(records),
                           "lag": processing_lag(consumer)}
            print(json.dumps(observation))
            producer.produce("visits_observations", json.dumps(observation).encode("utf-8"))
            producer.poll(0)
            records, window_start = [], time.monotonic()
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()

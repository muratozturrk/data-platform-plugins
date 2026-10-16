"""Metadata Enhancer - Kafka: skip data with the broker's timestamp index + headers.

Kafka has no column statistics, but each log segment keeps a time index
(timestamp -> offset). `offsets_for_times` uses that metadata to jump straight
to the first record at/after a timestamp, instead of scanning the partition
from the beginning - the streaming analogue of skipping files by min/max.
Producers can also attach lightweight metadata in headers (schema version,
record count of a batch, source) so consumers decide to skip a message without
deserializing its payload.

Run:
    pip install confluent-kafka
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_time_index_seek.py visits 2024-07-01T09:00:00
"""
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def main(topic: str, since_iso: str) -> None:
    since_ms = int(datetime.fromisoformat(since_iso).replace(tzinfo=timezone.utc).timestamp() * 1000)
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "time-index-reader",
                         "enable.auto.commit": False})
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    # For offsets_for_times the `offset` field carries the timestamp to look up.
    lookups = [TopicPartition(topic, p, since_ms) for p in partitions]
    starts = consumer.offsets_for_times(lookups, timeout=10)
    print("start offsets:", [(tp.partition, tp.offset) for tp in starts])  # -1 = nothing newer
    consumer.assign([tp for tp in starts if tp.offset >= 0])

    read = 0
    while read < 100:
        msg = consumer.poll(2.0)
        if msg is None:
            break
        if msg.error():
            continue
        headers = dict(msg.headers() or [])
        if headers.get("schema_version", b"1") != b"1":
            continue  # header metadata lets us skip without parsing the value
        read += 1
        print(msg.partition(), msg.offset(), msg.timestamp()[1], msg.value()[:60])
    consumer.close()


if __name__ == "__main__":
    main(*(sys.argv[1:3] if len(sys.argv) > 2 else ("visits", "2024-07-01T09:00:00")))

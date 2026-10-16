"""Bucket - Kafka key-hash partitioning as bucketing + co-partitioning check.

A Kafka topic partition is a bucket: the default partitioner assigns
murmur2(key) % num_partitions. Two topics keyed by the same id with the SAME
partition count are co-partitioned, so a consumer (Kafka Streams, Flink, a
plain consumer assigned partition N of both topics) can join them locally,
without repartitioning - the streaming twin of a shuffle-free bucketed join.

This script (1) verifies co-partitioning of two topics and (2) produces keyed
records to both so the same user always lands in the same partition number.
Like bucket counts, partition counts are effectively immutable: adding
partitions remaps keys and breaks co-partitioning for existing data.

Run:
    pip install confluent-kafka
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_copartitioning_check.py visits users
"""
import json
import os
import sys

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def partition_counts(admin: AdminClient, topics: list[str]) -> dict[str, int]:
    metadata = admin.list_topics(timeout=10)
    return {t: len(metadata.topics[t].partitions) for t in topics}


def main(left: str, right: str) -> None:
    counts = partition_counts(AdminClient({"bootstrap.servers": BOOTSTRAP}), [left, right])
    if counts[left] != counts[right]:
        raise SystemExit(f"NOT co-partitioned: {counts}. Recreate with equal partition counts.")
    print(f"co-partitioned: {counts}")

    landed: dict[str, dict[str, int]] = {}

    def on_delivery(err, msg):
        if err is None:
            landed.setdefault(msg.key().decode(), {})[msg.topic()] = msg.partition()

    # Same partitioner on both producers; 'murmur2_random' matches the Java client default.
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "partitioner": "murmur2_random"})
    for user_id in ("u1", "u2", "u3", "u42"):
        producer.produce(left, key=user_id, value=json.dumps({"user_id": user_id, "page": "home"}),
                         on_delivery=on_delivery)
        producer.produce(right, key=user_id, value=json.dumps({"user_id": user_id, "country": "FR"}),
                         on_delivery=on_delivery)
    producer.flush()
    for user_id, parts in landed.items():
        print(user_id, parts, "OK" if len(set(parts.values())) == 1 else "MISMATCH")


if __name__ == "__main__":
    main(*(sys.argv[1:3] if len(sys.argv) > 2 else ("visits", "users")))

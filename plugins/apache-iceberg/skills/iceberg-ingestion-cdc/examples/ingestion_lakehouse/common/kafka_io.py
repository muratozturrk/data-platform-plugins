"""Kafka helpers built on confluent-kafka (librdkafka): topic admin and an idempotent JSON producer."""
import json
from typing import Callable, Iterable

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common.lakehouse import kafka_bootstrap


def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_bootstrap()})


def create_topic(name: str, partitions: int = 4) -> str:
    admin = _admin()  # keep a reference: the futures fail if the client is garbage-collected
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    try:
        futures[name].result(timeout=30)
    except KafkaException as e:
        if e.args[0].name() != "TOPIC_ALREADY_EXISTS":
            raise
    return name


def delete_topic(name: str) -> None:
    admin = _admin()
    for f in admin.delete_topics([name]).values():
        try:
            f.result(timeout=30)
        except KafkaException:
            pass


def produce(topic: str, records: Iterable[dict | str | None],
            key: Callable[[dict], object] | None = None) -> int:
    """Send records as JSON. A str is sent as-is (to inject malformed payloads), None as a tombstone."""
    producer = Producer({
        "bootstrap.servers": kafka_bootstrap(),
        "enable.idempotence": True,     # no broker-side duplicates on producer retries
        "linger.ms": 5,
    })
    n = 0
    for r in records:
        if isinstance(r, tuple):        # (key_dict, value) pairs, e.g. Debezium key + envelope/tombstone
            k, r = r
        else:
            k = key(r) if key and isinstance(r, dict) else None
        value = r if isinstance(r, str) or r is None else json.dumps(r)
        producer.produce(topic, key=None if k is None else json.dumps(k), value=value)
        n += 1
        if n % 10_000 == 0:
            producer.flush()
    producer.flush(30)
    return n

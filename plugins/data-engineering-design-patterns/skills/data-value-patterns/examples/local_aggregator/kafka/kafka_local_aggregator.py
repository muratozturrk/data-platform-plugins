"""Local Aggregator -- Kafka producer/consumer pair with a fixed key->partition contract.

Python analogue of Kafka Streams `groupByKey()`: the
consumer aggregates records of each assigned partition in memory and never
re-keys or repartitions them, because the producer guarantees that all records
of a visit_id land in the same partition.

  produce    reads JSON visits from stdin, sends each to
             partition = crc32(visit_id) % EXPECTED_PARTITIONS (explicit, not the
             client's default partitioner, so every producer agrees)
  aggregate  per assigned partition: {visit_id -> events, pages, first/last time};
             every FLUSH_SECONDS (and on rebalance revoke) emits the window's
             aggregates to the SAME partition number of the output topic,
             flushes, then commits that partition's offsets

Both sides refuse to start if the topic partition count differs from
EXPECTED_PARTITIONS: adding partitions re-maps keys and silently splits
aggregates. Changing it is a migration, not a config tweak.

Run:
  export KAFKA_BOOTSTRAP=localhost:9092 EXPECTED_PARTITIONS=12
  cat visits.jsonl | python kafka_local_aggregator.py produce
  python kafka_local_aggregator.py aggregate
  python kafka_local_aggregator.py --self-test
Prereqs: pip install confluent-kafka
"""
import json
import logging
import os
import sys
import time
import zlib
from collections import defaultdict
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("local-aggregator-kafka")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "visits")
TARGET_TOPIC = os.environ.get("TARGET_TOPIC", "visits-aggregated")
EXPECTED_PARTITIONS = int(os.environ.get("EXPECTED_PARTITIONS", "12"))
FLUSH_SECONDS = float(os.environ.get("FLUSH_SECONDS", "60"))


def partition_for(key: str, partitions: int) -> int:
    """Stable across processes, languages and client versions (unlike Python's hash())."""
    return zlib.crc32(key.encode("utf-8")) % partitions


class PartitionAggregates:
    """In-memory state of ONE input partition. Pure, no Kafka dependency."""

    def __init__(self) -> None:
        self.by_key: Dict[str, Dict] = {}
        self.last_offset = -1

    def add(self, visit: Dict, offset: int) -> None:
        agg = self.by_key.get(visit["visit_id"])
        if agg is None:
            agg = self.by_key[visit["visit_id"]] = {
                "visit_id": visit["visit_id"], "events": 0, "pages": [],
                "first_event_time": visit["event_time"], "last_event_time": visit["event_time"]}
        agg["events"] += 1
        agg["pages"].append(visit.get("page"))
        agg["first_event_time"] = min(agg["first_event_time"], visit["event_time"])
        agg["last_event_time"] = max(agg["last_event_time"], visit["event_time"])
        self.last_offset = max(self.last_offset, offset)

    def drain(self) -> List[Dict]:
        out, self.by_key = list(self.by_key.values()), {}
        return out


def check_partition_count(client, topic: str) -> None:
    metadata = client.list_topics(topic, timeout=10)
    found = len(metadata.topics[topic].partitions)
    if found != EXPECTED_PARTITIONS:
        raise SystemExit(f"{topic} has {found} partitions, expected {EXPECTED_PARTITIONS}: "
                         "key colocation is broken, use the Distributed Aggregator or reorganize")


def produce() -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    check_partition_count(producer, SOURCE_TOPIC)
    failed: List[str] = []
    sent = 0
    for line in sys.stdin:
        if not line.strip():
            continue
        visit = json.loads(line)
        producer.produce(SOURCE_TOPIC, key=visit["visit_id"], value=line.strip(),
                         partition=partition_for(visit["visit_id"], EXPECTED_PARTITIONS),
                         on_delivery=lambda err, msg: err and failed.append(str(err)))
        producer.poll(0)
        sent += 1
    if producer.flush(60) or failed:
        raise SystemExit(f"delivery failed: {failed[:5]}")
    log.info("produced %d visits", sent)


class Aggregator:
    def __init__(self) -> None:
        from confluent_kafka import Consumer, Producer

        self.consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "local-aggregator",
                                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        self.producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
        check_partition_count(self.producer, SOURCE_TOPIC)
        check_partition_count(self.producer, TARGET_TOPIC)   # output keeps the same locality
        self.state: Dict[int, PartitionAggregates] = defaultdict(PartitionAggregates)
        self.errors: List[str] = []

    def flush_partition(self, partition: int) -> None:
        from confluent_kafka import TopicPartition

        part = self.state.get(partition)
        if part is None or part.last_offset < 0:
            return
        aggregates = part.drain()
        for agg in aggregates:
            self.producer.produce(TARGET_TOPIC, key=agg["visit_id"], value=json.dumps(agg),
                                  partition=partition,
                                  on_delivery=lambda err, msg: err and self.errors.append(str(err)))
        if self.producer.flush(60) or self.errors:
            raise RuntimeError(f"partition {partition}: aggregates not delivered {self.errors[:3]}")
        # commit only after the window's aggregates are acked
        self.consumer.commit(offsets=[TopicPartition(SOURCE_TOPIC, partition, part.last_offset + 1)],
                             asynchronous=False)
        log.info("partition %d: %d aggregates, committed offset %d", partition, len(aggregates),
                 part.last_offset + 1)
        del self.state[partition]

    def on_revoke(self, _consumer, partitions) -> None:
        for tp in partitions:            # emit what we hold before another member takes over
            self.flush_partition(tp.partition)

    def run(self) -> None:
        self.consumer.subscribe([SOURCE_TOPIC], on_revoke=self.on_revoke)
        next_flush = time.monotonic() + FLUSH_SECONDS
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is not None and not msg.error():
                    self.state[msg.partition()].add(json.loads(msg.value()), msg.offset())
                if time.monotonic() >= next_flush:
                    for partition in list(self.state):
                        self.flush_partition(partition)
                    next_flush = time.monotonic() + FLUSH_SECONDS
        finally:
            self.consumer.close()


def _self_test() -> None:
    assert partition_for("visit-1", 12) == partition_for("visit-1", 12)
    assert partition_for("visit-1", 12) == zlib.crc32(b"visit-1") % 12
    part = PartitionAggregates()
    part.add({"visit_id": "a", "event_time": "10:01", "page": "cart"}, 5)
    part.add({"visit_id": "a", "event_time": "10:00", "page": "home"}, 6)
    part.add({"visit_id": "b", "event_time": "10:02", "page": "home"}, 7)
    out = {a["visit_id"]: a for a in part.drain()}
    assert out["a"]["events"] == 2 and out["a"]["first_event_time"] == "10:00"
    assert out["a"]["last_event_time"] == "10:01" and out["b"]["pages"] == ["home"]
    assert part.last_offset == 7 and part.drain() == []
    print("self-test OK")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "--self-test":
        _self_test()
    elif mode == "produce":
        produce()
    elif mode == "aggregate":
        Aggregator().run()
    else:
        raise SystemExit("usage: kafka_local_aggregator.py produce|aggregate|--self-test")

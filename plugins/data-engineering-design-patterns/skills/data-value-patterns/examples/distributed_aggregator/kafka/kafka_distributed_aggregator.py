"""Distributed Aggregator -- MapReduce on plain Kafka consumers (shuffle = repartition topic).

The visits topic is keyed by visit_id, so events of one aggregation key
(event_date|country|device_type) sit in many partitions and consumers. Like a
framework's shuffle, the job exchanges records over the network so one reducer
owns each key (what Kafka Streams does with groupBy() + a repartition topic):

  visits --(mapper: partial count per key per poll = map-side combine)-->
  visits-agg-repartition (key = aggregation key -> hash partitioner = hashpartitioning)
  --(reducer: sum partials, own in-memory totals)--> visits-agg (compacted, running totals)

Exactly-once without dedup tables: both roles are consume-transform-produce loops in
Kafka transactions (produced records + consumer offsets commit atomically), and the
downstream reads with isolation.level=read_committed. Reducer state is restored on
partition assignment from the compacted output topic, which is co-partitioned with
the repartition topic (output partition = input partition).

Hot key? SALT_BUCKETS=N salts the mapper key ("key#0".."key#N-1") so one key spreads
over N reducers. The output then holds N partial totals per key;
the second stage (a consumer summing them after stripping "#i") is left out here.

Run:   python kafka_distributed_aggregator.py --role mapper     (N instances)
       python kafka_distributed_aggregator.py --role reducer    (M instances)
Test:  python kafka_distributed_aggregator.py --self-test       (no broker needed)
Env:   KAFKA_BOOTSTRAP, VISITS_TOPIC, REPARTITION_TOPIC, OUTPUT_TOPIC, SALT_BUCKETS
Prereqs: pip install confluent-kafka; the repartition and output topics have the same
partition count; output topic cleanup.policy=compact.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import zlib
from collections import Counter
from dataclasses import dataclass
from typing import Iterable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kafka_distributed_aggregator")


@dataclass(frozen=True)
class Config:
    bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    visits_topic: str = os.environ.get("VISITS_TOPIC", "visits")
    repartition_topic: str = os.environ.get("REPARTITION_TOPIC", "visits-agg-repartition")
    output_topic: str = os.environ.get("OUTPUT_TOPIC", "visits-agg")
    salt_buckets: int = int(os.environ.get("SALT_BUCKETS", "0"))
    batch_size: int = int(os.environ.get("BATCH_SIZE", "1000"))
    instance_id: str = os.environ.get("INSTANCE_ID", socket.gethostname())


# -- Pure map / reduce logic -----------------------------------------------------------
def aggregation_key(event: dict) -> str:
    ctx = event.get("context") or {}
    country = (ctx.get("user") or {}).get("country") or "unknown"
    device = (ctx.get("technical") or {}).get("dev_type") or "unknown"
    return f"{event['event_time'][:10]}|{country}|{device}"


def salted(key: str, visit_id: str, buckets: int) -> str:
    """Deterministic salt (crc32 of visit id), so a replay lands in the same bucket."""
    return f"{key}#{zlib.crc32(visit_id.encode()) % buckets}" if buckets > 0 else key


def map_partials(events: Iterable[dict], salt_buckets: int = 0) -> Counter:
    """Map + combine: one partial count per key instead of one record per event."""
    partials: Counter = Counter()
    for event in events:
        partials[salted(aggregation_key(event), str(event.get("visit_id", "")), salt_buckets)] += 1
    return partials


def reduce_partials(totals: dict, partials: Iterable[tuple[str, int]]) -> set[str]:
    """Sum partials into the reducer's totals; returns the keys that changed."""
    changed = set()
    for key, count in partials:
        totals[key] = totals.get(key, 0) + count
        changed.add(key)
    return changed


# -- Kafka plumbing ------------------------------------------------------------------------
def _consumer(cfg: Config, group: str):
    from confluent_kafka import Consumer
    return Consumer({"bootstrap.servers": cfg.bootstrap, "group.id": group,
                     "enable.auto.commit": False, "auto.offset.reset": "earliest",
                     "isolation.level": "read_committed"})


def _producer(cfg: Config, role: str):
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": cfg.bootstrap, "enable.idempotence": True,
                         "transactional.id": f"distributed-aggregator-{role}-{cfg.instance_id}"})
    producer.init_transactions()
    return producer


def _commit_transaction(producer, consumer) -> bool:
    """Outputs and input offsets in one atomic step; abort -> the batch is re-read."""
    from confluent_kafka import KafkaException
    try:
        producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                             consumer.consumer_group_metadata())
        producer.commit_transaction()
        return True
    except KafkaException as exc:
        if exc.args[0].txn_requires_abort():
            producer.abort_transaction()
            log.warning("transaction aborted, rewinding to committed offsets: %s", exc)
            for tp in consumer.committed(consumer.assignment()):
                consumer.seek(tp)
            return False
        raise


def run_mapper(cfg: Config) -> None:
    consumer = _consumer(cfg, "distributed-aggregator-mapper")
    producer = _producer(cfg, "mapper")
    consumer.subscribe([cfg.visits_topic])
    try:
        while True:
            messages = consumer.consume(num_messages=cfg.batch_size, timeout=1.0)
            events = []
            for msg in messages:
                if msg.error():
                    log.warning("consume error: %s", msg.error())
                    continue
                try:
                    events.append(json.loads(msg.value()))
                except (ValueError, TypeError):
                    log.warning("skip non-JSON record p=%d o=%d", msg.partition(), msg.offset())
            if not messages:
                continue
            producer.begin_transaction()
            for key, count in map_partials((e for e in events if e.get("event_time")), cfg.salt_buckets).items():
                producer.produce(cfg.repartition_topic, key=key, value=json.dumps({"count": count}))
            _commit_transaction(producer, consumer)
    finally:
        consumer.close()


class Reducer:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.totals: dict[int, dict[str, int]] = {}      # repartition partition -> key -> total
        self.consumer = _consumer(cfg, "distributed-aggregator-reducer")
        self.producer = _producer(cfg, "reducer")

    def restore(self, partition: int) -> dict[str, int]:
        """Rebuild totals from the compacted output partition (committed data only)."""
        from confluent_kafka import Consumer, TopicPartition
        reader = Consumer({"bootstrap.servers": self.cfg.bootstrap, "group.id": "unused-restore",
                           "enable.auto.commit": False, "isolation.level": "read_committed"})
        low, high = reader.get_watermark_offsets(TopicPartition(self.cfg.output_topic, partition))
        totals: dict[str, int] = {}
        if high > low:
            reader.assign([TopicPartition(self.cfg.output_topic, partition, low)])
            while True:
                messages = reader.consume(num_messages=1000, timeout=2.0)
                for msg in messages:
                    if not msg.error():
                        totals[msg.key().decode()] = json.loads(msg.value())["total"]
                position = reader.position([TopicPartition(self.cfg.output_topic, partition)])[0].offset
                # empty poll also ends: the last offsets can be transaction markers, never delivered
                if position >= high or not messages:
                    break
        reader.close()
        return totals

    def on_assign(self, consumer, partitions) -> None:
        for tp in partitions:
            self.totals[tp.partition] = self.restore(tp.partition)
        log.info("assigned %s", {p: len(t) for p, t in self.totals.items()})

    def on_revoke(self, consumer, partitions) -> None:
        for tp in partitions:
            self.totals.pop(tp.partition, None)

    def run(self) -> None:
        self.consumer.subscribe([self.cfg.repartition_topic], on_assign=self.on_assign, on_revoke=self.on_revoke)
        try:
            while True:
                messages = [m for m in self.consumer.consume(num_messages=self.cfg.batch_size, timeout=1.0)
                            if not m.error()]
                if not messages:
                    continue
                self.producer.begin_transaction()
                by_partition: dict[int, list] = {}
                for msg in messages:
                    by_partition.setdefault(msg.partition(), []).append(
                        (msg.key().decode(), int(json.loads(msg.value())["count"])))
                for partition, partials in by_partition.items():
                    totals = self.totals.setdefault(partition, {})
                    for key in reduce_partials(totals, partials):
                        self.producer.produce(self.cfg.output_topic, partition=partition, key=key,
                                              value=json.dumps({"key": key, "total": totals[key]}))
                if not _commit_transaction(self.producer, self.consumer):
                    # in-memory totals are ahead of the aborted output -> rebuild from Kafka
                    for partition in list(self.totals):
                        self.totals[partition] = self.restore(partition)
        finally:
            self.consumer.close()


def _self_check() -> None:
    events = [{"visit_id": "v1", "event_time": "2024-01-15T10:00:00Z",
               "context": {"user": {"country": "PL"}, "technical": {"dev_type": "mobile"}}},
              {"visit_id": "v1", "event_time": "2024-01-15T10:01:00Z",
               "context": {"user": {"country": "PL"}, "technical": {"dev_type": "mobile"}}},
              {"visit_id": "v2", "event_time": "2024-01-15T11:00:00Z", "context": {}}]
    partials = map_partials(events)
    assert partials == Counter({"2024-01-15|PL|mobile": 2, "2024-01-15|unknown|unknown": 1})
    totals: dict = {}
    reduce_partials(totals, partials.items())
    changed = reduce_partials(totals, map_partials(events[:1]).items())
    assert totals["2024-01-15|PL|mobile"] == 3 and changed == {"2024-01-15|PL|mobile"}
    s = map_partials(events, salt_buckets=4)
    assert sum(s.values()) == 3 and all("#" in k for k in s)
    assert salted("k", "v1", 4) == salted("k", "v1", 4)          # replay-stable salt
    print("self-check OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["mapper", "reducer"])
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_check()
    elif args.role == "mapper":
        run_mapper(Config())
    elif args.role == "reducer":
        Reducer(Config()).run()
    else:
        parser.error("--role or --self-test required")

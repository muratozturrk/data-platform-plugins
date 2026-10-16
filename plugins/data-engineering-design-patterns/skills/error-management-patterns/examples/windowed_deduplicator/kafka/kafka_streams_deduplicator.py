"""Windowed Deduplicator - plain Kafka consumer with an event-time window.

Same mechanics as Spark's watermark-based dropDuplicates, hand-written:
  * deduplication attributes: (visit_id, event_time) by default;
  * the window: keys are remembered while ``event_time >= watermark`` where
    watermark = max event time seen - window. Older keys are evicted (heap
    ordered by event time, O(log n) per record); older records are late and
    ignored, exactly like a watermark would do;
  * state store "local with fault tolerance": the in-memory window is
    snapshotted to a JSON file (atomic rename) before each offset commit, and
    restored on start. Order per poll: produce -> flush -> snapshot -> commit.
    A crash between flush and snapshot re-emits that poll's records: dedup gives
    exactly-once *processing*, not exactly-once *delivery*.

Single-instance sample: with several consumers, partition the state by Kafka
partition (one snapshot per partition) or use a remote store (Redis SET NX EX).

Run:  python kafka_streams_deduplicator.py [--window-seconds 600]
      python kafka_streams_deduplicator.py --self-test
Requires: pip install kafka-python. Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import heapq
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("kafka_deduplicator")

ACCEPTED, DUPLICATE, LATE = "accepted", "duplicate", "late"


class WindowedDeduplicationState:
    """Event-time window of seen keys with watermark-based expiration."""

    def __init__(self, window_seconds: float):
        self.window = window_seconds
        self.max_event_time = float("-inf")
        self.seen: Dict[str, float] = {}
        self.expirations: List[Tuple[float, str]] = []

    @property
    def watermark(self) -> float:
        return self.max_event_time - self.window

    def check(self, key: str, event_time: float) -> str:
        if event_time < self.watermark:
            return LATE
        if key in self.seen:
            return DUPLICATE
        self.seen[key] = event_time
        heapq.heappush(self.expirations, (event_time, key))
        if event_time > self.max_event_time:
            self.max_event_time = event_time
            self._evict()
        return ACCEPTED

    def _evict(self) -> None:
        while self.expirations and self.expirations[0][0] < self.watermark:
            _, key = heapq.heappop(self.expirations)
            self.seen.pop(key, None)

    def snapshot(self, path: Path) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"max_event_time": self.max_event_time, "seen": self.seen}))
        os.replace(tmp, path)

    @classmethod
    def restore(cls, path: Path, window_seconds: float) -> "WindowedDeduplicationState":
        state = cls(window_seconds)
        if path.exists():
            doc = json.loads(path.read_text())
            state.max_event_time = doc["max_event_time"]
            for key, event_time in doc["seen"].items():
                state.seen[key] = event_time
                heapq.heappush(state.expirations, (event_time, key))
            state._evict()
        return state


def dedup_key(visit: dict, attributes: List[str]) -> str:
    return "|".join(str(visit.get(a)) for a in attributes)


def run(args: argparse.Namespace) -> None:
    from kafka import KafkaConsumer, KafkaProducer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    state_file = Path(args.state_file)
    state = WindowedDeduplicationState.restore(state_file, args.window_seconds)
    consumer = KafkaConsumer(args.input_topic, bootstrap_servers=bootstrap, group_id=args.group,
                             enable_auto_commit=False, auto_offset_reset="earliest")
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=10)
    attributes = args.attributes.split(",")
    counters = {ACCEPTED: 0, DUPLICATE: 0, LATE: 0}
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            for records in polled.values():
                for record in records:
                    visit = json.loads(record.value)
                    event_time = dt.datetime.fromisoformat(visit["event_time"]).timestamp()
                    outcome = state.check(dedup_key(visit, attributes), event_time)
                    counters[outcome] += 1
                    if outcome == ACCEPTED:
                        producer.send(args.output_topic, key=record.key, value=record.value)
            if polled:
                producer.flush()
                state.snapshot(state_file)
                consumer.commit()
                log.info("counters=%s state_size=%s watermark=%s", counters, len(state.seen),
                         dt.datetime.fromtimestamp(state.watermark, dt.timezone.utc)
                         if state.seen else None)
    finally:
        producer.close()
        consumer.close()


def _self_check() -> None:
    s = WindowedDeduplicationState(window_seconds=600)
    assert s.check("a", 1000) == ACCEPTED
    assert s.check("a", 1000) == DUPLICATE
    assert s.check("b", 1500) == ACCEPTED
    assert s.check("c", 1700) == ACCEPTED   # watermark = 1100 -> "a" (1000) evicted
    assert "a" not in s.seen and s.check("a", 1000) == LATE
    assert s.check("b", 1500) == DUPLICATE
    tmp = Path(os.environ.get("TMP", "/tmp")) / "dedup_state_selftest.json"
    s.snapshot(tmp)
    restored = WindowedDeduplicationState.restore(tmp, 600)
    assert restored.check("b", 1500) == DUPLICATE and restored.watermark == 1100
    tmp.unlink()
    assert dedup_key({"visit_id": 1, "event_time": "t"}, ["visit_id", "event_time"]) == "1|t"
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--input-topic", default="visits")
    p.add_argument("--output-topic", default="visits_deduplicated")
    p.add_argument("--group", default="visits-deduplicator")
    p.add_argument("--attributes", default="visit_id,event_time")
    p.add_argument("--window-seconds", type=float, default=600)
    p.add_argument("--state-file", default="/tmp/dp/dedup/kafka-state.json")
    run(p.parse_args(argv))


if __name__ == "__main__":
    main()

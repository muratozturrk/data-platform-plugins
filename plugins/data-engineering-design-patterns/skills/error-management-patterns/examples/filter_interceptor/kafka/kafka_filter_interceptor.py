"""Filter Interceptor - streaming flavour on a Kafka consumer.

Streaming makes the pattern stateful: counters must live somewhere, and since
data never stops they need time boundaries - otherwise you cannot tell which
filter is active *now*. This consumer:
  * evaluates the same wrapped filters as the Spark example (first failing
    filter is counted, row dropped);
  * keeps per-filter counters in tumbling processing-time windows (default
    1 minute), the advice to "rely on time-based processing windows";
  * emits each closed window to a stats topic (and the log), e.g.
    {"window_start": ..., "input": 1200, "kept": 1010, "per_filter": {...}};
  * forwards kept records to the output topic, flush-then-commit per poll.

Counters are in memory: a crash loses the open window's partial counts (stats
are diagnostics, not data). Persist them with the offsets if that matters.

Run:  python kafka_filter_interceptor.py --window-seconds 60
      python kafka_filter_interceptor.py --self-test
Requires: pip install kafka-python. Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import logging
import os
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger("kafka_filter_interceptor")

FILTERS: List[Tuple[str, Callable[[Dict[str, Any]], bool]]] = [
    ("null_type", lambda d: d.get("type") is not None),
    ("short_type", lambda d: len(d["type"]) > 2),
    ("null_full_name", lambda d: d.get("full_name") is not None),
    ("null_version", lambda d: d.get("version") is not None),
]


class WindowedFilterStats:
    """Tumbling processing-time windows of per-filter counters."""

    def __init__(self, window_seconds: int, filters=FILTERS):
        self.window_seconds, self.filters = window_seconds, filters
        self.window_start: Optional[int] = None
        self._reset()

    def _reset(self) -> None:
        self.input = self.kept = 0
        self.per_filter = collections.Counter({name: 0 for name, _ in self.filters})

    def evaluate(self, record: Dict[str, Any], now: float) -> Tuple[bool, Optional[dict]]:
        """Returns (keep?, closed window stats or None)."""
        closed = self.roll(now)
        self.input += 1
        for name, predicate in self.filters:
            if not predicate(record):
                self.per_filter[name] += 1
                return False, closed
        self.kept += 1
        return True, closed

    def roll(self, now: float) -> Optional[dict]:
        start = int(now // self.window_seconds * self.window_seconds)
        if self.window_start is None:
            self.window_start = start
            return None
        if start == self.window_start:
            return None
        closed = {"window_start": dt.datetime.fromtimestamp(self.window_start, dt.timezone.utc).isoformat(),
                  "window_seconds": self.window_seconds, "input": self.input, "kept": self.kept,
                  "per_filter": dict(self.per_filter)}
        self.window_start = start
        self._reset()
        return closed


def run(args: argparse.Namespace) -> None:
    from kafka import KafkaConsumer, KafkaProducer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = KafkaConsumer(args.input_topic, bootstrap_servers=bootstrap, group_id=args.group,
                             enable_auto_commit=False, auto_offset_reset="earliest")
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=10)
    stats = WindowedFilterStats(args.window_seconds)

    def publish(closed: Optional[dict]) -> None:
        if closed:
            log.info("filter stats %s", closed)
            producer.send(args.stats_topic, value=json.dumps(closed).encode("utf-8"))

    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            publish(stats.roll(time.time()))   # close windows even when idle
            for records in polled.values():
                for record in records:
                    keep, closed = stats.evaluate(json.loads(record.value), time.time())
                    publish(closed)
                    if keep:
                        producer.send(args.output_topic, key=record.key, value=record.value)
            if polled:
                producer.flush()
                consumer.commit()
    finally:
        producer.close()
        consumer.close()


def _self_check() -> None:
    s = WindowedFilterStats(window_seconds=60)
    assert s.evaluate({"type": None}, 0) == (False, None)
    assert s.evaluate({"type": "ab"}, 10)[0] is False
    assert s.evaluate({"type": "abc", "full_name": "n", "version": "1"}, 59)[0] is True
    keep, closed = s.evaluate({"type": "abc", "full_name": None}, 61)   # new window
    assert keep is False and closed["input"] == 3 and closed["kept"] == 1
    assert closed["per_filter"] == {"null_type": 1, "short_type": 1, "null_full_name": 0, "null_version": 0}
    assert s.roll(130)["per_filter"]["null_full_name"] == 1
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--input-topic", default="devices")
    p.add_argument("--output-topic", default="devices_valid")
    p.add_argument("--stats-topic", default="devices_filter_stats")
    p.add_argument("--group", default="devices-filter-interceptor")
    p.add_argument("--window-seconds", type=int, default=60)
    run(p.parse_args(argv))


if __name__ == "__main__":
    main()

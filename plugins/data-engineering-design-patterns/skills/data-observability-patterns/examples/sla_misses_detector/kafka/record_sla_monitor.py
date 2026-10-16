"""SLA Misses Detector - per-record SLA for a non-windowed streaming job (confluent-kafka).

A Flink-style per-record SLA monitor rewritten without Flink:

* The processing job stamps each output record with a `start_processing_time_ms`
  header (see `stamp_headers`) when it reads the input record.
* This monitor consumes the OUTPUT topic and computes, per record:
    processing-time delay = append time (record timestamp) - start_processing_time_ms
    event-time delay      = append time - event_time (payload), reported separately
* Delays are aggregated into 1-minute tumbling windows (append time) as P95 and MAX
  and compared with the SLA.

Run:  python record_sla_monitor.py --topic visits-reduced --sla-ms 5000
Test: python record_sla_monitor.py --self-test
Env:  KAFKA_BOOTSTRAP (default localhost:9094)
Deps: confluent-kafka
"""
import argparse
import json
import os
import statistics
import time
from collections import defaultdict

from confluent_kafka import Consumer


def stamp_headers() -> list[tuple[str, bytes]]:
    """Call in the processing job right after reading the input record."""
    return [("start_processing_time_ms", str(time.time_ns() // 1_000_000).encode())]


def p95(values: list[int]) -> float:
    return statistics.quantiles(values, n=20, method="inclusive")[-1] if len(values) > 1 else values[0]


def evaluate_window(minute: int, delays: dict[str, list[int]], sla_ms: int) -> None:
    for kind, values in delays.items():
        if not values:
            continue
        worst, pct = max(values), p95(values)
        status = "MISS" if pct > sla_ms else "ok"
        print(f"[{status}] minute={minute} {kind}: p95={pct:.0f}ms max={worst}ms n={len(values)}")


def run(topic: str, sla_ms: int) -> None:
    consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                         "group.id": "sla-monitor", "auto.offset.reset": "latest"})
    consumer.subscribe([topic])
    windows: dict[int, dict[str, list[int]]] = defaultdict(lambda: {"processing": [], "event_time": []})
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            _, append_ms = msg.timestamp()
            headers = dict(msg.headers() or [])
            minute = append_ms // 60_000
            if "start_processing_time_ms" in headers:
                windows[minute]["processing"].append(append_ms - int(headers["start_processing_time_ms"]))
            event_time_ms = json.loads(msg.value()).get("event_time_ms")
            if event_time_ms:
                windows[minute]["event_time"].append(append_ms - int(event_time_ms))
            # ponytail: close windows 1 minute after their end by wall clock; use a watermark for late data.
            for closed in [m for m in windows if m < time.time() // 60 - 1]:
                evaluate_window(closed, windows.pop(closed), sla_ms)
    finally:
        consumer.close()


def _self_test() -> None:
    assert p95([100]) == 100
    assert p95(list(range(1, 101))) == 95.05
    print("self-test OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="visits-reduced")
    parser.add_argument("--sla-ms", type=int, default=5000)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    _self_test() if args.self_test else run(args.topic, args.sla_ms)

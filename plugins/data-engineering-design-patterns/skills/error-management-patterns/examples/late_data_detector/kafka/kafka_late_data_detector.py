"""Late Data Detector - Kafka consumer with a hand-made watermark.

Equivalent of a Flink ProcessFunction: each record's
event time is compared with the current watermark; on-time records go to the
main topic, late ones to a late-data topic flagged ``is_late`` so the Static or
Dynamic Late Data Integrator can pick them up.

Watermark mechanics:
  * per topic-partition: MAX(event time) seen so far (monotonic);
  * across partitions: MIN (follow the slowest, default) or MAX (follow the
    fastest, smaller buffers but drops more records under event skew);
  * watermark = global event time - allowed lateness, and it never decreases;
  * the watermark advances once per poll batch (like a micro-batch), so all
    records of a poll are judged against the same input watermark.

The tracker state is snapshotted next to the committed offsets (state file
first, then commit) and restored at start-up, so a restart does not reset the
watermark to the epoch and accept very old data as on time.

Run:  python kafka_late_data_detector.py --allowed-lateness-minutes 30
      python kafka_late_data_detector.py --self-test
Requires: pip install kafka-python. Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Optional

log = logging.getLogger("kafka_late_data_detector")

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


class PartitionWatermark:
    def __init__(self, allowed_lateness: dt.timedelta, strategy: str = "min"):
        self.allowed_lateness, self.strategy = allowed_lateness, strategy
        self.partition_max: Dict[str, dt.datetime] = {}
        self.watermark = EPOCH

    def observe(self, partition: str, event_time: dt.datetime) -> None:
        if event_time > self.partition_max.get(partition, EPOCH):
            self.partition_max[partition] = event_time

    def advance(self) -> dt.datetime:
        if self.partition_max:
            agg = min if self.strategy == "min" else max
            self.watermark = max(self.watermark, agg(self.partition_max.values()) - self.allowed_lateness)
        return self.watermark

    def save(self, path: Path) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"watermark": self.watermark.isoformat(),
                                   "partition_max": {p: t.isoformat() for p, t in self.partition_max.items()}}))
        os.replace(tmp, path)

    def load(self, path: Path) -> None:
        if path.exists():
            doc = json.loads(path.read_text())
            self.watermark = dt.datetime.fromisoformat(doc["watermark"])
            self.partition_max = {p: dt.datetime.fromisoformat(t) for p, t in doc["partition_max"].items()}


def parse_event_time(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def run(args: argparse.Namespace) -> None:
    from kafka import KafkaConsumer, KafkaProducer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    state_path = Path(args.state_file)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tracker = PartitionWatermark(dt.timedelta(minutes=args.allowed_lateness_minutes), args.global_strategy)
    tracker.load(state_path)
    consumer = KafkaConsumer(args.input_topic, bootstrap_servers=bootstrap, group_id=args.group,
                             enable_auto_commit=False, auto_offset_reset="earliest")
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=10,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            if not polled:
                continue
            input_watermark, late = tracker.watermark, 0
            for tp, records in polled.items():
                for record in records:
                    visit = json.loads(record.value)
                    event_time = parse_event_time(visit["event_time"])
                    tracker.observe(f"{tp.topic}-{tp.partition}", event_time)
                    is_late = event_time < input_watermark
                    late += is_late
                    producer.send(args.late_topic if is_late else args.output_topic, key=record.key,
                                  value={"visit": visit, "is_late": is_late,
                                         "watermark": input_watermark.isoformat()})
            tracker.advance()
            producer.flush()
            tracker.save(state_path)
            consumer.commit()
            if late:
                log.warning("%s late record(s); watermark %s -> %s", late, input_watermark,
                            tracker.watermark)
    finally:
        producer.close()
        consumer.close()


def _self_check() -> None:
    u = dt.timezone.utc
    tracker = PartitionWatermark(dt.timedelta(minutes=30))
    for minute in (0, 5, 6):
        tracker.observe("visits-0", dt.datetime(2024, 1, 1, 10, minute, tzinfo=u))
    assert tracker.advance() == dt.datetime(2024, 1, 1, 9, 36, tzinfo=u)
    assert parse_event_time("2024-01-01T09:31:00") < tracker.watermark       # late
    tracker.observe("visits-0", dt.datetime(2024, 1, 1, 10, 7, tzinfo=u))
    assert tracker.advance() == dt.datetime(2024, 1, 1, 9, 37, tzinfo=u)
    tracker.observe("visits-1", dt.datetime(2024, 1, 1, 9, 0, tzinfo=u))    # slow partition
    assert tracker.advance() == dt.datetime(2024, 1, 1, 9, 37, tzinfo=u)    # monotonic
    path = Path(os.environ.get("TMP", "/tmp")) / "late_detector_selftest.json"
    tracker.save(path)
    other = PartitionWatermark(dt.timedelta(minutes=30))
    other.load(path)
    path.unlink()
    assert other.watermark == tracker.watermark and other.partition_max == tracker.partition_max
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--input-topic", default="visits")
    p.add_argument("--output-topic", default="visits_on_time")
    p.add_argument("--late-topic", default="visits_late")
    p.add_argument("--group", default="late-data-detector")
    p.add_argument("--allowed-lateness-minutes", type=int, default=30)
    p.add_argument("--global-strategy", choices=["min", "max"], default="min")
    p.add_argument("--state-file", default="/tmp/dp/late-data-detector/kafka-watermark.json")
    run(p.parse_args(argv))


if __name__ == "__main__":
    main()

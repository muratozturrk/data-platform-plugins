"""Dynamic Joiner -- stream-to-stream join in a plain Kafka consumer.

What Spark/Flink/Kafka Streams do natively, written out so the mechanics are
visible:
  * one time-bounded buffer per side (visits, ads), indexed by join key (page)
  * every new row is matched against the OTHER side's buffer, then buffered
  * GC watermark = min(max event time seen per side) - allowed lateness;
    the slower stream drives it, so the faster one waits
  * rows older than the watermark on arrival are late -> dropped and counted
  * on watermark advance: visits whose window [event_time, event_time + delay]
    is closed are evicted; unmatched ones are emitted with ad=None (left outer)

Delivery: output is produced, flushed, THEN offsets are committed. The commit
per partition stops at the oldest record still buffered, so buffered state is
rebuilt from Kafka after a crash. Matches emitted before the crash may be
emitted again (at-least-once) -> downstream dedup on (visit_id, ad_id).
For production prefer Kafka Streams KStream-KStream joins
(JoinWindows.ofTimeDifferenceAndGrace) with a changelog-backed state store.

Run:
  export KAFKA_BOOTSTRAP=localhost:9092
  python kafka_stream_stream_joiner.py               # consume visits + ads
  python kafka_stream_stream_joiner.py --self-test   # pure logic, no Kafka
Prereqs: pip install confluent-kafka
"""
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stream-stream-joiner")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
VISITS_TOPIC = os.environ.get("VISITS_TOPIC", "visits")
ADS_TOPIC = os.environ.get("ADS_TOPIC", "ads")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "visits-with-ads")
MAX_DISPLAY_DELAY_S = int(os.environ.get("MAX_DISPLAY_DELAY_S", "120"))   # join time boundary
ALLOWED_LATENESS_S = int(os.environ.get("ALLOWED_LATENESS_S", "600"))     # GC watermark delay
COMMIT_EVERY = int(os.environ.get("COMMIT_EVERY", "1000"))

Position = Tuple[str, int, int]  # topic, partition, offset


def ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


@dataclass
class Buffered:
    row: dict
    time: float
    position: Optional[Position]
    matched: bool = False


@dataclass
class TimeBoundedJoiner:
    """Left-outer interval join: ad.display_time BETWEEN visit.event_time AND + max_delay."""
    max_delay_s: float
    lateness_s: float
    visits: Dict[str, List[Buffered]] = field(default_factory=lambda: defaultdict(list))
    ads: Dict[str, List[Buffered]] = field(default_factory=lambda: defaultdict(list))
    max_visit_time: Optional[float] = None
    max_ad_time: Optional[float] = None
    late_rows: int = 0

    @property
    def watermark(self) -> Optional[float]:
        if self.max_visit_time is None or self.max_ad_time is None:
            return None  # one side silent -> no eviction yet (state grows: monitor it)
        return min(self.max_visit_time, self.max_ad_time) - self.lateness_s

    def _is_late(self, t: float) -> bool:
        wm = self.watermark
        if wm is not None and t < wm:
            self.late_rows += 1
            return True
        return False

    def add_visit(self, visit: dict, position: Optional[Position] = None) -> List[dict]:
        t = ts(visit["event_time"])
        if self._is_late(t):
            return []
        entry = Buffered(visit, t, position)
        out = [self._joined(visit, ad.row) for ad in self.ads[visit["page"]]
               if t <= ad.time <= t + self.max_delay_s]
        entry.matched = bool(out)
        self.visits[visit["page"]].append(entry)
        self.max_visit_time = max(t, self.max_visit_time or t)
        return out

    def add_ad(self, ad: dict, position: Optional[Position] = None) -> List[dict]:
        t = ts(ad["display_time"])
        if self._is_late(t):
            return []
        out = []
        for v in self.visits[ad["ad_page"]]:
            if v.time <= t <= v.time + self.max_delay_s:
                v.matched = True
                out.append(self._joined(v.row, ad))
        self.ads[ad["ad_page"]].append(Buffered(ad, t, position))
        self.max_ad_time = max(t, self.max_ad_time or t)
        return out

    def evict(self) -> List[dict]:
        """Drop rows that can no longer match; emit unmatched visits with NULL ad."""
        wm = self.watermark
        if wm is None:
            return []
        out = []
        for page, rows in list(self.visits.items()):
            keep = []
            for v in rows:
                if v.time + self.max_delay_s < wm:
                    if not v.matched:
                        out.append(self._joined(v.row, None))
                else:
                    keep.append(v)
            self.visits[page] = keep
        for page, rows in list(self.ads.items()):
            # a future visit has event_time >= wm, so an ad shown before wm can't match it
            self.ads[page] = [a for a in rows if a.time >= wm]
        return out

    def safe_offsets(self, last_seen: Dict[Tuple[str, int], int]) -> Dict[Tuple[str, int], int]:
        """Offset to commit per partition: oldest still-buffered offset, else last seen + 1."""
        commit = {tp: off + 1 for tp, off in last_seen.items()}
        for side in (self.visits, self.ads):
            for rows in side.values():
                for b in rows:
                    if b.position:
                        tp = (b.position[0], b.position[1])
                        commit[tp] = min(commit.get(tp, b.position[2]), b.position[2])
        return commit

    def state_rows(self) -> int:
        return sum(len(r) for r in self.visits.values()) + sum(len(r) for r in self.ads.values())

    @staticmethod
    def _joined(visit: dict, ad: Optional[dict]) -> dict:
        return {**visit, "ad_id": ad["ad_id"] if ad else None,
                "campaign": ad.get("campaign") if ad else None,
                "display_time": ad["display_time"] if ad else None}


def run() -> None:
    from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "stream-stream-joiner",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    joiner = TimeBoundedJoiner(MAX_DISPLAY_DELAY_S, ALLOWED_LATENESS_S)
    last_seen: Dict[Tuple[str, int], int] = {}
    consumer.subscribe([VISITS_TOPIC, ADS_TOPIC])

    def emit(rows: List[dict]) -> None:
        for r in rows:
            producer.produce(OUTPUT_TOPIC, key=r["visit_id"], value=json.dumps(r))
        producer.poll(0)

    def commit() -> None:
        if producer.flush(30) > 0:
            raise KafkaException("output not fully delivered; not committing offsets")
        offsets = [TopicPartition(t, p, o) for (t, p), o in joiner.safe_offsets(last_seen).items()]
        if offsets:
            consumer.commit(offsets=offsets, asynchronous=False)
        log.info("committed; state_rows=%d late_rows=%d watermark=%s",
                 joiner.state_rows(), joiner.late_rows, joiner.watermark)

    processed = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            pos = (msg.topic(), msg.partition(), msg.offset())
            try:
                row = json.loads(msg.value())
                emit(joiner.add_visit(row, pos) if msg.topic() == VISITS_TOPIC else joiner.add_ad(row, pos))
            except (ValueError, KeyError) as err:  # poison pill: skip it (or dead-letter it)
                log.error("unparseable record at %s: %s", pos, err)
            last_seen[(msg.topic(), msg.partition())] = msg.offset()
            emit(joiner.evict())
            processed += 1
            if processed % COMMIT_EVERY == 0:
                commit()
    finally:
        commit()
        consumer.close()


def _self_test() -> None:
    j = TimeBoundedJoiner(max_delay_s=120, lateness_s=60)
    assert j.add_visit({"visit_id": "v1", "page": "home", "event_time": "2024-06-01T10:00:00"}, ("visits", 0, 0)) == []
    # ad 90 s after the visit, same page -> match
    out = j.add_ad({"ad_id": "a1", "ad_page": "home", "display_time": "2024-06-01T10:01:30"}, ("ads", 0, 0))
    assert [r["ad_id"] for r in out] == ["a1"]
    # ad 3 min after -> outside the time boundary, no match
    assert j.add_ad({"ad_id": "a2", "ad_page": "home", "display_time": "2024-06-01T10:03:00"}) == []
    j.add_visit({"visit_id": "v2", "page": "cart", "event_time": "2024-06-01T10:02:00"}, ("visits", 0, 1))
    # watermark = min(10:02, 10:03) - 60 s = 10:01 -> nothing expired yet
    assert j.evict() == [] and j.safe_offsets({("visits", 0): 1, ("ads", 0): 0}) == {("visits", 0): 0, ("ads", 0): 0}
    # both sides move to 10:10 -> watermark 10:09: v1 evicted silently (matched), v2 emitted with NULL ad
    j.add_visit({"visit_id": "v3", "page": "x", "event_time": "2024-06-01T10:10:00"})
    j.add_ad({"ad_id": "a3", "ad_page": "y", "display_time": "2024-06-01T10:10:00"})
    expired = j.evict()
    assert [(r["visit_id"], r["ad_id"]) for r in expired] == [("v2", None)]
    # a visit older than the watermark is late and dropped
    assert j.add_visit({"visit_id": "v0", "page": "home", "event_time": "2024-06-01T10:00:00"}) == []
    assert j.late_rows == 1
    assert j.safe_offsets({("visits", 0): 1, ("ads", 0): 0}) == {("visits", 0): 2, ("ads", 0): 1}
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else run()

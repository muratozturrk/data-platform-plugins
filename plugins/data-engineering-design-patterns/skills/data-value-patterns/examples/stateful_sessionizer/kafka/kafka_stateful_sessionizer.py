"""Stateful Sessionizer -- hand-rolled state store on plain Kafka consumers.

Without a framework you code the state store interactions yourself: a fast, volatile in-memory store plus a fault-tolerant copy. Here:

  in-memory store   dict per assigned input partition: visit_id -> in-flight session
  fault tolerance   compacted changelog topic, co-partitioned with the input
                    (state of input partition N lives in changelog partition N)
  checkpoint        every CHECKPOINT_INTERVAL_S: emit completed sessions, write
                    changed state (tombstones for removed keys), flush, then
                    commit input offsets -> at-least-once on restart

Session logic mirrors the Spark sample: resume or create, accumulate pages in
event-time order (dedupe replays), expire on EVENT TIME: a session closes when the
partition watermark (max event time - allowed lateness) passes
base watermark + gap. Processing-time expiry is avoided on purpose: a slow
consumer (write retries) would close sessions too early.

Rebalance = state rebalancing: newly assigned partitions are not
processed until their state is restored from the changelog.

Preconditions: producers key visits by visit_id; changelog has the same partition
count as the input topic and cleanup.policy=compact.

Run:   python kafka_stateful_sessionizer.py
Test:  python kafka_stateful_sessionizer.py --self-test      (no broker needed)
Env:   KAFKA_BOOTSTRAP, VISITS_TOPIC, SESSIONS_TOPIC, CHANGELOG_TOPIC, CONSUMER_GROUP,
       SESSION_GAP_MINUTES, ALLOWED_LATENESS_S, CHECKPOINT_INTERVAL_S
Prereqs: pip install kafka-python
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kafka_stateful_sessionizer")


@dataclass(frozen=True)
class Config:
    bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    visits_topic: str = os.environ.get("VISITS_TOPIC", "visits")
    sessions_topic: str = os.environ.get("SESSIONS_TOPIC", "sessions")              # compacted, key = session_id
    changelog_topic: str = os.environ.get("CHANGELOG_TOPIC", "sessionizer-state-changelog")
    group_id: str = os.environ.get("CONSUMER_GROUP", "stateful-sessionizer")
    gap_ms: int = int(os.environ.get("SESSION_GAP_MINUTES", "10")) * 60_000
    allowed_lateness_ms: int = int(os.environ.get("ALLOWED_LATENESS_S", "60")) * 1000
    checkpoint_interval_s: int = int(os.environ.get("CHECKPOINT_INTERVAL_S", "15"))


# -- Pure state logic --------------------------------------------------------------
def to_ms(iso_ts: str) -> int:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


@dataclass
class PartitionSessions:
    """In-memory state of ONE input partition plus its event-time watermark."""
    gap_ms: int
    allowed_lateness_ms: int
    sessions: dict = field(default_factory=dict)      # visit_id -> {"user_id", "visits", "timeout_ms"}
    max_event_ms: int = 0
    dirty: set = field(default_factory=set)            # keys to write to the changelog
    late_events: int = 0

    @property
    def watermark_ms(self) -> int:
        return max(self.max_event_ms - self.allowed_lateness_ms, 0) if self.max_event_ms else 0

    def apply(self, event: dict) -> None:
        """Create or resume, then accumulate one visit event."""
        event_ms = to_ms(event["event_time"])
        if self.watermark_ms and event_ms < self.watermark_ms:
            self.late_events += 1                      # same rule as Spark: rows behind the watermark are dropped
            return
        visit_id = event["visit_id"]
        state = self.sessions.setdefault(visit_id, {"user_id": event.get("user_id"), "visits": [], "timeout_ms": 0})
        pair = [event.get("page"), event_ms]
        if pair not in state["visits"]:                # replay after restart -> no duplicate page
            state["visits"].append(pair)
            state["visits"].sort(key=lambda v: (v[1], v[0] or ""))
        self.max_event_ms = max(self.max_event_ms, event_ms)
        # Watermark INCLUDING this event (no watermark yet -> event time).
        # A stale pre-event watermark could expire a fresh session when event time jumps > gap.
        base = self.watermark_ms or event_ms
        state["timeout_ms"] = base + self.gap_ms
        self.dirty.add(visit_id)

    def expire(self) -> list[dict]:
        """Finalize every session whose event-time timeout the watermark has passed."""
        wm = self.watermark_ms
        done = [k for k, s in self.sessions.items() if wm and s["timeout_ms"] <= wm]
        completed = [build_session(k, self.sessions.pop(k)) for k in done]
        self.dirty.update(done)                        # popped keys -> changelog tombstones
        return completed


def build_session(visit_id: str, state: dict) -> dict:
    visits = state["visits"]
    start_ms, end_ms = visits[0][1], visits[-1][1]
    return {"session_id": f"{visit_id}-{start_ms}",   # deterministic, never wall-clock
            "visit_id": visit_id, "user_id": state["user_id"],
            "start_time_ms": start_ms, "end_time_ms": end_ms,
            "visited_pages": [{"page": p, "event_time_as_ms": t} for p, t in visits],
            "duration_in_milliseconds": end_ms - start_ms, "is_completed": True}


# -- Kafka wiring ------------------------------------------------------------------
class StatefulSessionizer:
    def __init__(self, cfg: Config):
        from kafka import KafkaConsumer, KafkaProducer
        self.cfg = cfg
        self.state: dict[int, PartitionSessions] = {}
        self.consumer = KafkaConsumer(
            bootstrap_servers=cfg.bootstrap, group_id=cfg.group_id, enable_auto_commit=False,
            auto_offset_reset="earliest", max_poll_records=500,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")))
        self.producer = KafkaProducer(
            bootstrap_servers=cfg.bootstrap, acks="all", retries=10,
            max_in_flight_requests_per_connection=1,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: None if v is None else json.dumps(v).encode("utf-8"))

    # Rebalance listener: state follows the partition.
    def on_partitions_revoked(self, revoked) -> None:
        if revoked:
            self.checkpoint()
        for tp in revoked:
            self.state.pop(tp.partition, None)
        log.info("revoked=%s", sorted(tp.partition for tp in revoked))

    def on_partitions_assigned(self, assigned) -> None:
        for tp in assigned:
            self.state[tp.partition] = self.restore(tp.partition)
        log.info("assigned=%s restored_keys=%s", sorted(tp.partition for tp in assigned),
                 {p: len(s.sessions) for p, s in self.state.items()})

    def restore(self, partition: int) -> PartitionSessions:
        """Rebuild the in-memory store from the compacted changelog partition."""
        from kafka import KafkaConsumer, TopicPartition
        ps = PartitionSessions(self.cfg.gap_ms, self.cfg.allowed_lateness_ms)
        tp = TopicPartition(self.cfg.changelog_topic, partition)
        reader = KafkaConsumer(bootstrap_servers=self.cfg.bootstrap, enable_auto_commit=False)
        try:
            reader.assign([tp])
            reader.seek_to_beginning(tp)
            end = reader.end_offsets([tp])[tp]
            while reader.position(tp) < end:
                for rec in reader.poll(timeout_ms=1000).get(tp, []):
                    key = rec.key.decode("utf-8")
                    if key == "__watermark__":
                        ps.max_event_ms = int(rec.value)
                    elif rec.value is None:
                        ps.sessions.pop(key, None)
                    else:
                        ps.sessions[key] = json.loads(rec.value)
        finally:
            reader.close()
        return ps

    def checkpoint(self) -> None:
        """Outputs -> changelog -> flush -> commit. A crash anywhere before the
        commit replays input; dedupe + deterministic session_id absorb it."""
        for partition, ps in self.state.items():
            for session in ps.expire():
                self.producer.send(self.cfg.sessions_topic, key=session["session_id"], value=session)
            for key in ps.dirty:
                self.producer.send(self.cfg.changelog_topic, key=key, value=ps.sessions.get(key),
                                   partition=partition)
            self.producer.send(self.cfg.changelog_topic, key="__watermark__", value=ps.max_event_ms,
                               partition=partition)
            ps.dirty.clear()
        self.producer.flush()
        self.consumer.commit()

    def run(self) -> None:
        from kafka import ConsumerRebalanceListener

        outer = self

        class Listener(ConsumerRebalanceListener):
            def on_partitions_revoked(self, revoked):
                outer.on_partitions_revoked(revoked)

            def on_partitions_assigned(self, assigned):
                outer.on_partitions_assigned(assigned)

        self.consumer.subscribe([self.cfg.visits_topic], listener=Listener())
        last_checkpoint = time.monotonic()
        try:
            while True:
                for tp, records in self.consumer.poll(timeout_ms=1000).items():
                    ps = self.state.setdefault(tp.partition, PartitionSessions(self.cfg.gap_ms,
                                                                                self.cfg.allowed_lateness_ms))
                    for rec in records:
                        try:
                            ps.apply(rec.value)
                        except (KeyError, TypeError, ValueError) as exc:
                            log.warning("skip malformed record p=%d o=%d: %s", tp.partition, rec.offset, exc)
                # Checkpoint on a cadence, not per record: fewer changelog writes, larger replay window.
                if time.monotonic() - last_checkpoint >= self.cfg.checkpoint_interval_s:
                    self.checkpoint()
                    last_checkpoint = time.monotonic()
                    log.info("checkpoint | in_flight=%d late_dropped=%d",
                             sum(len(s.sessions) for s in self.state.values()),
                             sum(s.late_events for s in self.state.values()))
        except KeyboardInterrupt:
            log.info("stopping")
        finally:
            self.checkpoint()
            self.consumer.close()
            self.producer.close()


def _self_check() -> None:
    ps = PartitionSessions(gap_ms=10 * 60_000, allowed_lateness_ms=60_000)
    ps.apply({"visit_id": "A", "user_id": "u1", "page": "home", "event_time": "2024-01-01T10:00:00Z"})
    ps.apply({"visit_id": "A", "user_id": "u1", "page": "p1", "event_time": "2024-01-01T10:01:00Z"})
    ps.apply({"visit_id": "A", "user_id": "u1", "page": "p1", "event_time": "2024-01-01T10:01:00Z"})  # replay
    assert len(ps.sessions["A"]["visits"]) == 2 and ps.expire() == []
    ps.apply({"visit_id": "B", "user_id": "u2", "page": "home", "event_time": "2024-01-01T10:15:00Z"})
    done = ps.expire()                                  # watermark 10:14 >= A timeout (10:00 + 10 min)
    assert [s["visit_id"] for s in done] == ["A"] and "A" in ps.dirty and "A" not in ps.sessions
    assert done[0]["session_id"] == f"A-{to_ms('2024-01-01T10:00:00Z')}"
    assert done[0]["duration_in_milliseconds"] == 60_000
    ps.apply({"visit_id": "C", "user_id": "u3", "page": "x", "event_time": "2024-01-01T09:00:00Z"})
    assert ps.late_events == 1 and "C" not in ps.sessions
    print("self-check OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    if parser.parse_args().self_test:
        _self_check()
    else:
        StatefulSessionizer(Config()).run()

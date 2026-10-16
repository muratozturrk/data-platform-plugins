"""
Keyed Idempotency on Apache Kafka -- session keys from the broker append time.

Consumes `visits` (message key = user_id, JSON value {user_id, page, event_time}),
builds sessions per user with an inactivity gap and produces them to a COMPACTED
`sessions` topic keyed by session_id = sha256(user_id | first append time).

Why it is idempotent:
- The key uses the broker append time (message.timestamp.type=LogAppendTime),
  which never changes for a stored record. Event time is mutable for this
  purpose: late events would move "the first event".
  The topic config is verified at start-up; CreateTime (producer-set) is
  rejected unless ALLOW_CREATE_TIME=1.
- Offsets are committed only up to the first record of the oldest OPEN
  session. After a crash the consumer re-reads every open session from its
  first record, so it recomputes the same min append time and the same key.
- Partial sessions are emitted with the final key; later versions overwrite them.

Consequences you still live with:
- Kafka is append-only: the overwrite happens through asynchronous log
  compaction, so consumers can see several versions of one session for a while.
  They share the key, so downstream can keep the latest one per key.
- Mutable data source: if retention/compaction on `visits` deletes the first
  event of a session before a replay, the replay derives a different key. That
  breaks strict idempotency, but the data really did change.

Run:   pip install confluent-kafka ; python kafka_append_time_session_keys.py
Env:   KAFKA_BROKERS, VISITS_TOPIC, SESSIONS_TOPIC, GROUP_ID, SESSION_GAP_SECONDS,
       ALLOW_CREATE_TIME. (SASL credentials, if any: KAFKA_SASL_USERNAME/PASSWORD.)
Self-test (no Kafka needed):  python kafka_append_time_session_keys.py --self-test
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("append_time_session_keys")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
VISITS_TOPIC = os.environ.get("VISITS_TOPIC", "visits")
SESSIONS_TOPIC = os.environ.get("SESSIONS_TOPIC", "sessions")
GROUP_ID = os.environ.get("GROUP_ID", "sessions-keyed-idempotency")
SESSION_GAP_MS = int(os.environ.get("SESSION_GAP_SECONDS", "1800")) * 1000
ALLOW_CREATE_TIME = os.environ.get("ALLOW_CREATE_TIME") == "1"


# -- Pure session logic --------------------------------------------------------
def session_key(user_id: str, first_append_ms: int) -> str:
    """Deterministic key from immutable attributes only (no now(), no UUID)."""
    return hashlib.sha256(f"{user_id}|{first_append_ms}".encode()).hexdigest()


@dataclass
class OpenSession:
    user_id: str
    partition: int
    first_offset: int
    first_append_ms: int
    last_append_ms: int
    pages: list[str] = field(default_factory=list)

    def to_record(self, closed: bool) -> dict:
        return {"session_id": session_key(self.user_id, self.first_append_ms),
                "user_id": self.user_id, "pages": self.pages,
                "start_append_ms": self.first_append_ms,
                "last_append_ms": self.last_append_ms, "closed": closed}


class SessionBuilder:
    """Gap-based sessions on the append-time clock of each partition."""

    def __init__(self, gap_ms: int):
        self.gap_ms = gap_ms
        self.open: dict[str, OpenSession] = {}
        self.clock: dict[int, int] = {}          # partition -> max append time
        self.next_offset: dict[int, int] = {}    # partition -> last offset + 1

    def add(self, user_id: str, page: str, append_ms: int,
            partition: int, offset: int) -> list[OpenSession]:
        closed = []
        current = self.open.get(user_id)
        if current and append_ms - current.last_append_ms > self.gap_ms:
            closed.append(self.open.pop(user_id))
            current = None
        if current is None:
            current = OpenSession(user_id, partition, offset, append_ms, append_ms)
            self.open[user_id] = current
        current.last_append_ms = max(current.last_append_ms, append_ms)
        current.pages.append(page)
        self.clock[partition] = max(self.clock.get(partition, 0), append_ms)
        self.next_offset[partition] = offset + 1
        return closed

    def expire(self) -> list[OpenSession]:
        """Closes sessions idle for > gap on their partition's append clock."""
        expired = [s for s in self.open.values()
                   if self.clock.get(s.partition, 0) - s.last_append_ms > self.gap_ms]
        for s in expired:
            del self.open[s.user_id]
        return expired

    def safe_offsets(self) -> dict[int, int]:
        """Commit position per partition that lets a restart rebuild open sessions."""
        safe = dict(self.next_offset)
        for s in self.open.values():
            safe[s.partition] = min(safe.get(s.partition, s.first_offset), s.first_offset)
        return safe


# -- Kafka wiring ----------------------------------------------------------------
def _client_config() -> dict:
    conf = {"bootstrap.servers": KAFKA_BROKERS}
    if os.environ.get("KAFKA_SASL_USERNAME"):
        conf.update({"security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
                     "sasl.username": os.environ["KAFKA_SASL_USERNAME"],
                     "sasl.password": os.environ["KAFKA_SASL_PASSWORD"]})
    return conf


def verify_append_time(admin, topic: str) -> None:
    from confluent_kafka.admin import ConfigResource

    futures = admin.describe_configs([ConfigResource("topic", topic)])
    configs = next(iter(futures.values())).result()
    ts_type = configs["message.timestamp.type"].value
    if ts_type != "LogAppendTime":
        msg = (f"{topic} uses message.timestamp.type={ts_type}; session keys need "
               "the broker-controlled LogAppendTime")
        if not ALLOW_CREATE_TIME:
            raise RuntimeError(msg)
        log.warning("%s -- continuing because ALLOW_CREATE_TIME=1", msg)


def verify_compacted(admin, topic: str) -> None:
    from confluent_kafka.admin import ConfigResource

    futures = admin.describe_configs([ConfigResource("topic", topic)])
    policy = next(iter(futures.values())).result()["cleanup.policy"].value
    if "compact" not in policy:
        raise RuntimeError(f"{topic} must have cleanup.policy=compact (is {policy})")


def run() -> None:
    from confluent_kafka import (Consumer, KafkaException, Producer,
                                 TIMESTAMP_LOG_APPEND_TIME, TopicPartition)
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(_client_config())
    verify_append_time(admin, VISITS_TOPIC)
    verify_compacted(admin, SESSIONS_TOPIC)

    consumer = Consumer({**_client_config(), "group.id": GROUP_ID,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    # Idempotent producer: no duplicates from internal retries within a session.
    # Replays after a restart are covered by the deterministic key instead.
    producer = Producer({**_client_config(), "enable.idempotence": True, "acks": "all"})
    builder = SessionBuilder(SESSION_GAP_MS)

    def emit(session: OpenSession, closed: bool) -> None:
        record = session.to_record(closed)
        producer.produce(SESSIONS_TOPIC, key=record["session_id"],
                         value=json.dumps(record).encode())

    consumer.subscribe([VISITS_TOPIC])
    try:
        while True:
            messages = consumer.consume(num_messages=500, timeout=1.0)
            touched: dict[str, OpenSession] = {}
            for msg in messages:
                if msg.error():
                    raise KafkaException(msg.error())
                ts_type, append_ms = msg.timestamp()
                if ts_type != TIMESTAMP_LOG_APPEND_TIME and not ALLOW_CREATE_TIME:
                    raise RuntimeError(f"offset {msg.offset()} has no append time")
                try:
                    visit = json.loads(msg.value())
                    user_id, page = str(visit["user_id"]), str(visit["page"])
                except (ValueError, KeyError, TypeError):
                    log.warning("skipping unparsable visit at %s:%d",
                                msg.partition(), msg.offset())
                    continue
                for done in builder.add(user_id, page, append_ms,
                                        msg.partition(), msg.offset()):
                    emit(done, closed=True)
                touched[user_id] = builder.open[user_id]
            for done in builder.expire():
                touched.pop(done.user_id, None)
                emit(done, closed=True)
            for session in touched.values():
                emit(session, closed=False)   # partial result, same final key
            if not messages and not touched:
                continue
            # Commit only after the broker acknowledged every produced session.
            if producer.flush(30) > 0:
                raise RuntimeError("sessions not acknowledged; not committing offsets")
            offsets = [TopicPartition(VISITS_TOPIC, p, o)
                       for p, o in builder.safe_offsets().items()]
            if offsets:
                consumer.commit(offsets=offsets, asynchronous=False)
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        producer.flush(30)
        consumer.close()


def _self_check() -> None:
    gap = 1000
    b = SessionBuilder(gap)
    assert b.add("u1", "home", 10_000, 0, 100) == []
    assert b.add("u1", "cart", 10_500, 0, 101) == []
    assert b.safe_offsets() == {0: 100}            # open session pins the offset
    closed = b.add("u1", "home", 12_000, 0, 102)   # gap exceeded -> new session
    assert len(closed) == 1 and closed[0].pages == ["home", "cart"]
    first_key = closed[0].to_record(True)["session_id"]
    assert first_key == session_key("u1", 10_000)
    assert b.safe_offsets() == {0: 102}
    b.add("u2", "faq", 14_000, 0, 103)             # advances partition clock
    expired = b.expire()
    assert [s.user_id for s in expired] == ["u1"]
    # Replaying from the safe offset rebuilds the same key.
    replay = SessionBuilder(gap)
    replay.add("u1", "home", 10_000, 0, 100)
    replay.add("u1", "cart", 10_500, 0, 101)
    again = replay.add("u1", "x", 12_000, 0, 102)
    assert again[0].to_record(True)["session_id"] == first_key
    print("kafka_append_time_session_keys self-check OK")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        _self_check()
    else:
        run()

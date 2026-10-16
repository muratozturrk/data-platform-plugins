"""Partial State Writer - session state with early (partial) emissions (confluent-kafka).

* Each user's session is updated per visit; every EMIT_EVERY visits a PARTIAL version is
  produced with is_final=false.
* A session closes when the event-time watermark (max event time seen - allowed lateness)
  passes its end + inactivity gap; the closed version is produced with is_final=true.
* Output topic `sessions` is keyed by session_id and should be log-compacted
  (cleanup.policy=compact): consumers reading the latest value per key get upsert
  semantics without MERGE.
Late data after a final emission opens a new session (or must correct the final result).

Run:  python partial_state_consumer.py
Test: python partial_state_consumer.py --self-test
Env:  KAFKA_BOOTSTRAP
Deps: confluent-kafka
"""
import json
import os
import sys

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
EMIT_EVERY = 5
GAP_MS = 30 * 60 * 1000
ALLOWED_LATENESS_MS = 5 * 60 * 1000


class Sessions:
    def __init__(self) -> None:
        self.open: dict[str, dict] = {}
        self.watermark = 0

    def add(self, user_id: str, event_ms: int) -> list[dict]:
        """Returns the session versions to emit (partial and/or final)."""
        out = []
        session = self.open.get(user_id)
        if session and event_ms - session["end_ms"] > GAP_MS:  # gap -> previous session is over
            out.append(self._close(user_id))
            session = None
        if session is None:
            session = self.open[user_id] = {"session_id": f"{user_id}-{event_ms}", "user_id": user_id,
                                             "start_ms": event_ms, "end_ms": event_ms, "visits": 0}
        session["visits"] += 1
        session["start_ms"], session["end_ms"] = min(session["start_ms"], event_ms), max(session["end_ms"], event_ms)
        if session["visits"] % EMIT_EVERY == 1 or EMIT_EVERY == 1:
            out.append({**session, "is_final": False})
        self.watermark = max(self.watermark, event_ms - ALLOWED_LATENESS_MS)
        return out + self.expire()

    def expire(self) -> list[dict]:
        done = [u for u, s in self.open.items() if s["end_ms"] + GAP_MS < self.watermark]
        return [self._close(u) for u in done]

    def _close(self, user_id: str) -> dict:
        return {**self.open.pop(user_id), "is_final": True}


def main() -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "partial-sessions",
                         "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe(["visits"])
    sessions = Sessions()  # ponytail: in-memory state, lost on restart; use a state store/changelog topic in prod
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            visit = json.loads(msg.value())
            for version in sessions.add(visit["user_id"], int(visit["event_time_ms"])):
                producer.produce("sessions", key=version["session_id"], value=json.dumps(version))
            producer.poll(0)
    finally:
        producer.flush(10)
        consumer.close()


def _self_test() -> None:
    s = Sessions()
    first = s.add("u1", 0)
    assert first[0]["is_final"] is False and first[0]["visits"] == 1
    for t in range(1, 5):
        s.add("u1", t * 1000)
    assert [v["visits"] for v in s.add("u1", 6000)] == [6]          # partial every 5 visits
    closing = s.add("u1", GAP_MS + 10_000_000)                        # long gap -> final + new partial
    assert closing[0]["is_final"] is True and closing[0]["visits"] == 6
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else main()

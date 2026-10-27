"""A source database simulator that emits Debezium-style change events.

Envelope format follows the Debezium PostgreSQL connector with the JSON converter and
schemas disabled (value.converter.schemas.enable=false):
https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events
Each event is keyed by the primary key; a delete is followed by a tombstone (null value),
as Debezium does by default (tombstones.on.delete=true) for Kafka log compaction.
"""
import random

COLUMNS = ("id", "name", "email", "tier")


class CustomerDb:
    def __init__(self, seed: int = 7):
        self.rows: dict[int, dict] = {}
        self.lsn = 1_000
        self.rng = random.Random(seed)

    def _event(self, op, before, after, lsn=None):
        if lsn is None:
            self.lsn += 10
            lsn = self.lsn
        key = {"id": (after or before)["id"]}
        ts = 1_736_500_000_000 + lsn
        return key, {"before": before, "after": after, "op": op, "ts_ms": ts + 5,
                     "source": {"connector": "postgresql", "db": "crm", "schema": "public",
                                "table": "customers", "lsn": lsn, "ts_ms": ts}}

    def insert(self, id_, tier="free"):
        row = {"id": id_, "name": f"customer {id_}", "email": f"c{id_}@example.com", "tier": tier}
        self.rows[id_] = row
        return [self._event("c", None, dict(row))]

    def update(self, id_, **changes):
        before = dict(self.rows[id_])
        self.rows[id_].update(changes)
        return [self._event("u", before, dict(self.rows[id_]))]

    def delete(self, id_):
        before = self.rows.pop(id_)
        key, envelope = self._event("d", before, None)
        return [(key, envelope), (key, None)]          # delete event + tombstone

    def stale_update(self, id_, lsn, **changes):
        """An old change that arrives late (e.g. replayed from another partition or a backfill)."""
        after = {**self.rows[id_], **changes}
        return [self._event("u", dict(self.rows[id_]), after, lsn=lsn)]

    def random_ops(self, n: int, key_space: int = 200):
        events = []
        for _ in range(n):
            id_ = self.rng.randrange(key_space)
            if id_ not in self.rows:
                events += self.insert(id_)
            elif self.rng.random() < 0.2:
                events += self.delete(id_)
            else:
                events += self.update(id_, tier=self.rng.choice(["free", "pro", "enterprise"]),
                                      email=f"c{id_}.{self.lsn}@example.com")
        return events

    def state(self) -> dict[int, tuple]:
        return {i: tuple(r[c] for c in COLUMNS) for i, r in self.rows.items()}

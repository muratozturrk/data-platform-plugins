"""Checkpointer - Kafka consumer with explicit checkpoints.

The "data store based" + "intentional checkpointing action" flavour: the
consumer itself confirms what it processed. ``--mode`` shows the three
delivery outcomes:

  at-least-once  process + write, THEN commit to __consumer_offsets.
                 A crash between the two replays the batch -> duplicates
                 (make the sink idempotent: the UPSERT below).
  at-most-once   commit FIRST, then process. A crash after the commit loses
                 the batch - only for data you can afford to drop.
  transactional  offsets stored in the sink DB in the same transaction as the
                 results; on (re)assignment the consumer seeks to them. The DB is
                 the checkpoint, so output and progress can never diverge.

Checkpoint frequency: one commit per poll (``--max-poll-records``) - larger
polls = less commit overhead, more data to replay after a failure.

DB (sink + transactional checkpoint):
  CREATE TABLE visits (visit_id TEXT PRIMARY KEY, page TEXT, event_time TIMESTAMPTZ);
  CREATE TABLE consumer_offsets (group_id TEXT, topic TEXT, partition INT,
    next_offset BIGINT, PRIMARY KEY (group_id, topic, partition));

Run:  python kafka_offset_checkpointer.py --mode at-least-once
      python kafka_offset_checkpointer.py --self-test
Requires: pip install kafka-python psycopg2-binary. Env: KAFKA_BOOTSTRAP_SERVERS, PG_DSN.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Dict, Iterable, Optional, Tuple

log = logging.getLogger("offset_checkpointer")

UPSERT_VISIT = """
INSERT INTO visits (visit_id, page, event_time) VALUES (%s, %s, %s)
ON CONFLICT (visit_id) DO UPDATE SET page = EXCLUDED.page, event_time = EXCLUDED.event_time
WHERE visits.event_time <= EXCLUDED.event_time
"""
UPSERT_OFFSET = """
INSERT INTO consumer_offsets (group_id, topic, partition, next_offset) VALUES (%s, %s, %s, %s)
ON CONFLICT (group_id, topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset
"""


def next_offsets(records: Iterable[Tuple[str, int, int]]) -> Dict[Tuple[str, int], int]:
    """(topic, partition, offset) -> {(topic, partition): last offset + 1}."""
    result: Dict[Tuple[str, int], int] = {}
    for topic, partition, offset in records:
        result[(topic, partition)] = max(result.get((topic, partition), 0), offset + 1)
    return result


def write_batch(conn, group_id: str, records, store_offsets: bool) -> None:
    with conn, conn.cursor() as cur:            # one transaction
        for r in records:
            visit = json.loads(r.value)
            cur.execute(UPSERT_VISIT, (visit["visit_id"], visit.get("page"), visit["event_time"]))
        if store_offsets:
            for (topic, partition), offset in next_offsets(
                    (r.topic, r.partition, r.offset) for r in records).items():
                cur.execute(UPSERT_OFFSET, (group_id, topic, partition, offset))


def make_seek_listener(consumer, conn, group_id: str):
    from kafka import ConsumerRebalanceListener

    class SeekToStoredOffsets(ConsumerRebalanceListener):
        def on_partitions_revoked(self, revoked):
            pass  # every polled batch is committed in the DB before the next poll

        def on_partitions_assigned(self, assigned):
            with conn.cursor() as cur:
                cur.execute("SELECT topic, partition, next_offset FROM consumer_offsets WHERE group_id = %s",
                            (group_id,))
                stored = {(t, p): o for t, p, o in cur.fetchall()}
            conn.rollback()  # end the read-only transaction
            for tp in assigned:
                if (tp.topic, tp.partition) in stored:
                    consumer.seek(tp, stored[(tp.topic, tp.partition)])

    return SeekToStoredOffsets()


def run(args: argparse.Namespace) -> None:
    import psycopg2
    from kafka import KafkaConsumer

    conn = psycopg2.connect(os.environ["PG_DSN"])
    consumer = KafkaConsumer(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                             group_id=args.group, enable_auto_commit=False,  # checkpoints are ours
                             auto_offset_reset="earliest", max_poll_records=args.max_poll_records)
    if args.mode == "transactional":
        consumer.subscribe([args.topic], listener=make_seek_listener(consumer, conn, args.group))
    else:
        consumer.subscribe([args.topic])
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            records = [r for batch in polled.values() for r in batch]
            if not records:
                continue
            if args.mode == "at-most-once":
                consumer.commit()                     # checkpoint before processing
                write_batch(conn, args.group, records, store_offsets=False)
            elif args.mode == "at-least-once":
                write_batch(conn, args.group, records, store_offsets=False)
                consumer.commit()                     # checkpoint after processing
            else:
                write_batch(conn, args.group, records, store_offsets=True)
                consumer.commit()                     # informational only: keeps lag metrics right
            log.info("checkpointed %s records (%s)", len(records), args.mode)
    finally:
        consumer.close()
        conn.close()


def _self_check() -> None:
    offsets = next_offsets([("visits", 0, 10), ("visits", 0, 12), ("visits", 1, 3)])
    assert offsets == {("visits", 0): 13, ("visits", 1): 4}
    assert "WHERE visits.event_time <= EXCLUDED.event_time" in UPSERT_VISIT
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mode", choices=["at-least-once", "at-most-once", "transactional"],
                   default="at-least-once")
    p.add_argument("--topic", default="visits")
    p.add_argument("--group", default="visits-db-sink")
    p.add_argument("--max-poll-records", type=int, default=500)
    run(p.parse_args(argv))


if __name__ == "__main__":
    main()

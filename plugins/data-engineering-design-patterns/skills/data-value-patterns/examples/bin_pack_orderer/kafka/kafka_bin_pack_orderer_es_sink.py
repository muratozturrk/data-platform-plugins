"""Bin Pack Orderer -- Kafka consumer feeding an Elasticsearch _bulk sink.

Pattern: Kafka gives per-partition order, but the sink's bulk API can
partially commit (some items succeed, some fail). Retrying a naive batch
would write e.g. 10:20 before 10:00 for the same entity. Bin packing fixes it:
each bulk request (bin) holds at most one record per entity, so retrying the
failed items of a bin cannot reorder anything.

Flow per poll window:
  consume up to BATCH_SIZE msgs -> sort by (entity, event_time) -> pack bins
  -> deliver bin 0, 1, 2 ... (retry failed items inside the bin)
  -> commit offsets only after ALL bins are written

Run:
  export KAFKA_BOOTSTRAP=localhost:9092 ES_URL=http://localhost:9200
  export ES_API_KEY=...            # optional; never hardcode
  python kafka_bin_pack_orderer_es_sink.py

Prereqs: pip install confluent-kafka requests
"""
import json
import logging
import os
import time
from typing import Dict, List

import requests
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bin-pack-orderer")

TOPIC = os.environ.get("SOURCE_TOPIC", "visits")
INDEX = os.environ.get("ES_INDEX", "partner-visits")
BATCH_SIZE = 2000
MAX_ATTEMPTS = 5


def pack_into_bins(records: List[Dict], key: str = "visit_id") -> List[List[Dict]]:
    records = sorted(records, key=lambda r: (r[key], r["event_time"]))
    bins: List[List[Dict]] = []
    last, pos = None, 0
    for r in records:
        if r[key] != last:
            last, pos = r[key], 0
        if len(bins) <= pos:
            bins.append([])
        bins[pos].append(r)
        pos += 1
    return bins


class EsBulkSink:
    def __init__(self) -> None:
        self.url = os.environ.get("ES_URL", "http://localhost:9200") + "/_bulk"
        self.headers = {"Content-Type": "application/x-ndjson"}
        if os.environ.get("ES_API_KEY"):
            self.headers["Authorization"] = f"ApiKey {os.environ['ES_API_KEY']}"

    def write(self, records: List[Dict]) -> List[Dict]:
        """Returns the records that failed (partial commit)."""
        body = "".join(
            json.dumps({"index": {"_index": INDEX, "_id": f"{r['visit_id']}-{r['event_time']}"}}) + "\n"
            + json.dumps(r) + "\n" for r in records)
        resp = requests.post(self.url, data=body, headers=self.headers, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if not result.get("errors"):
            return []
        return [records[i] for i, item in enumerate(result["items"]) if item["index"].get("error")]


def deliver_bins(bins: List[List[Dict]], sink: EsBulkSink) -> None:
    for n, pending in enumerate(bins):
        for attempt in range(MAX_ATTEMPTS):
            pending = sink.write(pending)
            if not pending:
                break
            log.warning("bin %d: %d items failed, retry %d", n, len(pending), attempt + 1)
            time.sleep(min(0.2 * 2 ** attempt, 5))
        else:
            # Stop here: no commit, so the whole window is re-read on restart.
            # Deterministic _id makes the replay idempotent.
            raise RuntimeError(f"bin {n} still has {len(pending)} failed items")


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id": "bin-pack-orderer",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    sink = EsBulkSink()
    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=5.0)
            records = [json.loads(m.value()) for m in msgs if m.error() is None]
            if not records:
                continue
            bins = pack_into_bins(records)
            deliver_bins(bins, sink)
            consumer.commit(asynchronous=False)
            log.info("delivered %d records in %d bins", len(records), len(bins))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

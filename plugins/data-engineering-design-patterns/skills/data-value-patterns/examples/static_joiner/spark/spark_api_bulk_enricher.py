"""Static Joiner -- enrichment from an API with bulk lookups.

The reference (IP -> geolocation) lives behind an HTTP API. Calling it once
per record makes the network the bottleneck, so each Spark task:
  1. buffers BUFFER_THRESHOLD rows,
  2. asks the API once for the DISTINCT IPs of the buffer it has not seen yet,
  3. caches the answers for the rest of the partition,
  4. writes the enriched rows to Kafka and flushes.

This variant is NOT idempotent: replaying yesterday's visits asks today's API.
When replays must reproduce the same output, materialize the API into an
SCD table first and use spark_static_joiner_batch.py --mode scd2.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_api_bulk_enricher.py \
      --processing-date 2024-01-15
  python spark_api_bulk_enricher.py --self-test
Env: GEO_API_URL, GEO_API_TOKEN (optional, never hardcoded), KAFKA_BOOTSTRAP,
     VISITS_PATH, OUTPUT_TOPIC. Executors need requests + confluent-kafka.
"""
import argparse
import json
import logging
import os
import sys
import time
from typing import Callable, Dict, Iterable, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-bulk-enricher")

GEO_API_URL = os.environ.get("GEO_API_URL", "http://localhost:8080/geolocation/fetch")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "visits-enriched")
VISITS_PATH = os.environ.get("VISITS_PATH", "/data/silver/visits")
BUFFER_THRESHOLD = 100
MAX_ATTEMPTS = 4
UNKNOWN = {"country": None, "city": None}


def http_fetch(ips: List[str]) -> Dict[str, dict]:
    """One request for many IPs. Retries 5xx/timeouts with backoff, fails on 4xx."""
    import requests

    headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
    if os.environ.get("GEO_API_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['GEO_API_TOKEN']}"
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = requests.get(GEO_API_URL, params={"ips": ",".join(ips)}, headers=headers, timeout=10)
            if resp.status_code < 500:
                resp.raise_for_status()
                return resp.json()["mapped"]
        except (requests.ConnectionError, requests.Timeout) as err:
            log.warning("geo API attempt %d failed: %s", attempt + 1, err)
        time.sleep(min(0.5 * 2 ** attempt, 8))
    raise RuntimeError(f"geo API unavailable after {MAX_ATTEMPTS} attempts")


class WriterWithEnricher:
    """Buffers rows, enriches them with one bulk lookup per buffer, then sinks them."""

    def __init__(self, fetch: Callable[[List[str]], Dict[str, dict]],
                 sink: Callable[[dict], None], threshold: int = BUFFER_THRESHOLD) -> None:
        self.fetch, self.sink, self.threshold = fetch, sink, threshold
        self.buffer: List[dict] = []
        self.enriched_ips: Dict[str, dict] = {}   # per-task cache
        self.api_calls = 0

    def process(self, row: dict) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= self.threshold:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        missing = sorted({r["ip"] for r in self.buffer if r.get("ip")} - self.enriched_ips.keys())
        if missing:
            mapped = self.fetch(missing)
            self.api_calls += 1
            # remember misses too, so an unknown IP is not asked for again
            self.enriched_ips.update({ip: mapped.get(ip, UNKNOWN) for ip in missing})
        for r in self.buffer:
            geo = self.enriched_ips.get(r.get("ip"), UNKNOWN)
            self.sink({**r, "geo": geo, "_ref_miss": geo is UNKNOWN})
        self.buffer = []


def write_partition(rows: Iterable) -> None:
    from confluent_kafka import KafkaException, Producer

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True})

    def on_delivery(err, _msg):
        if err is not None:
            raise KafkaException(err)

    def sink(record: dict) -> None:
        producer.produce(OUTPUT_TOPIC, key=str(record["visit_id"]),
                         value=json.dumps(record, default=str), on_delivery=on_delivery)
        producer.poll(0)

    writer = WriterWithEnricher(http_fetch, sink)
    for row in rows:
        writer.process(row.asDict())
    writer.flush()
    if producer.flush(60) > 0:
        raise RuntimeError("records not delivered to Kafka; failing the task so Spark retries it")


def main() -> None:
    from pyspark.sql import SparkSession, functions as F

    p = argparse.ArgumentParser()
    p.add_argument("--processing-date", required=True)
    args = p.parse_args()
    spark = SparkSession.builder.appName("static-joiner-api-bulk").getOrCreate()
    visits = (spark.read.format("delta").load(VISITS_PATH)
              .filter(F.col("event_date") == args.processing_date)
              .select("visit_id", "user_id", "event_time", "page", "ip"))
    # cluster rows of the same IP in the same task to raise the cache hit ratio
    visits.repartition(64, "ip").foreachPartition(write_partition)
    spark.stop()


def _self_test() -> None:
    calls: List[List[str]] = []
    out: List[dict] = []

    def fake_fetch(ips):
        calls.append(ips)
        return {ip: {"country": "PL", "city": "Warsaw"} for ip in ips if ip != "10.0.0.9"}

    w = WriterWithEnricher(fake_fetch, out.append, threshold=3)
    for i, ip in enumerate(["1.1.1.1", "1.1.1.1", "10.0.0.9", "1.1.1.1", "2.2.2.2"]):
        w.process({"visit_id": i, "ip": ip})
    w.flush()
    assert calls == [["1.1.1.1", "10.0.0.9"], ["2.2.2.2"]], calls   # distinct + cached
    assert len(out) == 5 and out[2]["_ref_miss"] and not out[0]["_ref_miss"]
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else main()

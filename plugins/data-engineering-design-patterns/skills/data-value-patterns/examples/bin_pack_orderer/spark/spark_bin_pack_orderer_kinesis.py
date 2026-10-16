"""Bin Pack Orderer -- PySpark batch job writing ordered events to Kinesis.

Pattern: deliver per-entity ordered records through a bulk API that has
PARTIAL COMMIT semantics (Kinesis PutRecords, DynamoDB BatchWriteItem,
Elasticsearch _bulk) without breaking the order.

  1. sortWithinPartitions(entity, event_time)  -- local sort, no shuffle
     (repartition by entity first if the input is not already partitioned by it)
  2. pack: the n-th record of every entity goes to bin n, so every bin holds
     at most ONE record per entity
  3. deliver bins sequentially; retry only the failed records of the current
     bin; never start bin n+1 until bin n is fully written

Run:
  spark-submit spark_bin_pack_orderer_kinesis.py \
      --input /data/silver/visits/date=2024-01-15 --stream visits-ordered
  python spark_bin_pack_orderer_kinesis.py --self-test   # no Spark needed

Prereqs: pyspark, boto3, AWS credentials from the default provider chain
(env vars / instance profile), never hardcoded.
"""
import argparse
import json
import time
from typing import Callable, Iterable, List, Sequence

MAX_BULK_SIZE = 500          # Kinesis PutRecords limit per request
MAX_ATTEMPTS = 5


def pack_into_bins(rows: Iterable, key: Callable) -> List[list]:
    """rows MUST be sorted by (key, event_time). Returns bins where each bin
    contains at most one row per key, preserving per-key order across bins."""
    bins: List[list] = []
    last_key, position = object(), 0
    for row in rows:
        k = key(row)
        if k != last_key:
            last_key, position = k, 0
        if len(bins) <= position:
            bins.append([])
        bins[position].append(row)
        position += 1
    return bins


def deliver_bin(records: Sequence[dict], put_bulk: Callable[[Sequence[dict]], List[int]]) -> None:
    """put_bulk returns indices of records that failed (partial commit).
    Retrying them is safe for ordering: a bin has one record per entity."""
    pending = list(records)
    for attempt in range(MAX_ATTEMPTS):
        failed: List[dict] = []
        for start in range(0, len(pending), MAX_BULK_SIZE):
            chunk = pending[start:start + MAX_BULK_SIZE]
            failed.extend(chunk[i] for i in put_bulk(chunk))
        if not failed:
            return
        pending = failed
        time.sleep(min(2 ** attempt * 0.2, 5))
    # Fail the task: continuing would deliver bin n+1 before bin n.
    raise RuntimeError(f"{len(pending)} records still failing after {MAX_ATTEMPTS} attempts")


def kinesis_put_bulk(stream: str):
    import boto3
    client = boto3.client("kinesis")

    def put(records: Sequence[dict]) -> List[int]:
        resp = client.put_records(StreamName=stream, Records=[
            {"Data": json.dumps(r, default=str).encode("utf-8"), "PartitionKey": str(r["visit_id"])}
            for r in records])
        if resp["FailedRecordCount"] == 0:
            return []
        return [i for i, res in enumerate(resp["Records"]) if "ErrorCode" in res]
    return put


def write_partition(stream: str):
    def _write(rows) -> None:
        put = kinesis_put_bulk(stream)
        dict_rows = (r.asDict() for r in rows)
        for delivery_bin in pack_into_bins(dict_rows, key=lambda r: r["visit_id"]):
            deliver_bin(delivery_bin, put)
    return _write


def main() -> None:
    from pyspark.sql import SparkSession, functions as F

    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--stream", required=True)
    args = p.parse_args()

    spark = SparkSession.builder.appName("bin-pack-orderer").getOrCreate()
    events = spark.read.json(args.input)
    # repartition only if the input isn't already partitioned by the entity
    (events.repartition("visit_id")
        .sortWithinPartitions(F.col("visit_id"), F.col("event_time"))
        .foreachPartition(write_partition(args.stream)))


def _self_test() -> None:
    rows = [{"visit_id": v, "event_time": t} for v, t in
            [("a", 1), ("a", 2), ("a", 3), ("b", 1), ("c", 1), ("c", 2)]]
    bins = pack_into_bins(rows, key=lambda r: r["visit_id"])
    assert [[(r["visit_id"], r["event_time"]) for r in b] for b in bins] == [
        [("a", 1), ("b", 1), ("c", 1)], [("a", 2), ("c", 2)], [("a", 3)]]
    # partial commit: first call fails index 1, retry succeeds
    calls = []
    deliver_bin(bins[0], lambda recs: calls.append(len(recs)) or ([1] if len(calls) == 1 else []))
    assert calls == [3, 1]
    print("self-test OK")


if __name__ == "__main__":
    import sys
    _self_test() if "--self-test" in sys.argv else main()

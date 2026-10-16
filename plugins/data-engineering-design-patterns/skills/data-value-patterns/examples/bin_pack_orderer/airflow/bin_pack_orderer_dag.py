"""Bin Pack Orderer -- Airflow 2.x TaskFlow DAG (partner API sync).

Scenario: every 10 minutes, take the buffered partner visit events and
push them, per partner and in event-time order, to a Kinesis stream that has
partial-commit bulk semantics (PutRecords).

Ordering guarantees here:
  * inside a run   -> bins (one record per visit_id per bulk request)
  * across runs    -> max_active_runs=1 + depends_on_past=True
  * task retries   -> RE-EMIT already delivered bins (pattern limitation);
                      consumers must dedup on the event id.

Run: drop into dags/, set Variables `bin_pack_staging_dir` and optional
`bin_pack_stream_prefix`; AWS creds via connection/instance profile.
Prereqs: apache-airflow>=2.4, boto3.
"""
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable

MAX_ATTEMPTS = 5
KINESIS_MAX_RECORDS = 500


def pack_into_bins(records: List[Dict]) -> List[List[Dict]]:
    records = sorted(records, key=lambda r: (r["visit_id"], r["event_time"]))
    bins: List[List[Dict]] = []
    last, pos = None, 0
    for r in records:
        if r["visit_id"] != last:
            last, pos = r["visit_id"], 0
        if len(bins) <= pos:
            bins.append([])
        bins[pos].append(r)
        pos += 1
    return bins


@dag(
    schedule=timedelta(minutes=10),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"depends_on_past": True, "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["data-value", "ordering"],
)
def bin_pack_orderer():

    @task
    def list_partners(data_interval_start=None) -> List[str]:
        window = Path(Variable.get("bin_pack_staging_dir")) / data_interval_start.strftime("%Y%m%d%H%M")
        return sorted(p.name for p in window.iterdir() if p.is_dir()) if window.exists() else []

    @task
    def deliver_partner(partner: str, data_interval_start=None) -> Dict:
        import boto3
        window = Path(Variable.get("bin_pack_staging_dir")) / data_interval_start.strftime("%Y%m%d%H%M")
        records = [json.loads(line) for f in sorted((window / partner).glob("*.jsonl"))
                   for line in f.read_text().splitlines() if line]
        stream = Variable.get("bin_pack_stream_prefix", default_var="partner-") + partner
        client = boto3.client("kinesis")
        bins = pack_into_bins(records)
        for n, pending in enumerate(bins):
            for attempt in range(MAX_ATTEMPTS):
                failed = []
                # a bin can exceed the 500-records PutRecords limit: chunks of one bin
                # are safe to send one after another, each holds one record per visit_id
                for start in range(0, len(pending), KINESIS_MAX_RECORDS):
                    chunk = pending[start:start + KINESIS_MAX_RECORDS]
                    resp = client.put_records(StreamName=stream, Records=[
                        {"Data": json.dumps(r).encode(), "PartitionKey": r["visit_id"]} for r in chunk])
                    failed += [chunk[i] for i, res in enumerate(resp["Records"]) if "ErrorCode" in res]
                pending = failed
                if not pending:
                    break
                time.sleep(min(0.2 * 2 ** attempt, 5))
            else:
                raise RuntimeError(f"{partner}: bin {n} not fully delivered; stopping to keep order")
        return {"partner": partner, "records": len(records), "bins": len(bins)}

    deliver_partner.expand(partner=list_partners())


bin_pack_orderer()

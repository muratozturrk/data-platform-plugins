"""In-Place Overwriter (data removal) - Airflow 2.x TaskFlow DAG on S3.

Batches all pending removal requests into ONE rewrite (rewriting terabytes per
request is too expensive), then:
  rewrite -> staging prefix  ->  validate staging  ->  promote staging -> output
The public dataset is only touched after a fully computed staging copy exists,
so a retried/failed rewrite never causes data loss.

Run: put in dags/; env BUCKET, OUTPUT_PREFIX, STAGING_PREFIX, SPARK_JOB (path to
../spark/in_place_overwriter_job.py), REQUESTS_FILE. Needs boto3 + an `aws_default`
connection (or default credential chain) and spark-submit on the worker.
Enable S3 bucket versioning if you need to roll back a buggy removal.
"""
import json
import os
import subprocess
from datetime import datetime

from airflow.decorators import dag, task

BUCKET = os.environ.get("BUCKET", "dp-visits")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/")
SPARK_JOB = os.environ.get("SPARK_JOB", "/opt/jobs/in_place_overwriter_job.py")
REQUESTS_FILE = os.environ.get("REQUESTS_FILE", "/tmp/dp/removal_requests.json")


@dag(schedule="@weekly", start_date=datetime(2024, 1, 1), catchup=False, max_active_runs=1,
     tags=["security", "data-removal"])
def in_place_overwriter():

    @task
    def collect_requests() -> list:
        with open(REQUESTS_FILE) as f:
            return sorted({r["user_id"] for r in json.load(f) if r.get("status") == "pending"})

    @task
    def rewrite_to_staging(user_ids: list) -> list:
        if not user_ids:
            return user_ids
        env = {**os.environ,
               "INPUT_DIR": f"s3a://{BUCKET}/{OUTPUT_PREFIX}",
               "STAGING_DIR": f"s3a://{BUCKET}/{STAGING_PREFIX}",
               "PROMOTE": "false"}  # the job writes + checks staging; promotion is the S3 task below
        subprocess.run(["spark-submit", SPARK_JOB, "json", *user_ids], check=True, env=env)
        return user_ids

    @task
    def validate_staging(user_ids: list) -> list:
        import boto3
        s3 = boto3.client("s3")
        listed = s3.list_objects_v2(Bucket=BUCKET, Prefix=STAGING_PREFIX, MaxKeys=5)
        if user_ids and listed.get("KeyCount", 0) == 0:
            raise ValueError("staging is empty, refusing to promote")
        return user_ids

    @task
    def promote(user_ids: list) -> None:
        if not user_ids:
            return
        import boto3
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(BUCKET)
        bucket.objects.filter(Prefix=OUTPUT_PREFIX).delete()
        for obj in bucket.objects.filter(Prefix=STAGING_PREFIX):
            target = OUTPUT_PREFIX + obj.key[len(STAGING_PREFIX):]
            bucket.copy({"Bucket": BUCKET, "Key": obj.key}, target)
        # delete staging only after the full copy succeeded (copy is not atomic on S3)
        bucket.objects.filter(Prefix=STAGING_PREFIX).delete()

    promote(validate_staging(rewrite_to_staging(collect_requests())))


in_place_overwriter()

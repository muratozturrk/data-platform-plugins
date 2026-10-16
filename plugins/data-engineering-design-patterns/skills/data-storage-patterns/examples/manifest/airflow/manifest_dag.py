"""Manifest - Airflow 2.x: idempotent Redshift COPY driven by a per-run manifest.

1. `create_manifest` lists the run's S3 prefix ONCE and writes
   s3://<bucket>/manifests/<run>.json - but only if it doesn't exist yet, so a
   retried/replayed run loads exactly the same files even if new files arrived.
2. `copy_with_manifest` COPYs the manifest files (`MANIFEST` keyword) into a staging
   table and replaces matching rows in `customer`, all in one transaction.

Run: put in dags/; requires apache-airflow-providers-amazon. AWS credentials and
the Redshift IAM role come from connections / env (REDSHIFT_COPY_ROLE_ARN), never code.
"""
import json
import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = "devices"
MANIFEST_PREFIX = "manifests"


@dag(schedule="@hourly", start_date=datetime(2025, 6, 1), catchup=False,
     tags=["dp", "storage", "manifest"])
def manifest():

    @task
    def create_manifest(ts_nodash=None, ds=None) -> str:
        s3 = S3Hook(aws_conn_id="aws_default")
        key = f"{MANIFEST_PREFIX}/manifest_{ts_nodash}.json"
        if s3.check_for_key(key, bucket_name=BUCKET):
            return f"s3://{BUCKET}/{key}"  # replay: reuse the frozen file list
        files = s3.list_keys(bucket_name=BUCKET, prefix=f"input/date={ds}/")
        if not files:
            raise ValueError(f"no files for {ds}")
        # Redshift requires meta.content_length per entry for columnar (Parquet/ORC) COPY.
        body = {"entries": [{"url": f"s3://{BUCKET}/{f}", "mandatory": True,
                             "meta": {"content_length": s3.head_object(f, BUCKET)["ContentLength"]}}
                            for f in files]}
        s3.load_string(json.dumps(body), key=key, bucket_name=BUCKET)
        return f"s3://{BUCKET}/{key}"

    @task
    def copy_with_manifest(manifest_uri: str):
        role = os.environ["REDSHIFT_COPY_ROLE_ARN"]
        # Staging + delete/insert by key keeps replays idempotent (DELETE, not TRUNCATE:
        # TRUNCATE commits implicitly in Redshift). autocommit=False -> one transaction.
        RedshiftSQLHook(redshift_conn_id="redshift_default").run([
            "DELETE FROM customer_staging",
            f"COPY customer_staging FROM '{manifest_uri}' IAM_ROLE '{role}' FORMAT AS PARQUET MANIFEST",
            "DELETE FROM customer USING customer_staging WHERE customer.id = customer_staging.id",
            "INSERT INTO customer SELECT * FROM customer_staging"])

    copy_with_manifest(create_manifest())


manifest()

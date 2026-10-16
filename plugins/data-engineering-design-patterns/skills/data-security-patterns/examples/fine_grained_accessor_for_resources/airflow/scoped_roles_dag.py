"""Fine-Grained Accessor for Resources - Airflow 2.x TaskFlow with per-task scoped AWS roles.

Each task uses its own Airflow AWS connection whose extra holds a `role_arn`
(the hook calls STS AssumeRole). Roles carry least-privilege, identity-based
policies scoped to ONE bucket/prefix, so the export task physically cannot
touch the raw zone. A bootstrap task shows the policy documents (normally
managed by Terraform - keep IAM in IaC, watch quotas: AWS allows 1,500 custom
policies per account by default).

Run: put in dags/; pip install apache-airflow-providers-amazon; create connections
  aws_raw_reader    extra: {"role_arn": "arn:aws:iam::<acct>:role/visits-raw-reader"}
  aws_export_writer extra: {"role_arn": "arn:aws:iam::<acct>:role/visits-export-writer"}
Env RAW_BUCKET, EXPORT_BUCKET.
"""
import json
import os
from datetime import datetime

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("RAW_BUCKET", "dp-visits-raw")
EXPORT_BUCKET = os.environ.get("EXPORT_BUCKET", "dp-visits-export")


def reader_policy(bucket: str) -> dict:
    return {"Version": "2012-10-17", "Statement": [{
        "Sid": "VisitsS3Reader", "Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
        "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]}]}


def writer_policy(bucket: str, prefix: str) -> dict:
    return {"Version": "2012-10-17", "Statement": [{
        "Sid": "VisitsExportWriter", "Effect": "Allow", "Action": ["s3:PutObject"],
        "Resource": [f"arn:aws:s3:::{bucket}/{prefix}*"]}]}


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["security", "iam"])
def fine_grained_accessor_for_resources():

    @task
    def show_policies() -> None:
        print(json.dumps(reader_policy(RAW_BUCKET), indent=2))
        print(json.dumps(writer_policy(EXPORT_BUCKET, "daily/"), indent=2))

    @task
    def read_raw(ds=None) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id="aws_raw_reader")  # read-only role
        keys = hook.list_keys(bucket_name=RAW_BUCKET, prefix=f"visits/{ds}/") or []
        lines = [hook.read_key(k, RAW_BUCKET) for k in keys]
        return "\n".join(lines)

    @task
    def export(content: str, ds=None) -> None:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id="aws_export_writer")  # write-only role, one prefix
        hook.load_string(content, key=f"daily/{ds}/visits.jsonl", bucket_name=EXPORT_BUCKET, replace=True)

    @task
    def prove_isolation() -> None:
        """The export role must NOT be able to list the raw bucket."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from botocore.exceptions import ClientError
        try:
            S3Hook(aws_conn_id="aws_export_writer").get_conn().list_objects_v2(Bucket=RAW_BUCKET, MaxKeys=1)
        except ClientError as error:
            if error.response["Error"]["Code"] == "AccessDenied":
                return
            raise
        raise PermissionError("export role can read the raw bucket - policy too broad")

    show_policies() >> export(read_raw()) >> prove_isolation()


fine_grained_accessor_for_resources()

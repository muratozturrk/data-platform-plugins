"""Secretless Connector - Airflow 2.x TaskFlow DAG using workload identity, no keys.

boto3's default credential chain resolves short-lived STS credentials from
the worker's identity: IRSA (EKS service account annotated with a role ARN,
AWS_WEB_IDENTITY_TOKEN_FILE injected), ECS task role, or EC2 instance profile.
Nothing to store, rotate or leak. The first task fails fast if someone sneaks
static keys into the environment, the second shows WHO the job runs as.

"Secretless" isn't "workless": the role, its trust policy (sts:AssumeRoleWithWebIdentity
for the service account) and least-privilege permissions must be set up (IaC).

Setup (EKS): kubectl annotate sa airflow-worker \
   eks.amazonaws.com/role-arn=arn:aws:iam::<acct>:role/airflow-visits-reader
Run: put in dags/; pip install boto3; env VISITS_BUCKET.
"""
import os
from datetime import datetime

from airflow.decorators import dag, task

VISITS_BUCKET = os.environ.get("VISITS_BUCKET", "dp-visits")


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["security", "identity"])
def secretless_connector():

    @task
    def refuse_static_keys() -> None:
        leaked = [k for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if os.environ.get(k)]
        if leaked:
            raise PermissionError(f"static credentials found in env: {leaked} - use the workload identity")

    @task
    def whoami() -> str:
        import boto3
        session = boto3.Session()
        method = session.get_credentials().method  # e.g. 'assume-role-with-web-identity', 'iam-role'
        arn = session.client("sts").get_caller_identity()["Arn"]
        print(f"running as {arn} via {method}")
        if method in ("env", "shared-credentials-file", "config-file"):
            raise PermissionError(f"long-lived credentials used ({method})")
        return arn

    @task
    def count_objects(_: str, ds=None) -> int:
        import boto3
        paginator = boto3.client("s3").get_paginator("list_objects_v2")
        total = sum(page.get("KeyCount", 0)
                    for page in paginator.paginate(Bucket=VISITS_BUCKET, Prefix=f"visits/{ds}/"))
        print(f"{total} objects for {ds}")
        return total

    refuse_static_keys() >> count_objects(whoami())


secretless_connector()

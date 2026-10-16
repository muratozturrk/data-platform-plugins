"""Encryptor - Airflow 2.x TaskFlow DAG auditing and enforcing S3 SSE-KMS at rest + TLS in transit.

For each bucket in the data platform:
  1. read its default encryption; if not aws:kms with the expected key -> set it
  2. make sure the KMS key is enabled and NOT pending deletion (data-loss risk)
  3. merge a statement denying non-TLS requests (aws:SecureTransport=false) into the
     bucket's EXISTING policy (other statements are kept; only our Sid is replaced)
Prefer IaC (Terraform `aws_s3_bucket_server_side_encryption_configuration`)
for the declaration; this DAG is the drift detector / remediator.

Run: put in dags/; pip install boto3; env BUCKETS=dp-visits,dp-users KMS_KEY_ARN=arn:aws:kms:...
     ENFORCE=true to remediate (default: audit only, fail on drift).
     Credentials: default chain (instance role/IRSA) - no keys.
     Self-check of the policy merge: python enforce_bucket_encryption_dag.py --self-test
"""
import json
import os
import sys
from datetime import datetime
from typing import Optional

BUCKETS = [b for b in os.environ.get("BUCKETS", "dp-visits").split(",") if b]
KMS_KEY_ARN = os.environ.get("KMS_KEY_ARN", "")
ENFORCE = os.environ.get("ENFORCE", "false") == "true"


TLS_SID = "DenyInsecureTransport"


def tls_only_statement(bucket: str) -> dict:
    return {"Sid": TLS_SID, "Effect": "Deny", "Principal": "*", "Action": "s3:*",
            "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
            "Condition": {"Bool": {"aws:SecureTransport": "false"}}}


def merge_statement(existing_policy: Optional[str], statement: dict) -> str:
    """Return the existing policy with `statement` added, or replacing the statement with the same Sid.
    Every other statement (cross-account grants, other deny rules) is kept untouched."""
    policy = json.loads(existing_policy) if existing_policy else {"Version": "2012-10-17", "Statement": []}
    statements = policy.get("Statement", [])
    if isinstance(statements, dict):  # a single statement may be stored as an object
        statements = [statements]
    policy["Statement"] = [s for s in statements if s.get("Sid") != statement["Sid"]] + [statement]
    return json.dumps(policy, sort_keys=True)


def read_bucket_policy(s3, bucket: str) -> Optional[str]:
    from botocore.exceptions import ClientError
    try:
        return s3.get_bucket_policy(Bucket=bucket)["Policy"]
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchBucketPolicy":
            return None
        raise  # AccessDenied etc. must not be mistaken for "no policy" -> we'd wipe it


def _self_check() -> None:
    other = {"Sid": "PartnerRead", "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::1:root"},
             "Action": "s3:GetObject", "Resource": "arn:aws:s3:::b/*"}
    tls = tls_only_statement("b")
    merged = json.loads(merge_statement(json.dumps({"Version": "2012-10-17", "Statement": other}), tls))
    assert [s["Sid"] for s in merged["Statement"]] == ["PartnerRead", TLS_SID]
    again = json.loads(merge_statement(json.dumps(merged), tls))
    assert again == merged, "merge must be idempotent"
    fresh = json.loads(merge_statement(None, tls))
    assert fresh["Statement"] == [tls] and fresh["Version"] == "2012-10-17"
    print("self-check ok")


if __name__ == "__main__" and "--self-test" in sys.argv:
    _self_check()
    sys.exit(0)

from airflow.decorators import dag, task  # noqa: E402  (after the self-test so it runs without Airflow)


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["security", "encryption"])
def encryptor_enforcement():

    @task
    def check_kms_key() -> None:
        import boto3
        meta = boto3.client("kms").describe_key(KeyId=KMS_KEY_ARN)["KeyMetadata"]
        if meta["KeyState"] != "Enabled":
            raise RuntimeError(f"KMS key state is {meta['KeyState']} - encrypted data may become unreadable")

    @task
    def enforce_bucket(bucket: str) -> str:
        import boto3
        from botocore.exceptions import ClientError
        s3 = boto3.client("s3")
        try:
            rules = s3.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"]["Rules"]
            default = rules[0]["ApplyServerSideEncryptionByDefault"]
        except ClientError as error:
            if error.response["Error"]["Code"] != "ServerSideEncryptionConfigurationNotFoundError":
                raise
            default = {}
        kms_ok = default.get("SSEAlgorithm") == "aws:kms" and default.get("KMSMasterKeyID") == KMS_KEY_ARN
        current = read_bucket_policy(s3, bucket)
        merged = merge_statement(current, tls_only_statement(bucket))
        tls_ok = current is not None and json.loads(current) == json.loads(merged)
        if kms_ok and tls_ok:
            return f"{bucket}: ok"
        if not ENFORCE:
            raise ValueError(f"{bucket}: drift (sse-kms ok={kms_ok}, tls-only ok={tls_ok}, default={default})")
        if not kms_ok:
            s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={"Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": KMS_KEY_ARN},
                "BucketKeyEnabled": True}]})  # bucket keys cut KMS calls (cost/CPU overhead)
        if not tls_ok:
            # ponytail: read-modify-write without a lock; a concurrent policy edit in between is lost
            s3.put_bucket_policy(Bucket=bucket, Policy=merged)
        return f"{bucket}: remediated (sse-kms={not kms_ok}, tls-only={not tls_ok})"

    @task
    def report(results: list) -> None:
        for line in results:
            print(line)

    check_kms_key() >> report(enforce_bucket.expand(bucket=BUCKETS))


encryptor_enforcement()

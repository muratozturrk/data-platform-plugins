"""Probe the security and integrity features that storage vendors are compared on.

Vendor matrices rate "security & compliance" from documentation. This module measures
what an S3 endpoint actually does, with one status per feature:

  enforced       the store applied the rule (a forbidden action was refused, data was recovered, ...)
  accepted       the configuration was stored, but the effect is not observable within a test
  not supported  the request was rejected
  breaks writes  the configuration was accepted and then made normal PUTs fail
"""
import json
import os
import urllib.error
import urllib.request

from botocore.exceptions import ClientError

ENFORCED, ACCEPTED, UNSUPPORTED, BREAKS = "enforced", "accepted", "not supported", "breaks writes"


def _anon_get(s3, bucket, key) -> int:
    try:
        return urllib.request.urlopen(f"{s3.meta.endpoint_url}/{bucket}/{key}", timeout=5).status
    except urllib.error.HTTPError as e:
        return e.code


def bucket_policy(s3, b) -> str:
    s3.put_object(Bucket=b, Key="public/report.csv", Body=b"a,b")
    s3.put_object(Bucket=b, Key="private/pii.csv", Body=b"x,y")
    if _anon_get(s3, b, "public/report.csv") != 403:
        return UNSUPPORTED
    s3.put_bucket_policy(Bucket=b, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject"],
        "Resource": [f"arn:aws:s3:::{b}/public/*"]}]}))
    ok = _anon_get(s3, b, "public/report.csv") == 200 and _anon_get(s3, b, "private/pii.csv") == 403
    return ENFORCED if ok else ACCEPTED


def versioning(s3, b) -> str:
    s3.put_bucket_versioning(Bucket=b, VersioningConfiguration={"Status": "Enabled"})
    s3.put_object(Bucket=b, Key="v/data.parquet", Body=b"v1")
    s3.delete_object(Bucket=b, Key="v/data.parquet")                    # accidental delete
    try:
        restore_deleted(s3, b, "v/data.parquet")
        return ENFORCED if s3.get_object(Bucket=b, Key="v/data.parquet")["Body"].read() == b"v1" else ACCEPTED
    except (ClientError, IndexError):
        return UNSUPPORTED


def restore_deleted(s3, bucket: str, key: str) -> None:
    """Undo a delete on a versioned bucket by removing the latest delete marker."""
    markers = s3.list_object_versions(Bucket=bucket, Prefix=key).get("DeleteMarkers", [])
    latest = [m for m in markers if m["Key"] == key and m["IsLatest"]][0]
    s3.delete_object(Bucket=bucket, Key=key, VersionId=latest["VersionId"])


def create_locked_bucket(s3, b, days: int = 1) -> None:
    s3.create_bucket(Bucket=b, ObjectLockEnabledForBucket=True)
    s3.put_object_lock_configuration(Bucket=b, ObjectLockConfiguration={
        "ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": days}}})


def object_lock(s3, b) -> str:
    try:
        create_locked_bucket(s3, b)
    except ClientError:
        return UNSUPPORTED
    vid = s3.put_object(Bucket=b, Key="ledger/2025.parquet", Body=b"record")["VersionId"]
    try:
        s3.delete_object(Bucket=b, Key="ledger/2025.parquet", VersionId=vid)
    except ClientError as e:
        return ENFORCED if e.response["Error"]["Code"] == "AccessDenied" else ACCEPTED
    return ACCEPTED


def lifecycle(s3, b) -> str:
    rule = {"ID": "expire-tmp", "Status": "Enabled", "Filter": {"Prefix": "tmp/"}, "Expiration": {"Days": 7}}
    try:
        s3.put_bucket_lifecycle_configuration(Bucket=b, LifecycleConfiguration={"Rules": [rule]})
        stored = s3.get_bucket_lifecycle_configuration(Bucket=b)["Rules"]
    except ClientError:
        return UNSUPPORTED
    return ACCEPTED if stored[0]["ID"] == "expire-tmp" else UNSUPPORTED   # 7 days: not observable here


def sse_s3(s3, b) -> str:
    try:
        s3.put_object(Bucket=b, Key="sse/s3", Body=b"secret", ServerSideEncryption="AES256")
    except ClientError:
        return UNSUPPORTED
    return ENFORCED if s3.head_object(Bucket=b, Key="sse/s3").get("ServerSideEncryption") == "AES256" else ACCEPTED


def sse_c(s3, b) -> str:
    key = os.urandom(32)
    try:
        s3.put_object(Bucket=b, Key="sse/c", Body=b"secret", SSECustomerAlgorithm="AES256", SSECustomerKey=key)
    except ClientError:
        return UNSUPPORTED
    try:
        s3.get_object(Bucket=b, Key="sse/c")                         # without the key: must fail
        return ACCEPTED
    except ClientError:
        body = s3.get_object(Bucket=b, Key="sse/c", SSECustomerAlgorithm="AES256",
                             SSECustomerKey=key)["Body"].read()
        return ENFORCED if body == b"secret" else ACCEPTED


def default_encryption(s3, b) -> str:
    try:
        s3.put_bucket_encryption(Bucket=b, ServerSideEncryptionConfiguration={"Rules": [
            {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
    except ClientError:
        return UNSUPPORTED
    try:
        s3.put_object(Bucket=b, Key="sse/default", Body=b"plain request")
    except ClientError:
        return BREAKS
    return ENFORCED if s3.head_object(Bucket=b, Key="sse/default").get("ServerSideEncryption") == "AES256" else ACCEPTED


def tagging(s3, b) -> str:
    s3.put_object(Bucket=b, Key="t/obj", Body=b"t", Tagging="classification=pii")
    tags = s3.get_object_tagging(Bucket=b, Key="t/obj")["TagSet"]
    return ENFORCED if tags == [{"Key": "classification", "Value": "pii"}] else UNSUPPORTED


FEATURES = {
    "bucket_policy": bucket_policy,
    "versioning": versioning,
    "object_lock": object_lock,
    "lifecycle": lifecycle,
    "sse_s3": sse_s3,
    "sse_c": sse_c,
    "tagging": tagging,
    "default_encryption": default_encryption,     # last: it may break the bucket
}


def feature_matrix(s3, bucket_prefix: str) -> dict[str, str]:
    """One fresh bucket per feature, so a feature that breaks a bucket cannot affect the others."""
    out = {}
    for name, probe in FEATURES.items():
        bucket = f"{bucket_prefix}-{name.replace('_', '-')}"
        if name != "object_lock":
            s3.create_bucket(Bucket=bucket)
        try:
            out[name] = probe(s3, bucket)
        except ClientError:
            out[name] = UNSUPPORTED
    return out


def retained_versions(s3, bucket: str, prefix: str) -> list[dict]:
    return [v for v in s3.list_object_versions(Bucket=bucket, Prefix=prefix).get("Versions", [])]

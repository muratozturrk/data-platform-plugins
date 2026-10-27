"""S3 API compatibility checks for an Iceberg storage layer (the operations S3FileIO uses).

Rule: "evaluate not only whether a storage solution claims S3 compatibility, but also how
mature the implementation is." Each check is one S3 behavior, tagged with who relies on it.
Iceberg S3FileIO operations: https://iceberg.apache.org/docs/1.8.1/aws/#s3-fileio
AWS reference behavior: https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html
"""
import os
from dataclasses import dataclass
from typing import Callable

from botocore.exceptions import ClientError

MiB = 1024 * 1024


@dataclass(frozen=True)
class Check:
    name: str
    used_by: str          # who depends on it
    required: bool        # required for Iceberg S3FileIO
    fn: Callable


def _put_get(s3, b, p):
    body = os.urandom(4096)
    s3.put_object(Bucket=b, Key=f"{p}/obj", Body=body)
    return s3.get_object(Bucket=b, Key=f"{p}/obj")["Body"].read() == body


def _suffix_range(s3, b, p):
    s3.put_object(Bucket=b, Key=f"{p}/f.parquet", Body=b"PAR1" + b"x" * 100 + b"\x10\x00\x00\x00PAR1")
    return s3.get_object(Bucket=b, Key=f"{p}/f.parquet", Range="bytes=-8")["Body"].read() == b"\x10\x00\x00\x00PAR1"


def _offset_range(s3, b, p):
    s3.put_object(Bucket=b, Key=f"{p}/r", Body=bytes(range(256)))
    r = s3.get_object(Bucket=b, Key=f"{p}/r", Range="bytes=10-19")
    return r["Body"].read() == bytes(range(10, 20)) and r["ResponseMetadata"]["HTTPStatusCode"] == 206


def _head_size(s3, b, p):
    s3.put_object(Bucket=b, Key=f"{p}/h", Body=b"y" * 1234)
    return s3.head_object(Bucket=b, Key=f"{p}/h")["ContentLength"] == 1234


def _multipart(s3, b, p):
    key = f"{p}/mpu"
    up = s3.create_multipart_upload(Bucket=b, Key=key)["UploadId"]
    parts = [s3.upload_part(Bucket=b, Key=key, UploadId=up, PartNumber=i, Body=body)["ETag"]
             for i, body in ((1, b"a" * 5 * MiB), (2, b"b" * 1024))]
    s3.complete_multipart_upload(Bucket=b, Key=key, UploadId=up, MultipartUpload={
        "Parts": [{"PartNumber": i + 1, "ETag": e} for i, e in enumerate(parts)]})
    return s3.head_object(Bucket=b, Key=key)["ContentLength"] == 5 * MiB + 1024


def _batch_delete(s3, b, p):
    keys = [f"{p}/del/{i}" for i in range(20)]
    for k in keys:
        s3.put_object(Bucket=b, Key=k, Body=b"d")
    r = s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in keys]})
    return len(r.get("Deleted", [])) == 20 and s3.list_objects_v2(Bucket=b, Prefix=f"{p}/del/")["KeyCount"] == 0


def _list_pagination(s3, b, p):
    for i in range(7):
        s3.put_object(Bucket=b, Key=f"{p}/page/{i}", Body=b"l")
    pages = list(s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{p}/page/",
                                                               PaginationConfig={"PageSize": 3}))
    return len(pages) == 3 and sum(pg["KeyCount"] for pg in pages) == 7


def _overwrite_visible(s3, b, p):
    s3.put_object(Bucket=b, Key=f"{p}/ow", Body=b"1")
    s3.put_object(Bucket=b, Key=f"{p}/ow", Body=b"2")
    return s3.get_object(Bucket=b, Key=f"{p}/ow")["Body"].read() == b"2"


def _if_none_match(s3, b, p):
    s3.put_object(Bucket=b, Key=f"{p}/cond", Body=b"first", IfNoneMatch="*")
    try:
        s3.put_object(Bucket=b, Key=f"{p}/cond", Body=b"second", IfNoneMatch="*")
    except ClientError as e:
        return e.response["Error"]["Code"] == "PreconditionFailed"
    return False


def _if_match(s3, b, p):
    etag = s3.put_object(Bucket=b, Key=f"{p}/cas", Body=b"v1")["ETag"]
    s3.put_object(Bucket=b, Key=f"{p}/cas", Body=b"v2", IfMatch=etag)      # compare-and-swap succeeds
    try:
        s3.put_object(Bucket=b, Key=f"{p}/cas", Body=b"v3", IfMatch=etag)  # stale ETag must fail
    except ClientError as e:
        return e.response["Error"]["Code"] == "PreconditionFailed"
    return False


def _no_directory_objects(s3, b, p):
    s3.put_object(Bucket=b, Key=f"{p}/dir/file", Body=b"f")
    try:
        s3.head_object(Bucket=b, Key=f"{p}/dir/")
    except ClientError as e:
        return e.response["Error"]["Code"] in ("404", "NoSuchKey")
    return False


def _empty_prefix_vanishes(s3, b, p):
    s3.put_object(Bucket=b, Key=f"{p}/gone/sub/file", Body=b"g")
    s3.delete_object(Bucket=b, Key=f"{p}/gone/sub/file")
    r = s3.list_objects_v2(Bucket=b, Prefix=f"{p}/gone/", Delimiter="/")
    return r.get("KeyCount", 0) == 0 and not r.get("CommonPrefixes")


CHECKS = [
    Check("put_get_roundtrip", "all data and metadata files", True, _put_get),
    Check("suffix_range_get", "Parquet footer read (bytes=-N)", True, _suffix_range),
    Check("offset_range_get", "column chunk reads (S3InputStream)", True, _offset_range),
    Check("head_object_size", "file length for planning and readers", True, _head_size),
    Check("multipart_upload", "S3FileIO writes > s3.multipart.size (32 MB)", True, _multipart),
    Check("batch_delete", "S3FileIO bulk delete (expire_snapshots, purge)", True, _batch_delete),
    Check("list_pagination", "remove_orphan_files, prefix listing", True, _list_pagination),
    Check("overwrite_visible", "strong read-after-write", True, _overwrite_visible),
    Check("if_none_match_put", "create-only writes (S3 conditional writes, 2024)", False, _if_none_match),
    Check("if_match_put", "compare-and-swap on an object (S3, 2024)", False, _if_match),
    Check("no_directory_objects", "flat namespace like AWS S3", False, _no_directory_objects),
    Check("empty_prefix_vanishes", "flat namespace like AWS S3", False, _empty_prefix_vanishes),
]


def run_checks(s3, bucket: str, prefix: str) -> dict[str, bool]:
    results = {}
    for c in CHECKS:
        try:
            results[c.name] = bool(c.fn(s3, bucket, f"{prefix}/{c.name}"))
        except ClientError:
            results[c.name] = False
    return results

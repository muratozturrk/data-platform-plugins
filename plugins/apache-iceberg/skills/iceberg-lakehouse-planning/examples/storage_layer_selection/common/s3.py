"""S3 clients for both object stores and a request counter based on SeaweedFS Prometheus metrics.

SeaweedFS exports `SeaweedFS_s3_request_total{bucket, code, type}`. Taking the difference of two
scrapes gives the exact S3 requests an operation caused on a bucket, including requests made by
Spark / Iceberg, which boto3-side counting cannot see. HEAD requests are counted as type GET.
"""
import os
import re
import urllib.request
from collections import Counter
from contextlib import contextmanager
from functools import lru_cache
from urllib.parse import urlparse

import boto3
from botocore.config import Config

STORES = {
    "seaweedfs": (os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"),
                  os.getenv("AWS_ACCESS_KEY_ID", "admin"), os.getenv("AWS_SECRET_ACCESS_KEY", "password")),  # local dev default
    "rustfs": (os.getenv("RUSTFS_ENDPOINT", "http://rustfs:9000"),
               os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"), os.getenv("RUSTFS_SECRET_KEY", "rustfs-secret")),  # local dev default
}
METRICS_URL = os.getenv("S3_METRICS_URL", "http://seaweedfs:9324/metrics")
_LINE = re.compile(r'^SeaweedFS_s3_request_total\{bucket="([^"]*)",code="(\d+)",type="(\w+)"\} (\S+)$')


@lru_cache(maxsize=None)
def client(store: str = "seaweedfs", access_key: str | None = None, secret_key: str | None = None,
           max_pool: int = 64):
    endpoint, ak, sk = STORES[store]
    return boto3.client(
        "s3", endpoint_url=endpoint, region_name="us-east-1",
        aws_access_key_id=access_key or ak, aws_secret_access_key=secret_key or sk,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1},
                      max_pool_connections=max_pool))


@lru_cache(maxsize=None)
def arrow_fs(store: str = "seaweedfs"):
    """pyarrow.fs.S3FileSystem for a store (Arrow's C++ S3 client)."""
    from pyarrow import fs
    endpoint, ak, sk = STORES[store]
    u = urlparse(endpoint)
    return fs.S3FileSystem(access_key=ak, secret_key=sk, endpoint_override=u.netloc,
                           scheme=u.scheme, region="us-east-1")


def ensure_bucket(store: str, bucket: str, **kwargs) -> None:
    s3 = client(store)
    if bucket not in {b["Name"] for b in s3.list_buckets().get("Buckets", [])}:
        s3.create_bucket(Bucket=bucket, **kwargs)


def empty_bucket(store: str, bucket: str, prefix: str = "") -> None:
    s3 = client(store)
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def split_uri(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def list_keys(store: str, bucket: str, prefix: str = "") -> list[str]:
    s3 = client(store)
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def scrape(bucket: str) -> Counter:
    """Current SeaweedFS request counters for one bucket, keyed by (type, status code)."""
    counts: Counter = Counter()
    for line in urllib.request.urlopen(METRICS_URL, timeout=10).read().decode().splitlines():
        m = _LINE.match(line)
        if m and m.group(1) == bucket:
            counts[(m.group(3), m.group(2))] += int(float(m.group(4)))
    return counts


class RequestDelta(Counter):
    """(type, code) -> count, plus helpers: by_type('GET'), ranged_gets, total."""

    def by_type(self, t: str) -> int:
        return sum(v for (typ, _), v in self.items() if typ == t)

    @property
    def ranged_gets(self) -> int:
        return self[("GET", "206")]

    @property
    def total(self) -> int:
        return sum(self.values())


@contextmanager
def count_requests(bucket: str):
    """with count_requests('warehouse') as d: ...  -> d holds the requests made inside the block."""
    delta = RequestDelta()
    before = scrape(bucket)
    yield delta
    delta.update(scrape(bucket) - before)

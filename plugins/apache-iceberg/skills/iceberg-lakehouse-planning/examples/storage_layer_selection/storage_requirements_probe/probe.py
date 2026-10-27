"""Measure a storage endpoint against four requirement categories.

Rule: "look beyond advertised throughput. Benchmark typical access patterns, measure tail
latency, and stress-test concurrency." This probe does that over the S3 API, plus integrity
(checksum validation, read-after-write visibility) and security (anonymous access, encryption
at rest) checks. Numbers from the lab stack are local measurements, not benchmarks.
Modeled on the request mix of S3 benchmark tools such as https://github.com/minio/warp.
"""
import base64
import hashlib
import os
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from botocore.exceptions import ClientError


@dataclass(frozen=True)
class LatencyStats:
    n: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float

    @classmethod
    def of(cls, samples_s: list[float]) -> "LatencyStats":
        ms = sorted(s * 1000 for s in samples_s)
        q = statistics.quantiles(ms, n=100, method="inclusive")
        return cls(len(ms), round(statistics.fmean(ms), 2), round(q[49], 2), round(q[94], 2),
                   round(q[98], 2), round(ms[-1], 2))


@dataclass
class ProbeReport:
    store: str
    small_get: LatencyStats | None = None
    requests_per_s: dict[int, float] = field(default_factory=dict)   # concurrency -> req/s
    p99_ms: dict[int, float] = field(default_factory=dict)           # concurrency -> p99 latency
    rejects_bad_checksum: bool | None = None
    etag_is_md5: bool | None = None
    read_after_write: bool | None = None
    list_after_write: bool | None = None
    anonymous_denied: bool | None = None
    sse_s3: bool | None = None

    @property
    def concurrency_speedup(self) -> float:
        levels = sorted(self.requests_per_s)
        return round(self.requests_per_s[levels[-1]] / self.requests_per_s[levels[0]], 2)

    @property
    def tail_inflation(self) -> float:
        """p99 at the highest concurrency divided by p99 of sequential requests."""
        levels = sorted(self.p99_ms)
        return round(self.p99_ms[levels[-1]] / self.p99_ms[levels[0]], 2)


def seed_objects(s3, bucket: str, prefix: str, n: int, size: int) -> list[str]:
    body = os.urandom(size)
    keys = [f"{prefix}/obj-{i:05d}" for i in range(n)]
    with ThreadPoolExecutor(16) as pool:
        list(pool.map(lambda k: s3.put_object(Bucket=bucket, Key=k, Body=body), keys))
    return keys


def _timed_get(s3, bucket: str, key: str) -> float:
    t = time.perf_counter()
    s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return time.perf_counter() - t


def get_latency(s3, bucket: str, keys: list[str], concurrency: int = 1) -> tuple[LatencyStats, float]:
    """Per-request latency distribution and aggregate requests/second at a concurrency level."""
    t = time.perf_counter()
    with ThreadPoolExecutor(concurrency) as pool:
        samples = list(pool.map(lambda k: _timed_get(s3, bucket, k), keys))
    return LatencyStats.of(samples), len(keys) / (time.perf_counter() - t)


def checksum_is_validated(s3, bucket: str, key: str) -> bool:
    """Upload with a wrong Content-MD5: an integrity-checking store must reject it (BadDigest)."""
    wrong = base64.b64encode(hashlib.md5(b"something else").digest()).decode()
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"payload", ContentMD5=wrong)
    except ClientError as e:
        return e.response["Error"]["Code"] == "BadDigest"
    return False


def etag_matches_md5(s3, bucket: str, key: str) -> bool:
    body = os.urandom(1024)
    etag = s3.put_object(Bucket=bucket, Key=key, Body=body)["ETag"].strip('"')
    return etag == hashlib.md5(body).hexdigest()


def visibility(s3, bucket: str, prefix: str, rounds: int = 25) -> tuple[bool, bool]:
    """Read-after-write and list-after-write: is a new or overwritten object visible at once?"""
    raw = lst = True
    for i in range(rounds):
        key = f"{prefix}/v-{i}"
        s3.put_object(Bucket=bucket, Key=key, Body=b"v1")
        s3.put_object(Bucket=bucket, Key=key, Body=b"v2")              # overwrite
        raw &= s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"v2"
        listed = s3.list_objects_v2(Bucket=bucket, Prefix=key).get("Contents", [])
        lst &= [o["Key"] for o in listed] == [key]
    return raw, lst


def anonymous_read_denied(s3, bucket: str, key: str) -> bool:
    s3.put_object(Bucket=bucket, Key=key, Body=b"private")
    try:
        urllib.request.urlopen(f"{s3.meta.endpoint_url}/{bucket}/{key}", timeout=5)
    except urllib.error.HTTPError as e:
        return e.code == 403
    return False


def sse_s3_supported(s3, bucket: str, key: str) -> bool:
    """Server-side encryption with store-managed keys (SSE-S3, `x-amz-server-side-encryption`)."""
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"secret", ServerSideEncryption="AES256")
        return s3.head_object(Bucket=bucket, Key=key).get("ServerSideEncryption") == "AES256"
    except ClientError:
        return False


def run_probe(store: str, s3, bucket: str, prefix: str, n_objects: int = 200,
              object_size: int = 4096, concurrency_levels=(1, 16)) -> ProbeReport:
    r = ProbeReport(store)
    keys = seed_objects(s3, bucket, f"{prefix}/small", n_objects, object_size)
    get_latency(s3, bucket, keys[:20])                                   # warm-up
    for c in concurrency_levels:
        stats, rps = get_latency(s3, bucket, keys, c)
        r.requests_per_s[c], r.p99_ms[c] = round(rps, 1), stats.p99_ms
        if c == 1:
            r.small_get = stats
    r.rejects_bad_checksum = checksum_is_validated(s3, bucket, f"{prefix}/md5")
    r.etag_is_md5 = etag_matches_md5(s3, bucket, f"{prefix}/etag")
    r.read_after_write, r.list_after_write = visibility(s3, bucket, f"{prefix}/vis")
    r.anonymous_denied = anonymous_read_denied(s3, bucket, f"{prefix}/private")
    r.sse_s3 = sse_s3_supported(s3, bucket, f"{prefix}/sse")
    return r

"""Filesystem operations that behave differently on object storage (block vs object storage).

A filesystem on block storage has directories and an atomic rename. An object store has a flat
key space: a "directory" is only a key prefix, listing is paginated (max 1,000 keys per
ListObjectsV2 call), and "rename" is CopyObject + DeleteObject for every key. This is why
Iceberg never renames files and commits by swapping a pointer in the catalog instead
(https://iceberg.apache.org/docs/1.8.1/aws/#s3-fileio).
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass


def put_many(s3, bucket: str, keys: list[str], body: bytes, threads: int = 16) -> None:
    with ThreadPoolExecutor(threads) as pool:
        list(pool.map(lambda k: s3.put_object(Bucket=bucket, Key=k, Body=body), keys))


def rename_prefix(s3, bucket: str, src: str, dst: str, fail_after: int | None = None) -> int:
    """What s3a and other filesystem shims do for rename(dir): copy every object, then delete it.

    `fail_after` simulates a crash of the client in the middle of the operation.
    """
    moved = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=src):
        for obj in page.get("Contents", []):
            if fail_after is not None and moved == fail_after:
                raise RuntimeError(f"client crashed after {moved} objects")
            new_key = dst + obj["Key"][len(src):]
            s3.copy_object(Bucket=bucket, Key=new_key, CopySource={"Bucket": bucket, "Key": obj["Key"]})
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
            moved += 1
    return moved


def make_local_files(directory: str, n: int, body: bytes) -> None:
    os.makedirs(directory, exist_ok=True)
    for i in range(n):
        with open(os.path.join(directory, f"part-{i:05d}"), "wb") as f:
            f.write(body)


@dataclass(frozen=True)
class Listing:
    keys: int
    common_prefixes: list[str]
    pages: int


def list_prefix(s3, bucket: str, prefix: str, delimiter: str | None = None) -> Listing:
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    if delimiter:
        kwargs["Delimiter"] = delimiter
    keys, prefixes, pages = 0, [], 0
    for page in s3.get_paginator("list_objects_v2").paginate(**kwargs):
        pages += 1
        keys += len(page.get("Contents", []))
        prefixes += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
    return Listing(keys, prefixes, pages)


@dataclass(frozen=True)
class ReadCost:
    objects: int
    bytes: int
    sequential_s: float
    parallel_s: float


def read_all(s3fs, bucket: str, keys: list[str], threads: int = 16) -> ReadCost:
    """Read every object once sequentially and once with `threads` concurrent GETs.

    Uses Arrow's C++ S3 client (pyarrow.fs.S3FileSystem, the I/O layer of PyIceberg), which
    releases the GIL, so the threads really overlap like an engine's I/O threads do.
    """
    get = lambda k: len(s3fs.open_input_file(f"{bucket}/{k}").read())  # noqa: E731
    t = time.perf_counter()
    total = sum(get(k) for k in keys)
    seq = time.perf_counter() - t
    t = time.perf_counter()
    with ThreadPoolExecutor(threads) as pool:
        list(pool.map(get, keys))
    return ReadCost(len(keys), total, round(seq, 3), round(time.perf_counter() - t, 3))

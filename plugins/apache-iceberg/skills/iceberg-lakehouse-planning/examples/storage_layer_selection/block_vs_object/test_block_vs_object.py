import os
import shutil

import pytest

from common.s3 import arrow_fs, client, count_requests, empty_bucket, ensure_bucket

from .iceberg_compare import build_table
from .object_semantics import (list_prefix, make_local_files, put_many, read_all,
                               rename_prefix)

BUCKET = "objsem"
BLOCK_DIR = "/data/block/objsem"
MiB = 1024 * 1024


@pytest.fixture(scope="module")
def s3():
    for store in ("seaweedfs", "rustfs"):
        ensure_bucket(store, BUCKET)
    yield client("seaweedfs")
    for store in ("seaweedfs", "rustfs"):
        empty_bucket(store, BUCKET)
    shutil.rmtree(BLOCK_DIR, ignore_errors=True)


def test_object_rename_is_one_copy_and_one_delete_per_object(s3, uid, measure):
    put_many(s3, BUCKET, [f"{uid}/tmp/part-{i:05d}" for i in range(50)], os.urandom(64 * 1024))
    with count_requests(BUCKET) as d:
        moved = rename_prefix(s3, BUCKET, f"{uid}/tmp/", f"{uid}/final/")
    measure("object_rename_requests", dict(d))
    assert moved == 50
    assert d.by_type("COPY") == 50 and d.by_type("DELETE") == 50

    src, dst = f"{BLOCK_DIR}/{uid}/tmp", f"{BLOCK_DIR}/{uid}/final"
    make_local_files(src, 50, os.urandom(64 * 1024))
    inode = os.stat(src).st_ino
    os.rename(src, dst)                     # one metadata operation, whatever the file count
    assert len(os.listdir(dst)) == 50 and not os.path.exists(src)
    assert os.stat(dst).st_ino == inode      # same directory, only its name changed


def test_interrupted_object_rename_leaves_a_partial_state(s3, uid, measure):
    put_many(s3, BUCKET, [f"{uid}/staging/part-{i:05d}" for i in range(50)], b"x" * 1024)
    with pytest.raises(RuntimeError):
        rename_prefix(s3, BUCKET, f"{uid}/staging/", f"{uid}/published/", fail_after=20)
    left = list_prefix(s3, BUCKET, f"{uid}/staging/").keys
    arrived = list_prefix(s3, BUCKET, f"{uid}/published/").keys
    measure("after_crash", f"staging={left} published={arrived}")
    assert (left, arrived) == (30, 20)       # readers of either prefix see an incomplete dataset


@pytest.mark.parametrize("store", ["seaweedfs", "rustfs"])
def test_listing_is_by_prefix_and_paginated(s3, store, uid, measure):
    s3 = client(store)
    keys = [f"{uid}/events/day={d}/part-{i:04d}.parquet" for d in range(3) for i in range(335)]
    put_many(s3, BUCKET, keys, b"p")
    with count_requests(BUCKET) as d:
        flat = list_prefix(s3, BUCKET, f"{uid}/events/")
    dirs = list_prefix(s3, BUCKET, f"{uid}/events/", delimiter="/")
    measure(f"{store}_listing", f"{flat.keys} keys -> {flat.pages} pages; delimiter view: "
                                f"{len(dirs.common_prefixes)} common prefixes, {dirs.keys} keys")
    assert flat.keys == 1005 and flat.pages == 2           # max 1,000 keys per ListObjectsV2 page
    if store == "seaweedfs":
        assert d.by_type("LIST") == 2                      # one request per page
    assert dirs.keys == 0 and len(dirs.common_prefixes) == 3   # "directories" are synthesized


def test_many_small_objects_cost_more_requests_and_time_than_few_large(s3, uid, measure):
    total = 16 * MiB
    big = [f"{uid}/big/f-0"]
    small = [f"{uid}/small/f-{i:04d}" for i in range(256)]
    put_many(s3, BUCKET, big, os.urandom(total))
    put_many(s3, BUCKET, small, os.urandom(total // 256))
    afs = arrow_fs("seaweedfs")
    read_all(afs, BUCKET, big + small[:16])                   # warm-up
    with count_requests(BUCKET) as d_big:
        big_cost = read_all(afs, BUCKET, big)
    with count_requests(BUCKET) as d_small:
        small_cost = read_all(afs, BUCKET, small)
    measure("1x16MiB", f"{big_cost} requests={dict(d_big)}")
    measure("256x64KiB", f"{small_cost} requests={dict(d_small)}")
    assert big_cost.bytes == small_cost.bytes == total
    # Arrow issues a HEAD (counted as GET by SeaweedFS) before each GET: 2 requests per object per pass.
    assert d_big.by_type("GET") == 2 * 2 and d_small.by_type("GET") == 2 * 2 * 256
    assert small_cost.sequential_s > 3 * big_cost.sequential_s           # per-request cost dominates
    assert small_cost.parallel_s < small_cost.sequential_s / 2           # parallel GETs hide most of it


def test_same_iceberg_table_on_block_and_object_storage(spark, uid, measure):
    ns = f"cmp_{uid}"
    block = build_table(spark, "local", ns, "orders")
    obj = build_table(spark, "lake", ns, "orders")
    measure("block_local_disk", f"write={block.write_s}s scan={block.scan_s}s metadata={block.metadata_files}")
    measure("object_s3fileio", f"write={obj.write_s}s scan={obj.scan_s}s metadata={obj.metadata_files}")
    assert (block.rows, block.amount) == (obj.rows, obj.amount) == (60_000, block.amount)

    # Hadoop catalog on a filesystem: sequential versions + a hint file, published by rename.
    assert [f for f in block.metadata_files if f.endswith(".metadata.json")] == \
           [f"v{i}.metadata.json" for i in range(1, 5)]
    assert "version-hint.text" in block.metadata_files
    # REST catalog on object storage: unique file names, never renamed, no hint file.
    json_files = [f for f in obj.metadata_files if f.endswith(".metadata.json")]
    assert len(json_files) == 4 and all(f[:5].isdigit() and len(f) > 40 for f in json_files)
    assert "version-hint.text" not in obj.metadata_files
    for cat in ("local", "lake"):
        spark.sql(f"DROP TABLE {cat}.{ns}.orders PURGE")
        spark.sql(f"DROP NAMESPACE {cat}.{ns}")


def test_hadoop_catalog_on_object_storage_commits_by_copy_and_delete(spark, uid, measure):
    from common.lakehouse import events_df
    ns = f"commit_{uid}"
    for cat in ("s3a", "lake"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{ns}")
        spark.sql(f"CREATE TABLE {cat}.{ns}.t (order_id BIGINT, amount DOUBLE) USING iceberg")
    df = events_df(spark, 1000).select("order_id", "amount")
    with count_requests("warehouse") as rename_commit:
        df.writeTo(f"s3a.{ns}.t").append()
    with count_requests("warehouse") as pointer_commit:
        df.writeTo(f"lake.{ns}.t").append()
    measure("hadoop_catalog_s3a_append", dict(rename_commit))
    measure("rest_catalog_s3fileio_append", dict(pointer_commit))
    assert rename_commit.by_type("COPY") >= 1 and rename_commit.by_type("DELETE") >= 1
    assert rename_commit[("GET", "404")] > 0            # existence probes for files and "directories"
    assert pointer_commit.by_type("COPY") == 0 and pointer_commit.by_type("DELETE") == 0
    assert pointer_commit.by_type("LIST") == 0
    assert pointer_commit.total < rename_commit.total
    for cat in ("s3a", "lake"):
        spark.sql(f"DROP TABLE {cat}.{ns}.t PURGE")
    # Not dropped for s3a: SeaweedFS keeps the emptied table "directory" (see storage_standards), so the
    # Hadoop catalog still sees a non-empty namespace (NamespaceNotEmptyException).
    spark.sql(f"DROP NAMESPACE lake.{ns}")

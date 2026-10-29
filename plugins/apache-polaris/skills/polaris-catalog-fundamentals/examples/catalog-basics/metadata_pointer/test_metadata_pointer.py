"""Polaris as the metadata pointer: an Iceberg table's metadata files read from storage with ONLY the
credentials Polaris vends in its LoadTable response (no static S3 keys anywhere in the engine)."""
import datetime as dt
import os
import uuid

import numpy as np
import pyarrow as pa
import pytest
from pyiceberg.transforms import DayTransform

from metadata_pointer.anatomy import TableFiles, current_snapshot

COUNTRIES = ["US", "DE", "TR", "GB", "FR", "BR", "IN", "JP"]


def week_of_orders(start: dt.datetime, seed: int, rows: int = 250) -> pa.Table:
    """Orders spread over the 7 days starting at `start` (one daily partition each)."""
    rng = np.random.default_rng(seed)
    ts = np.datetime64(start) + np.sort(rng.integers(0, 7 * 86400, rows)).astype("timedelta64[s]")
    return pa.table({
        "order_id": pa.array(np.arange(1, rows + 1, dtype=np.int64) + seed * 1_000_000),
        "order_ts": pa.array(ts.astype("datetime64[us]")),
        "country": pa.array(rng.choice(COUNTRIES, rows)),
        "amount": pa.array(np.round(rng.gamma(2.0, 40.0, rows), 2)),
    })


@pytest.fixture(scope="module")
def lake(admin, root):
    cat = f"anatomy_{uuid.uuid4().hex[:6]}"
    admin.create_catalog(cat, f"s3://warehouse/{cat}")
    admin.grant(cat, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    engine = root.pyiceberg(cat)
    engine.create_namespace("sales")
    jan = week_of_orders(dt.datetime(2025, 1, 6), seed=1)
    table = engine.create_table("sales.orders", schema=jan.schema)         # commit 1
    with table.update_spec() as spec:                                       # commit 2
        spec.add_field("order_ts", DayTransform(), "order_day")
    table.append(jan)                                                       # commit 3
    table.append(week_of_orders(dt.datetime(2025, 2, 3), seed=2))          # commit 4
    yield {"catalog": cat, "engine": engine, "root": root}
    admin.drop_catalog(cat)


def files(lake) -> TableFiles:
    return TableFiles(lake["root"], lake["catalog"], "sales", "orders")


def test_each_commit_writes_a_new_metadata_file_and_polaris_points_to_the_last(lake, measure):
    """metadata.json files are immutable: every commit writes a new one and Polaris moves its pointer."""
    tf = files(lake)
    meta = tf.metadata()
    on_storage = sorted(k for k in tf.list_keys(meta["location"] + "/metadata/") if k.endswith(".metadata.json"))
    assert tf.metadata_location in on_storage
    assert len(on_storage) == len(meta["metadata-log"]) + 1
    assert len(on_storage) == 4            # create, add partition spec, append, append
    measure("metadata_json_files", {"on_storage": len(on_storage), "metadata_log": len(meta["metadata-log"])})


def test_manifest_list_lets_a_reader_skip_manifests_by_partition(lake, measure):
    """Planning from metadata reached through the pointer: the manifest list's partition summaries
    tell a reader which manifests it can skip, using the same table-scoped vended keys."""
    tf = files(lake)
    manifests = tf.manifest_list(current_snapshot(tf.metadata()))
    assert len(manifests) == 2                                   # one manifest per append
    assert {m["added_files_count"] for m in manifests} == {7}    # 7 daily partitions per week
    ranges = []
    for m in manifests:
        summary = m["partitions"][0]                             # day ordinals since 1970-01-01
        lo, hi = (int.from_bytes(summary[k], "little", signed=True) for k in ("lower_bound", "upper_bound"))
        ranges.append((dt.date(1970, 1, 1) + dt.timedelta(days=lo), dt.date(1970, 1, 1) + dt.timedelta(days=hi)))
    jan8 = dt.date(2025, 1, 8)
    assert len([r for r in ranges if r[0] <= jan8 <= r[1]]) == 1   # the February manifest is skipped unread
    measure("manifest_partition_ranges", [f"{a}..{b}" for a, b in sorted(ranges)])


def test_engine_holds_no_storage_keys(lake):
    """Everything above ran through Polaris-vended, table-scoped temporary credentials."""
    assert "AWS_ACCESS_KEY_ID" not in os.environ and "AWS_SECRET_ACCESS_KEY" not in os.environ
    io_props = lake["engine"].load_table("sales.orders").io.properties
    assert io_props.get("s3.access-key-id", "").startswith("ASIA")   # temporary STS key
    assert files(lake).s3 is not None                                # LoadTable carried storage credentials

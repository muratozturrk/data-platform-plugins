"""The same workload at format versions 1, 2 and 3, measured on the files Spark wrote."""
import pytest
from pyspark.errors import AnalysisException
from py4j.protocol import Py4JJavaError

from common.lakehouse import metadata_location, read_metadata
from common.workload import create_orders, insert_orders, run_workload
from format_versions.versions import position_delete_file, profile

POS_FILE_PATH, POS_POS = 2147483546, 2147483545       # reserved field ids of position delete columns


@pytest.fixture(scope="module")
def profiles(spark, ns):
    out = {}
    for v in (1, 2, 3):
        run_workload(spark, f"{ns}.orders_v{v}", v)
        out[v] = profile(metadata_location(ns, f"orders_v{v}"))
    return out


def test_metadata_json_fields_by_version(profiles, measure):
    v1, v2, v3 = profiles[1], profiles[2], profiles[3]
    measure("v1_only_keys", sorted(v1.metadata_keys - v2.metadata_keys))
    measure("v2_added_keys", sorted(v2.metadata_keys - v1.metadata_keys))
    measure("v3_added_keys", sorted(v3.metadata_keys - v2.metadata_keys))
    assert {"schema", "partition-spec"} <= v1.metadata_keys - v2.metadata_keys    # v1 single-value fields
    assert v2.metadata_keys - v1.metadata_keys == {"last-sequence-number"}
    assert v3.metadata_keys == v2.metadata_keys                                     # 1.8.1: no next-row-id
    assert "sequence-number" not in v1.snapshot_keys and "sequence-number" in v2.snapshot_keys
    assert "first-row-id" not in v3.snapshot_keys


def test_sequence_numbers_exist_from_v2(profiles, measure):
    measure("v1", profiles[1].snapshot_sequence_numbers)
    measure("v2", profiles[2].snapshot_sequence_numbers)
    assert set(profiles[1].snapshot_sequence_numbers) == {None}
    for v in (2, 3):
        seqs = profiles[v].snapshot_sequence_numbers
        assert seqs == list(range(1, len(seqs) + 1))


def test_manifest_schemas_grow_with_the_version(profiles, measure):
    v1, v2, v3 = profiles[1], profiles[2], profiles[3]
    diffs = {
        "manifest_list v1->v2": (v2.manifest_list_ids - v1.manifest_list_ids, v1.manifest_list_ids - v2.manifest_list_ids),
        "manifest_entry v1->v2": (v2.manifest_entry_ids - v1.manifest_entry_ids, set()),
        "data_file v1->v2": (v2.data_file_ids - v1.data_file_ids, v1.data_file_ids - v2.data_file_ids),
        "data_file v2->v3": (v3.data_file_ids - v2.data_file_ids, v2.data_file_ids - v3.data_file_ids),
        "manifest_list v2->v3": (v3.manifest_list_ids - v2.manifest_list_ids, set()),
    }
    for k, (added, removed) in diffs.items():
        measure(k, {"added": sorted(added), "removed": sorted(removed)})
    assert diffs["manifest_list v1->v2"] == ({515, 516, 517}, set())    # sequence_number, min_seq, content
    assert diffs["manifest_entry v1->v2"][0] == {3, 4}                   # sequence_number, file_sequence_number
    assert diffs["data_file v1->v2"] == ({134, 135, 143}, {105})        # content, equality_ids, referenced_data_file
    assert diffs["data_file v2->v3"] == ({144, 145}, set())             # content_offset, content_size_in_bytes
    assert diffs["manifest_list v2->v3"][0] == set()                     # 1.8.1: no first_row_id (520)


def test_row_level_deletes_v1_blocked_v2_parquet_v3_deletion_vectors(spark, ns, profiles, measure):
    # v1: no delete files at all. Merge-on-read is refused; copy-on-write rewrites data files.
    create_orders(spark, f"{ns}.v1_mor", 1)
    insert_orders(spark, f"{ns}.v1_mor", 0, 100)
    with pytest.raises((Py4JJavaError, AnalysisException, Exception)) as err:
        spark.sql(f"DELETE FROM {ns}.v1_mor WHERE order_id % 50 = 7")
    measure("v1_merge_on_read_error", str(err.value).splitlines()[0][:80])
    assert "Deletes are supported in V2 and above" in str(err.value)
    spark.sql(f"ALTER TABLE {ns}.v1_mor SET TBLPROPERTIES ('write.delete.mode'='copy-on-write')")
    spark.sql(f"DELETE FROM {ns}.v1_mor WHERE order_id % 50 = 7")
    assert spark.table(f"{ns}.v1_mor").count() == 98
    assert profile(metadata_location(ns, "v1_mor")).delete_files == []

    # v2: position delete files in Parquet, reserved column ids, possibly several data files per file.
    v2 = profiles[2].delete_files
    pos = [position_delete_file(d["file_path"]) for d in v2]
    measure("v2_delete_files", [(d["file_format"], d["record_count"]) for d in v2])
    assert v2 and {d["file_format"] for d in v2} == {"PARQUET"} and {d["content"] for d in v2} == {1}
    assert all(p["field_ids"] == {"file_path": POS_FILE_PATH, "pos": POS_POS} for p in pos)

    # v3: deletion vectors: Puffin blobs, exactly one per referenced data file, located by offset + size.
    v3 = profiles[3].delete_files
    measure("v3_delete_files", [(d["file_format"], d["record_count"], d["content_offset"], d["content_size_in_bytes"]) for d in v3])
    measure("physical_files", {"v2_parquet": len({d["file_path"] for d in v2}),
                               "v3_puffin": len({d["file_path"] for d in v3}), "v3_dvs": len(v3)})
    assert v3 and {d["file_format"] for d in v3} == {"PUFFIN"}
    refs = [d["referenced_data_file"] for d in v3]
    assert len(refs) == len(set(refs))
    assert all(d["content_offset"] is not None and d["content_size_in_bytes"] > 0 for d in v3)
    assert sum(d["record_count"] for d in v3) == sum(d["record_count"] for d in v2)  # same rows deleted


def test_v3_defaults_and_new_types_in_iceberg_181(spark, ns, measure):
    """The v3 spec adds initial-default / write-default and timestamp_ns; check what Spark 3.5 + 1.8.1 writes."""
    t = f"{ns}.v3_features"
    create_orders(spark, t, 3)
    insert_orders(spark, t, 0, 10)
    spark.sql(f"ALTER TABLE {t} ADD COLUMN priority INT DEFAULT 5")
    md = read_metadata(metadata_location(ns, "v3_features"))
    col = next(f for f in md["schemas"][-1]["fields"] if f["name"] == "priority")
    measure("priority_field", col)
    assert "initial-default" not in col and "write-default" not in col           # DEFAULT silently dropped
    assert spark.sql(f"SELECT count(*) c FROM {t} WHERE priority IS NULL").first().c == 10
    with pytest.raises(Exception, match="Cannot find data for the output column"):
        spark.sql(f"INSERT INTO {t} (order_id) VALUES (1)")

    spark.sql(f"CREATE TABLE {ns}.v3_types (ts_ntz TIMESTAMP_NTZ, ts TIMESTAMP) USING iceberg "
              "TBLPROPERTIES ('format-version'='3')")
    types = {f["name"]: f["type"] for f in read_metadata(metadata_location(ns, "v3_types"))["schemas"][-1]["fields"]}
    measure("spark_type_mapping", types)
    assert types == {"ts_ntz": "timestamp", "ts": "timestamptz"}                  # micros; no timestamp_ns from Spark 3.5

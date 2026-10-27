"""Parse real Puffin files: table statistics (theta sketches) and v3 deletion vectors."""
import io

import pyarrow.parquet as pq
import pytest

from common.lakehouse import metadata_location, read_avro, read_metadata, read_object
from common.workload import create_orders, insert_orders
from puffin.puffin import (PuffinError, decode_deletion_vector, read_blob, read_footer,
                                   theta_estimate)


@pytest.fixture(scope="module")
def stats_table(spark, ns):
    t = f"{ns}.orders_stats"
    create_orders(spark, t, 2)
    insert_orders(spark, t, 0, 2000)
    path = spark.sql(f"CALL lake.system.compute_table_stats(table => '{t}')").first().statistics_file
    return t, path, read_metadata(metadata_location(ns, "orders_stats"))


@pytest.fixture(scope="module")
def dv_table(spark, ns):
    """v3 table, one data file of 1,000 rows, then a merge-on-read DELETE of every 7th order."""
    t = f"{ns}.orders_dv"
    spark.sql(f"CREATE TABLE {t} (order_id BIGINT, note STRING) USING iceberg "
              "TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {t} SELECT /*+ COALESCE(1) */ id, 'x' FROM range(1000)")
    spark.sql(f"DELETE FROM {t} WHERE order_id % 7 = 0")
    md = read_metadata(metadata_location(ns, "orders_dv"))
    snap = next(s for s in md["snapshots"] if s["snapshot-id"] == md["current-snapshot-id"])
    dvs = [e["data_file"] for m in read_avro(snap["manifest-list"])[2] if m["content"] == 1
           for e in read_avro(m["manifest_path"])[2]]
    return t, dvs


def test_statistics_file_footer_matches_table_metadata(stats_table, measure):
    _, path, md = stats_table
    entry = next(s for s in md["statistics"] if s["statistics-path"] == path)
    data = read_object(path)
    puffin = read_footer(data)
    measure("file_bytes", puffin.size)
    measure("footer_bytes", puffin.footer_size)
    measure("blobs", [(b.type, b.fields, b.compression_codec, b.length) for b in puffin.blobs])
    measure("created_by", puffin.properties.get("created-by", "")[:40])
    assert (puffin.size, puffin.footer_size) == (entry["file-size-in-bytes"], entry["file-footer-size-in-bytes"])
    assert [(b.type, b.fields, b.snapshot_id, b.sequence_number, b.properties) for b in puffin.blobs] == \
           [(b["type"], b["fields"], b["snapshot-id"], b["sequence-number"], b["properties"]) for b in entry["blob-metadata"]]
    assert {b.type for b in puffin.blobs} == {"apache-datasketches-theta-v1"}
    assert {b.compression_codec for b in puffin.blobs} == {"zstd"}
    assert len(puffin.blobs) == 7 and entry["snapshot-id"] == md["current-snapshot-id"]


def test_theta_sketches_estimate_the_real_distinct_counts(spark, stats_table, measure):
    t, path, md = stats_table
    data = read_object(path)
    names = {f["id"]: f["name"] for f in md["schemas"][-1]["fields"]}
    rows = {}
    for blob in read_footer(data).blobs:
        col = names[blob.fields[0]]
        exact = spark.sql(f"SELECT count(DISTINCT {col}) c FROM {t}").first().c
        rows[col] = (round(theta_estimate(read_blob(data, blob))), int(blob.properties["ndv"]), exact)
    measure("estimate_property_exact", rows)
    for col, (estimate, ndv, exact) in rows.items():
        assert estimate == ndv, col                                         # our decoder == Iceberg's
        assert abs(estimate - exact) <= max(1, 0.05 * exact), col           # sketch error bound


def test_deletion_vector_positions_match_the_deleted_rows(dv_table, measure):
    _, dvs = dv_table
    assert len(dvs) == 1
    dv = dvs[0]
    data = read_object(dv["file_path"])
    blob_meta = read_footer(data).blobs[0]
    # The manifest locates the blob without reading the footer: content_offset / content_size_in_bytes.
    assert (dv["content_offset"], dv["content_size_in_bytes"]) == (blob_meta.offset, blob_meta.length)
    assert blob_meta.type == "deletion-vector-v1" and blob_meta.fields == [2147483645]   # _pos
    assert blob_meta.properties["referenced-data-file"] == dv["referenced_data_file"]
    assert (blob_meta.snapshot_id, blob_meta.sequence_number) == (-1, -1)               # inherited
    positions = decode_deletion_vector(read_blob(data, blob_meta))

    rows = pq.read_table(io.BytesIO(read_object(dv["referenced_data_file"])), columns=["order_id"])
    expected = [i for i, v in enumerate(rows.column("order_id").to_pylist()) if v % 7 == 0]
    measure("dv", {"blob_bytes": blob_meta.length, "cardinality": blob_meta.properties["cardinality"],
                   "first_positions": positions[:5], "data_file_rows": rows.num_rows})
    assert positions == expected
    assert int(blob_meta.properties["cardinality"]) == len(positions) == dv["record_count"] == 143


def test_reader_rejects_corrupted_puffin_files(dv_table):
    _, dvs = dv_table
    data = bytearray(read_object(dvs[0]["file_path"]))
    blob = read_footer(bytes(data)).blobs[0]
    broken = bytearray(read_blob(bytes(data), blob))
    broken[12] ^= 0xFF                                                       # flip a bitmap byte
    with pytest.raises(PuffinError, match="CRC-32"):
        decode_deletion_vector(bytes(broken))
    with pytest.raises(PuffinError, match="magic"):
        read_footer(bytes(data[:-1]) + b"X")

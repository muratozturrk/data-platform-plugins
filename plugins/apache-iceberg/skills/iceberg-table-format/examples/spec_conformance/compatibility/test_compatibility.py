"""In-place upgrades v1 -> v2 -> v3, downgrade refusal, reader gates, PyIceberg 0.9.1 matrix."""
import hashlib

import pytest

from common.lakehouse import list_objects, metadata_location, read_avro, read_metadata, read_object
from conformance.spec_checker import check_table
from snapshots.snapshots import entries
from compatibility.compat import (UnsupportedFormatVersion, open_metadata, pyiceberg_matrix,
                                          try_op, upgrade)


def current(md: dict) -> dict:
    return next(s for s in md["snapshots"] if s["snapshot-id"] == md["current-snapshot-id"])


def create(spark, table: str, version: int, rows: int = 100) -> None:
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg "
              f"TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read')")
    spark.sql(f"INSERT INTO {table} SELECT id FROM range({rows})")


def test_upgrade_v1_v2_v3_in_place_rewrites_no_files(spark, ns, measure):
    t = f"{ns}.ledger"
    create(spark, t, 1)
    spark.sql(f"INSERT INTO {t} SELECT id FROM range(100, 200)")
    v1_location = metadata_location(ns, "ledger")
    v1_bytes = read_object(v1_location)
    location = read_metadata(v1_location)["location"]
    before = set(list_objects(location))

    upgrade(spark, t, 2)
    v2_location = metadata_location(ns, "ledger")
    md = read_metadata(v2_location)
    added = set(list_objects(location)) - before
    measure("files_added_by_upgrade", sorted(u.rsplit("/", 1)[1][:24] for u in added))
    assert added == {v2_location}                                    # one new metadata.json, nothing else
    assert read_object(v1_location) == v1_bytes                      # old metadata untouched (time travel)
    assert read_metadata(v1_location)["format-version"] == 1 and md["format-version"] == 2
    assert md["last-sequence-number"] == 0 and all("sequence-number" not in s for s in md["snapshots"])
    _, kv, _ = read_avro(current(md)["manifest-list"])
    assert kv["format-version"] == "1"                               # old manifest list keeps v1 layout

    spark.sql(f"INSERT INTO {t} SELECT id FROM range(200, 300)")      # first v2 commit
    md = read_metadata(metadata_location(ns, "ledger"))
    seqs = sorted({(e.file_path.rsplit("/", 1)[1][:5], e.sequence_number) for e in entries(current(md))},
                  key=lambda x: x[1])
    measure("data_sequence_numbers_after_first_v2_commit", sorted({s for _, s in seqs}))
    assert current(md)["sequence-number"] == 1
    assert {s for _, s in seqs} == {0, 1}                            # v1-era files are read as sequence 0

    upgrade(spark, t, 3)
    spark.sql(f"DELETE FROM {t} WHERE id % 10 = 0")                  # deletion vectors on v1-era files
    md = read_metadata(metadata_location(ns, "ledger"))
    dvs = [e for e in entries(current(md)) if e.content == 1]
    measure("dvs_after_v3_delete", len(dvs))
    assert dvs and {e.file_path.rsplit(".", 1)[1] for e in dvs} == {"puffin"}
    assert spark.table(t).count() == 270
    rep = check_table(metadata_location(ns, "ledger"))
    measure("violations_after_upgrades", sorted(rep.rules_violated))
    # Mixed v1 / v2 / v3 files in one table conform; only the 1.8.1 row-lineage gaps remain.
    assert rep.rules_violated <= {"metadata.required.next-row-id", "snapshot.required.first-row-id",
                                  "snapshot.required.added-rows"}


def test_upgrade_is_explicit_and_forward_only(spark, ns, measure):
    t = f"{ns}.forward"
    create(spark, t, 1)
    err = try_op(lambda: spark.sql(f"DELETE FROM {t} WHERE id = 1"))
    measure("v1_row_level_delete", err)
    assert "Deletes are supported in V2 and above" in err
    assert read_metadata(metadata_location(ns, "forward"))["format-version"] == 1   # no automatic upgrade
    upgrade(spark, t, 3)
    err = try_op(lambda: upgrade(spark, t, 2))
    measure("downgrade", err)
    assert "Cannot downgrade v3 table to v2" in err


def test_reader_version_gate(spark, ns):
    t = f"{ns}.gated"
    create(spark, t, 1, rows=10)
    v1_location = metadata_location(ns, "gated")
    upgrade(spark, t, 2)
    v2_location = metadata_location(ns, "gated")
    v1_reader = {1}
    assert open_metadata(v1_location, v1_reader)["format-version"] == 1             # old state still readable
    with pytest.raises(UnsupportedFormatVersion):
        open_metadata(v2_location, v1_reader)
    assert open_metadata(v2_location, {1, 2, 3})["format-version"] == 2             # newer readers read older


def test_pyiceberg_091_compatibility_matrix(spark, ns, catalog, measure):
    for name, version in (("p_v1", 1), ("p_v2_deletes", 2), ("p_v3", 3), ("p_v3_dv", 3)):
        create(spark, f"{ns}.{name}", version)
    for name in ("p_v2_deletes", "p_v3_dv"):
        spark.sql(f"DELETE FROM {ns}.{name} WHERE id % 10 = 0")
    matrix = pyiceberg_matrix(catalog, {n: f"{ns}.{n}" for n in ("p_v1", "p_v2_deletes", "p_v3", "p_v3_dv")})
    for label, result in matrix.items():
        measure(label, result)
    assert matrix["p_v1"] == {"read": "ok", "append": "ok"}
    assert matrix["p_v2_deletes"] == {"read": "ok", "append": "ok"}
    assert matrix["p_v3"]["read"] == "ok"
    assert "Cannot write manifest for table version: 3" in matrix["p_v3"]["append"]
    assert "'PUFFIN' is not a valid FileFormat" in matrix["p_v3_dv"]["read"]
    # PyIceberg applied the v2 position deletes, and Spark reads the rows PyIceberg appended.
    assert catalog.load_table(f"{ns}.p_v2_deletes").scan().to_arrow().num_rows == 91
    assert spark.table(f"{ns}.p_v1").count() == 101

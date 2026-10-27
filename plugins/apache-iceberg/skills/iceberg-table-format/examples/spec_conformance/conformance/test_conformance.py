"""Validate files written by Spark 3.5.5 / Iceberg 1.8.1 against the table spec."""
import copy

import pytest

from common.lakehouse import metadata_location, read_avro, read_metadata
from common.workload import run_workload
from conformance.spec_checker import (MANIFEST_LIST, Report, avro_fields, check_avro_fields,
                                              check_bounds, check_ids, check_metadata_fields,
                                              check_snapshots, check_table, field_types_of)


@pytest.fixture(scope="module")
def tables(spark, ns):
    """The same workload at format versions 1, 2 and 3: version -> current metadata.json location."""
    out = {}
    for v in (1, 2, 3):
        run_workload(spark, f"{ns}.orders_v{v}", v)
        out[v] = metadata_location(ns, f"orders_v{v}")
    return out


@pytest.mark.parametrize("version", [1, 2])
def test_spark_tables_conform_to_the_spec(tables, version, measure):
    rep = check_table(tables[version])
    measure("checks_run", sum(rep.checked.values()))
    measure("bounds_checked", rep.checked["stats.bounds"])
    assert rep.ok, rep.violations
    assert rep.checked["stats.bounds"] > 0 and rep.checked["avro.fields"] > 0


def test_iceberg_181_v3_tables_miss_only_the_row_lineage_fields(tables, measure):
    """Iceberg 1.8.1 writes v3 tables before row lineage existed in its writer."""
    rep = check_table(tables[3])
    measure("violated_rules", sorted(rep.rules_violated))
    measure("checks_run", sum(rep.checked.values()))
    assert rep.rules_violated == {"metadata.required.next-row-id",
                                  "snapshot.required.first-row-id",
                                  "snapshot.required.added-rows"}


def test_bounds_follow_single_value_serialization_and_string_truncation(tables, measure):
    md = read_metadata(tables[2])
    snap = next(s for s in md["snapshots"] if s["snapshot-id"] == md["current-snapshot-id"])
    _, _, manifests = read_avro(snap["manifest-list"])
    _, _, entries = read_avro(next(m for m in manifests if m["content"] == 0)["manifest_path"])
    rep = Report("", 2)
    stats = check_bounds(entries[0]["data_file"], field_types_of(md), rep)
    assert rep.ok, rep.violations
    c = stats["customer"]
    measure("customer", {k: c[k] for k in ("lower", "upper", "min", "max")})
    measure("amount", stats["amount"])
    # Strings longer than 16 characters: lower bound = prefix, upper bound = prefix with last char + 1.
    assert len(c["min"]) > 16 and c["lower"] == c["min"][:16]
    assert len(c["upper"]) == 16 and c["upper"] > c["max"][:16] and c["upper"][:15] == c["max"][:15]
    # Exact bounds for numbers, dates and timestamps (micros since epoch, little-endian long).
    for col in ("order_id", "amount", "order_date", "order_ts"):
        assert (stats[col]["lower"], stats[col]["upper"]) == (stats[col]["min"], stats[col]["max"]), col


def test_checker_detects_broken_metadata(tables):
    """Negative tests on copies of a real v2 metadata.json: each break maps to one spec rule."""
    good = read_metadata(tables[2])
    breaks = {
        "metadata.required.last-sequence-number": lambda m: m.pop("last-sequence-number"),
        "metadata.required.table-uuid": lambda m: m.pop("table-uuid"),
        "seq.last-sequence-number": lambda m: m["snapshots"][-1].update({"sequence-number": 99}),
        "seq.unique": lambda m: m["snapshots"][-1].update({"sequence-number": m["snapshots"][0]["sequence-number"]}),
        "ids.current-schema-id": lambda m: m.update({"current-schema-id": 42}),
        "ids.partition-field-id": lambda m: m["partition-specs"][0]["fields"][0].update({"field-id": 7}),
        "ids.default-sort-order-id": lambda m: m.update({"default-sort-order-id": 5}),
        "snapshots.main-ref": lambda m: m["refs"]["main"].update({"snapshot-id": m["snapshots"][0]["snapshot-id"]}),
        "snapshots.summary.operation": lambda m: m["snapshots"][0]["summary"].update({"operation": "upsert"}),
    }
    rep = Report("", 2)
    for check in (check_metadata_fields, check_ids, check_snapshots):
        check(good, rep)
    assert rep.ok, rep.violations
    for rule, breaker in breaks.items():
        md = copy.deepcopy(good)
        breaker(md)
        rep = Report("", 2)
        for check in (check_metadata_fields, check_ids, check_snapshots):
            check(md, rep)
        assert rule in rep.rules_violated, (rule, rep.violations)


def test_checker_detects_wrong_avro_field_ids(tables):
    md = read_metadata(tables[2])
    schema, _, _ = read_avro(md["snapshots"][-1]["manifest-list"])
    fields = avro_fields(schema)
    rep = Report("", 2)
    check_avro_fields(fields, MANIFEST_LIST, 2, "manifest_list", rep)
    assert rep.ok
    fields["sequence_number"] = 999
    del fields["min_sequence_number"]
    fields["row_count"] = 600
    rep = Report("", 2)
    check_avro_fields(fields, MANIFEST_LIST, 2, "manifest_list", rep)
    assert rep.rules_violated == {"manifest_list.field-id.sequence_number",
                                  "manifest_list.required.min_sequence_number",
                                  "manifest_list.unknown-field.row_count"}

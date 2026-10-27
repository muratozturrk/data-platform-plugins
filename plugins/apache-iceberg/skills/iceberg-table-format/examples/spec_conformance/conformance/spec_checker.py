"""Iceberg table spec conformance checker for real files (metadata.json, manifest lists, manifests, data).

Reads the raw files only (no Iceberg library) and checks them against the rules of the Apache Iceberg
table spec (https://iceberg.apache.org/spec/): required fields per format version, Avro field ids,
id references (schema / spec / sort order), snapshot and sequence-number rules, manifest counts and
partition summaries, and the single-value serialization of column bounds.

Field tables: name -> (field id, requirement for v1 v2 v3) with r = required, o = optional, - = not in
that version. They are transcribed from the spec's "Manifests", "Manifest Lists", "Snapshots" and
"Table Metadata Fields" sections.
"""
import io
import math
import struct
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow.compute as pc
import pyarrow.parquet as pq

from common.lakehouse import read_avro, read_metadata, read_object

TABLE_METADATA = {
    "format-version": "rrr", "table-uuid": "orr", "location": "rrr", "last-sequence-number": "-rr",
    "last-updated-ms": "rrr", "last-column-id": "rrr", "schema": "r--", "schemas": "orr",
    "current-schema-id": "orr", "partition-spec": "r--", "partition-specs": "orr",
    "default-spec-id": "orr", "last-partition-id": "orr", "sort-orders": "orr",
    "default-sort-order-id": "orr", "next-row-id": "--r",
}
SNAPSHOT = {
    "snapshot-id": "rrr", "timestamp-ms": "rrr", "manifest-list": "orr",
    "summary": "orr", "first-row-id": "--r", "added-rows": "--r",
}
MANIFEST_LIST = {
    "manifest_path": (500, "rrr"), "manifest_length": (501, "rrr"), "partition_spec_id": (502, "rrr"),
    "content": (517, "-rr"), "sequence_number": (515, "-rr"), "min_sequence_number": (516, "-rr"),
    "added_snapshot_id": (503, "rrr"), "added_files_count": (504, "orr"),
    "existing_files_count": (505, "orr"), "deleted_files_count": (506, "orr"),
    "added_rows_count": (512, "orr"), "existing_rows_count": (513, "orr"),
    "deleted_rows_count": (514, "orr"), "partitions": (507, "ooo"), "key_metadata": (519, "ooo"),
    "first_row_id": (520, "--o"),
}
MANIFEST_ENTRY = {
    "status": (0, "rrr"), "snapshot_id": (1, "roo"), "sequence_number": (3, "-oo"),
    "file_sequence_number": (4, "-oo"), "data_file": (2, "rrr"),
}
DATA_FILE = {
    "content": (134, "-rr"), "file_path": (100, "rrr"), "file_format": (101, "rrr"),
    "partition": (102, "rrr"), "record_count": (103, "rrr"), "file_size_in_bytes": (104, "rrr"),
    "block_size_in_bytes": (105, "r--"), "column_sizes": (108, "ooo"), "value_counts": (109, "ooo"),
    "null_value_counts": (110, "ooo"), "nan_value_counts": (137, "ooo"), "distinct_counts": (111, "oo-"),
    "lower_bounds": (125, "ooo"), "upper_bounds": (128, "ooo"), "key_metadata": (131, "ooo"),
    "split_offsets": (132, "ooo"), "equality_ids": (135, "-oo"), "sort_order_id": (140, "ooo"),
    "first_row_id": (142, "--o"), "referenced_data_file": (143, "-oo"), "content_offset": (144, "--o"),
    "content_size_in_bytes": (145, "--o"),
}
OPERATIONS = {"append", "replace", "overwrite", "delete"}
DATA_FORMATS = {"PARQUET", "AVRO", "ORC"}
STATUS_EXISTING, STATUS_ADDED, STATUS_DELETED = 0, 1, 2


@dataclass
class Report:
    metadata_location: str
    format_version: int
    violations: list[tuple[str, str]] = field(default_factory=list)   # (rule id, detail)
    checked: Counter = field(default_factory=Counter)                  # rule family -> checks run

    def fail(self, rule: str, detail: str = "") -> None:
        self.violations.append((rule, detail))

    @property
    def rules_violated(self) -> set[str]:
        return {r for r, _ in self.violations}

    @property
    def ok(self) -> bool:
        return not self.violations


def required(spec: str, version: int) -> bool:
    return spec[version - 1] == "r"


# ---------------------------------------------------------------- table metadata (metadata.json)
def check_metadata_fields(md: dict, rep: Report) -> None:
    v = md["format-version"]
    if v not in (1, 2, 3):
        rep.fail("metadata.format-version", f"unsupported {v}")
        return
    for name, spec in TABLE_METADATA.items():
        rep.checked["metadata.required"] += 1
        if required(spec, v) and name not in md:
            rep.fail(f"metadata.required.{name}", f"v{v} requires {name}")
    for snap in md.get("snapshots", []):
        for name, spec in SNAPSHOT.items():
            rep.checked["snapshot.required"] += 1
            if required(spec, v) and name not in snap:
                rep.fail(f"snapshot.required.{name}", f"snapshot {snap['snapshot-id']}")


def _max_field_id(fields: list[dict]) -> int:
    """Highest field id in a schema, including nested struct / list / map ids."""
    best = 0
    stack = list(fields)
    while stack:
        f = stack.pop()
        best = max(best, f.get("id", 0), f.get("element-id", 0), f.get("key-id", 0), f.get("value-id", 0))
        t = f.get("type")
        if isinstance(t, dict):
            stack += t.get("fields", [])
            stack += [t]          # list / map ids live on the type object
    return best


def check_ids(md: dict, rep: Report) -> None:
    """Referential integrity of schema, partition spec and sort order ids."""
    schemas = {s["schema-id"]: s for s in md.get("schemas", [])}
    specs = {s["spec-id"]: s for s in md.get("partition-specs", [])}
    orders = {o["order-id"]: o for o in md.get("sort-orders", [])}
    all_field_ids = {f["id"] for s in schemas.values() for f in s["fields"]}
    rep.checked["ids"] += 6
    if md.get("current-schema-id") not in schemas:
        rep.fail("ids.current-schema-id", str(md.get("current-schema-id")))
    if schemas and md["last-column-id"] < max(_max_field_id(s["fields"]) for s in schemas.values()):
        rep.fail("ids.last-column-id", "lower than a field id in use")
    if md.get("default-spec-id") not in specs:
        rep.fail("ids.default-spec-id", str(md.get("default-spec-id")))
    for spec in specs.values():
        for pf in spec["fields"]:
            rep.checked["ids.partition-field"] += 1
            if pf["field-id"] < 1000:
                rep.fail("ids.partition-field-id", f"{pf['name']}: {pf['field-id']} < 1000")
            if pf["field-id"] > md.get("last-partition-id", 999):
                rep.fail("ids.last-partition-id", f"{pf['name']}: {pf['field-id']}")
            if pf["source-id"] not in all_field_ids:
                rep.fail("ids.partition-source-id", f"{pf['name']}: {pf['source-id']}")
    if md.get("default-sort-order-id") not in orders:
        rep.fail("ids.default-sort-order-id", str(md.get("default-sort-order-id")))
    if 0 in orders and orders[0]["fields"]:
        rep.fail("ids.sort-order-0", "order id 0 is reserved for the unsorted order")
    for order in orders.values():
        for sf in order["fields"]:
            if sf["source-id"] not in all_field_ids:
                rep.fail("ids.sort-source-id", f"order {order['order-id']}: {sf['source-id']}")
    for snap in md.get("snapshots", []):
        if "schema-id" in snap and snap["schema-id"] not in schemas:
            rep.fail("ids.snapshot-schema-id", f"snapshot {snap['snapshot-id']}")


def check_snapshots(md: dict, rep: Report) -> None:
    """Snapshot list, refs, logs and sequence-number rules."""
    v = md["format-version"]
    snaps = {s["snapshot-id"]: s for s in md.get("snapshots", [])}
    cur = md.get("current-snapshot-id")
    rep.checked["snapshots"] += 2
    if cur not in (None, -1) and cur not in snaps:
        rep.fail("snapshots.current-snapshot-id", str(cur))
    main = md.get("refs", {}).get("main")
    if main and main["snapshot-id"] != cur:
        rep.fail("snapshots.main-ref", f"main={main['snapshot-id']} current={cur}")
    for ref, r in md.get("refs", {}).items():
        if r["snapshot-id"] not in snaps:
            rep.fail("snapshots.ref-target", ref)
    seqs = []
    for s in snaps.values():
        rep.checked["snapshots.rules"] += 1
        op = s.get("summary", {}).get("operation")
        if v >= 2 and op not in OPERATIONS:
            rep.fail("snapshots.summary.operation", f"{s['snapshot-id']}: {op}")
        if s["timestamp-ms"] > md["last-updated-ms"]:
            rep.fail("snapshots.timestamp", f"{s['snapshot-id']} is newer than last-updated-ms")
        seq = s.get("sequence-number", 0)          # v1 snapshots are read as sequence number 0
        seqs.append(seq)
        if v >= 2 and seq > md.get("last-sequence-number", 0):
            rep.fail("seq.last-sequence-number", f"{s['snapshot-id']}: {seq}")
        parent = snaps.get(s.get("parent-snapshot-id"))
        if v >= 2 and parent and seq != 0 and seq <= parent.get("sequence-number", 0):
            rep.fail("seq.increasing", f"{s['snapshot-id']}: {seq} <= parent")
    nonzero = [q for q in seqs if q]
    if len(nonzero) != len(set(nonzero)):
        rep.fail("seq.unique", "two snapshots share a sequence number")
    log = md.get("snapshot-log", [])
    if any(a["timestamp-ms"] > b["timestamp-ms"] for a, b in zip(log, log[1:])):
        rep.fail("snapshots.log-order", "snapshot-log is not in time order")
    mlog = md.get("metadata-log", [])
    if any(a["timestamp-ms"] > b["timestamp-ms"] for a, b in zip(mlog, mlog[1:])):
        rep.fail("metadata-log.order", "metadata-log is not in time order")


# ---------------------------------------------------------------- Avro schemas (manifest list, manifest)
def avro_fields(record_schema: dict) -> dict[str, int | None]:
    return {f["name"]: f.get("field-id") for f in record_schema["fields"]}


def check_avro_fields(actual: dict[str, int | None], expected: dict, version: int, where: str,
                      rep: Report) -> None:
    for name, (fid, spec) in expected.items():
        rep.checked["avro.fields"] += 1
        if name not in actual:
            if required(spec, version):
                rep.fail(f"{where}.required.{name}", f"v{version} requires field {fid}")
        elif actual[name] != fid:
            rep.fail(f"{where}.field-id.{name}", f"expected {fid}, found {actual[name]}")
    for name in actual.keys() - expected.keys():
        rep.fail(f"{where}.unknown-field.{name}", "not defined by the spec")


# ---------------------------------------------------------------- single-value serialization (spec: binary single-value serialization)
def decode_value(iceberg_type: str, b: bytes):
    if iceberg_type in ("int", "date"):
        return struct.unpack("<i", b)[0]
    if iceberg_type in ("long", "time", "timestamp", "timestamptz"):
        return struct.unpack("<q", b)[0]
    if iceberg_type == "float":
        return struct.unpack("<f", b)[0]
    if iceberg_type == "double":
        return struct.unpack("<d", b)[0]
    if iceberg_type == "boolean":
        return b != b"\x00"
    if iceberg_type == "string":
        return b.decode("utf-8")
    if iceberg_type.startswith("decimal"):
        scale = int(iceberg_type.split(",")[1].rstrip(")"))
        return Decimal(int.from_bytes(b, "big", signed=True)).scaleb(-scale)
    return b                                       # binary, fixed, uuid: raw bytes


def _as_spec_value(iceberg_type: str, v):
    """Convert a pyarrow scalar value to the spec's in-memory representation."""
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp()) * 1_000_000 + v.microsecond
    if isinstance(v, date):
        return (v - date(1970, 1, 1)).days
    return v


def partition_result_type(transform: str, source_type: str) -> str:
    if transform in ("identity",) or transform.startswith("truncate"):
        return source_type
    if transform == "void":
        return source_type
    return "int"                                   # year, month, day, hour, bucket[N]


def check_bounds(data_file: dict, field_types: dict[int, str], rep: Report) -> dict:
    """Compare the manifest's column stats with the Parquet file they describe.

    Spec: bounds are inclusive (lower <= every value <= upper) and may be truncated; counts are exact.
    Returns the decoded stats per column name for reporting.
    """
    table = pq.read_table(io.BytesIO(read_object(data_file["file_path"])))
    lower = {kv["key"]: kv["value"] for kv in data_file.get("lower_bounds") or []}
    upper = {kv["key"]: kv["value"] for kv in data_file.get("upper_bounds") or []}
    nulls = {kv["key"]: kv["value"] for kv in data_file.get("null_value_counts") or []}
    values = {kv["key"]: kv["value"] for kv in data_file.get("value_counts") or []}
    rep.checked["stats.record_count"] += 1
    if table.num_rows != data_file["record_count"]:
        rep.fail("stats.record_count", f"{data_file['file_path']}: {data_file['record_count']} != {table.num_rows}")
    decoded = {}
    for pa_field in table.schema:
        fid = int(pa_field.metadata[b"PARQUET:field_id"]) if pa_field.metadata else None
        t = field_types.get(fid)
        if t is None or t.startswith(("struct", "list", "map")) or isinstance(t, dict):
            continue
        col = table.column(pa_field.name)
        rep.checked["stats.counts"] += 1
        if fid in values and values[fid] != len(col):
            rep.fail("stats.value_counts", f"{pa_field.name}: {values[fid]} != {len(col)}")
        if fid in nulls and nulls[fid] != col.null_count:
            rep.fail("stats.null_value_counts", f"{pa_field.name}: {nulls[fid]} != {col.null_count}")
        if fid not in lower or col.null_count == len(col):
            continue
        mm = pc.min_max(col)
        actual_min = _as_spec_value(t, mm["min"].as_py())
        actual_max = _as_spec_value(t, mm["max"].as_py())
        lo, hi = decode_value(t, lower[fid]), decode_value(t, upper[fid])
        rep.checked["stats.bounds"] += 1
        if isinstance(lo, float) and (math.isnan(lo) or math.isnan(hi)):
            rep.fail("stats.bounds.nan", f"{pa_field.name}: NaN must not be a bound")
        elif not (lo <= actual_min and actual_max <= hi):
            rep.fail("stats.bounds", f"{pa_field.name}: [{lo}, {hi}] does not cover [{actual_min}, {actual_max}]")
        decoded[pa_field.name] = {"lower": lo, "upper": hi, "min": actual_min, "max": actual_max}
    return decoded


def _decode_partition_value(v):
    return (v - date(1970, 1, 1)).days if isinstance(v, date) else v   # fastavro applies the date type


# ---------------------------------------------------------------- manifest list + manifests
def check_snapshot_files(md: dict, snap: dict, rep: Report, field_types: dict[int, str],
                         check_stats: bool) -> None:
    v = md["format-version"]
    specs = {s["spec-id"]: s for s in md["partition-specs"]}
    seq = snap.get("sequence-number", 0)
    schema, kv, manifests = read_avro(snap["manifest-list"])
    file_version = int(kv.get("format-version", 1))     # files written before an upgrade keep their version
    rep.checked["manifest_list"] += 1
    if file_version > v:
        rep.fail("manifest_list.format-version", f"file v{file_version} in a v{v} table")
    if int(kv["snapshot-id"]) != snap["snapshot-id"]:
        rep.fail("manifest_list.snapshot-id", snap["manifest-list"])
    if file_version >= 2 and int(kv.get("sequence-number", -1)) != seq:
        rep.fail("manifest_list.sequence-number", f"{kv.get('sequence-number')} != {seq}")
    check_avro_fields(avro_fields(schema), MANIFEST_LIST, file_version, "manifest_list", rep)
    # sequence-number is required for snapshots written at v2+; v1-era snapshots in an upgraded
    # table have none and are read as sequence number 0 (spec: "Version 2" upgrade rules).
    rep.checked["snapshot.required"] += 1
    if file_version >= 2 and "sequence-number" not in snap:
        rep.fail("snapshot.required.sequence-number", f"snapshot {snap['snapshot-id']}")

    for m in manifests:
        m_seq = m.get("sequence_number", 0) or 0
        if m["partition_spec_id"] not in specs:
            rep.fail("manifest_list.partition_spec_id", m["manifest_path"])
            continue
        if file_version >= 2:
            rep.checked["manifest_list.seq"] += 1
            if m_seq > seq or m["min_sequence_number"] > m_seq:
                rep.fail("manifest_list.seq", f"{m['manifest_path']}: min {m['min_sequence_number']}, "
                                              f"seq {m_seq}, snapshot {seq}")
            if m["added_snapshot_id"] == snap["snapshot-id"] and m_seq != seq:
                rep.fail("manifest_list.seq.new-manifest", m["manifest_path"])
        check_manifest(m, specs[m["partition_spec_id"]], rep, field_types, check_stats)


def check_manifest(m: dict, spec: dict, rep: Report, field_types: dict[int, str], check_stats: bool) -> None:
    schema, kv, entries = read_avro(m["manifest_path"])
    version = int(kv.get("format-version", 1))
    rep.checked["manifest.kv"] += 1
    for key in ["schema", "partition-spec"] + (["format-version", "partition-spec-id", "content"] if version >= 2 else []):
        if key not in kv:
            rep.fail(f"manifest.kv.{key}", m["manifest_path"])
    entry_fields = avro_fields(schema)
    check_avro_fields(entry_fields, MANIFEST_ENTRY, version, "manifest_entry", rep)
    df_schema = next(f["type"] for f in schema["fields"] if f["name"] == "data_file")
    check_avro_fields(avro_fields(df_schema), DATA_FILE, version, "data_file", rep)

    counts = Counter(e["status"] for e in entries)
    rep.checked["manifest_list.counts"] += 1
    for status, key in ((STATUS_ADDED, "added_files_count"), (STATUS_EXISTING, "existing_files_count"),
                        (STATUS_DELETED, "deleted_files_count")):
        if m.get(key) is not None and m[key] != counts[status]:
            rep.fail(f"manifest_list.{key}", f"{m[key]} != {counts[status]} in {m['manifest_path']}")

    # Partition summaries in the manifest list must cover every partition value in the manifest.
    for pf, summary in zip(spec["fields"], m.get("partitions") or []):
        rtype = partition_result_type(pf["transform"], field_types.get(pf["source-id"], "binary"))
        vals = [_decode_partition_value(e["data_file"]["partition"][pf["name"]]) for e in entries]
        non_null = [x for x in vals if x is not None]
        rep.checked["manifest_list.partitions"] += 1
        if len(non_null) < len(vals) and not summary["contains_null"]:
            rep.fail("manifest_list.partitions.contains_null", m["manifest_path"])
        if non_null and summary["lower_bound"] is not None:
            lo, hi = decode_value(rtype, summary["lower_bound"]), decode_value(rtype, summary["upper_bound"])
            if not (lo <= min(non_null) and max(non_null) <= hi):
                rep.fail("manifest_list.partitions.bounds", f"{pf['name']}: [{lo}, {hi}] vs {non_null}")

    m_content = m.get("content", 0) or 0
    m_seq = m.get("sequence_number", 0) or 0
    for e in entries:
        df = e["data_file"]
        content = df.get("content", 0)
        rep.checked["manifest.entries"] += 1
        if e["status"] not in (0, 1, 2):
            rep.fail("manifest.status", str(e["status"]))
        if e["status"] == STATUS_ADDED and e.get("snapshot_id") not in (None, m["added_snapshot_id"]):
            rep.fail("manifest.added.snapshot_id", df["file_path"])
        if version >= 2 and e["status"] == STATUS_ADDED and e.get("sequence_number") not in (None, m_seq):
            rep.fail("manifest.added.sequence_number", df["file_path"])
        if version >= 2 and e["status"] != STATUS_ADDED and e.get("sequence_number") is None:
            rep.fail("manifest.inherit.only-added", f"status {e['status']} entry without explicit sequence number")
        if (m_content == 0) != (content == 0):
            rep.fail("manifest.content", f"manifest content {m_content}, file content {content}")
        if content == 0 and df["file_format"] not in DATA_FORMATS:
            rep.fail("data_file.file_format", df["file_format"])
        if df["file_format"] == "PUFFIN":                       # v3 deletion vector
            if content != 1 or None in (df.get("referenced_data_file"), df.get("content_offset"),
                                        df.get("content_size_in_bytes")):
                rep.fail("data_file.deletion-vector", df["file_path"])
        if check_stats and content == 0 and e["status"] != STATUS_DELETED:
            check_bounds(df, field_types, rep)


def field_types_of(md: dict) -> dict[int, str]:
    """Field id -> Iceberg type string over all schemas (types only widen, so the last one wins)."""
    types = {}
    for s in md.get("schemas", [md.get("schema")]):
        for f in s["fields"]:
            types[f["id"]] = f["type"] if isinstance(f["type"], str) else f["type"].get("type", "struct")
    return types


def check_table(metadata_location: str, check_stats: bool = True) -> Report:
    """Validate one metadata.json and every snapshot it references (stats: current snapshot only)."""
    md = read_metadata(metadata_location)
    rep = Report(metadata_location, md.get("format-version"))
    check_metadata_fields(md, rep)
    if rep.format_version not in (1, 2, 3):
        return rep
    check_ids(md, rep)
    check_snapshots(md, rep)
    types = field_types_of(md)
    for snap in md.get("snapshots", []):
        check_snapshot_files(md, snap, rep, types,
                             check_stats=check_stats and snap["snapshot-id"] == md.get("current-snapshot-id"))
    return rep

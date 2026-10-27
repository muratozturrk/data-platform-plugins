"""Technology audit collector: inventory the real systems, then derive findings.

Collects from
  * PostgreSQL catalog: schemas, tables/views, sizes, row counts, columns, primary keys
  * pg_stat_statements: the actual query workload (who reads/writes which dataset, how often)
  * S3 object listing: datasets, file formats, file sizes, Hive partition keys, schemas from file headers
and merges the stakeholder interview findings (interviews.yaml) into one report organized by the
four audit domains: storage, processing, governance, consumption.
"""
import io
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import yaml

from common.lakehouse import LEGACY_BUCKET, list_objects, s3_client

MARKER = "/* platform-audit */"         # the auditor's own queries are excluded from the workload
PII_PATTERN = re.compile(r"email|e_mail|phone|iban|ssn|full_name|first_name|last_name|birth|address|tax_id|passport")
ROW_FORMATS = {"csv", "json"}
WRITE_VERBS = ("insert", "update", "delete", "merge", "copy", "truncate")
DOMAINS = ("storage", "processing", "governance", "consumption")
INTERVIEWS = Path(__file__).with_name("interviews.yaml")


@dataclass
class Dataset:
    id: str                              # "pg:trading.trades" | "s3:legacy-lake/trades_history"
    system: str                          # "postgresql" | "s3"
    kind: str                            # "table" | "view" | "files"
    bytes: int = 0
    rows: int | None = None
    files: int = 0
    formats: dict[str, int] = field(default_factory=dict)       # format -> bytes
    columns: list[str] = field(default_factory=list)
    partition_keys: list[str] = field(default_factory=list)
    has_primary_key: bool | None = None
    pii_columns: list[str] = field(default_factory=list)
    semi_structured: bool = False
    reads: int = 0                       # calls from pg_stat_statements
    writes: int = 0

    @property
    def main_format(self) -> str:
        return max(self.formats, key=self.formats.get) if self.formats else self.system


@dataclass
class Finding:
    id: str
    domain: str                          # one of DOMAINS
    severity: str                        # high | medium | low
    summary: str
    datasets: list[str]
    evidence: dict = field(default_factory=dict)


@dataclass
class AuditReport:
    generated_at: str
    datasets: list[Dataset]
    workload: list[dict]
    findings: list[Finding]
    stakeholder_findings: list[dict]

    def dataset(self, dataset_id: str) -> Dataset:
        return next(d for d in self.datasets if d.id == dataset_id)

    def finding(self, finding_id: str) -> Finding | None:
        return next((f for f in self.findings if f.id == finding_id), None)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    def to_markdown(self) -> str:
        lines = ["| Domain | Finding | Severity | Datasets |", "|---|---|---|---|"]
        for f in sorted(self.findings, key=lambda f: (DOMAINS.index(f.domain), f.id)):
            lines.append(f"| {f.domain} | {f.id}: {f.summary} | {f.severity} | {', '.join(f.datasets)} |")
        lines += ["", "| Dataset | Format | Rows | Files | Bytes | Reads | Writes |", "|---|---|---|---|---|---|---|"]
        for d in self.datasets:
            lines.append(f"| {d.id} | {d.main_format} | {d.rows} | {d.files} | {d.bytes} | {d.reads} | {d.writes} |")
        return "\n".join(lines)


# ---------------------------------------------------------------- PostgreSQL

def collect_postgres(conn) -> list[Dataset]:
    with conn.cursor() as cur:
        cur.execute(f"""{MARKER}
            SELECT n.nspname, c.relname, c.relkind, pg_total_relation_size(c.oid),
                   EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid = c.oid AND k.contype = 'p')
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p', 'v')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'
              -- objects owned by extensions (e.g. the pg_stat_statements views) are not datasets
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
            ORDER BY 1, 2""")
        relations = cur.fetchall()
        cur.execute(f"""{MARKER}
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name, ordinal_position""")
        columns: dict[tuple, list[tuple]] = {}
        for schema, table, col, dtype in cur.fetchall():
            columns.setdefault((schema, table), []).append((col, dtype))

        datasets = []
        for schema, name, relkind, size, has_pk in relations:
            cols = columns.get((schema, name), [])
            kind = "view" if relkind == "v" else "table"
            rows = None
            if kind == "table":
                # ponytail: exact count(*) is fine for a lab; use pg_class.reltuples on multi-TB tables.
                cur.execute(f'{MARKER} SELECT count(*) FROM "{schema}"."{name}"')
                rows = cur.fetchone()[0]
            datasets.append(Dataset(
                id=f"pg:{schema}.{name}", system="postgresql", kind=kind, bytes=size, rows=rows,
                formats={"postgresql": size}, columns=[c for c, _ in cols],
                has_primary_key=has_pk if kind == "table" else None,
                pii_columns=[c for c, _ in cols if PII_PATTERN.search(c)],
                semi_structured=any(t in ("json", "jsonb") for _, t in cols)))
    return datasets


def collect_workload(conn, datasets: list[Dataset]) -> list[dict]:
    """Attribute pg_stat_statements calls to datasets; returns the statements, busiest first."""
    by_name = {d.id.removeprefix("pg:"): d for d in datasets if d.system == "postgresql"}
    with conn.cursor() as cur:
        cur.execute(f"""{MARKER}
            SELECT query, calls, round(total_exec_time::numeric, 3), rows
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND query NOT LIKE '%platform-audit%'
            ORDER BY calls DESC""")
        statements = []
        for query, calls, total_ms, rows in cur.fetchall():
            refs = sorted({m for m in re.findall(r"\b[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\b", query.lower())
                           if m in by_name})
            if not refs:
                continue                       # catalog queries, SET, pg_stat_statements_reset(), ...
            is_write = query.lstrip().lower().startswith(WRITE_VERBS)
            for ref in refs:
                if is_write:
                    by_name[ref].writes += calls
                else:
                    by_name[ref].reads += calls
            statements.append({"query": query, "calls": calls, "total_ms": float(total_ms),
                               "kind": "write" if is_write else "read", "datasets": [f"pg:{r}" for r in refs]})
    return statements


# ---------------------------------------------------------------- object storage

def _columns_of(key: str, fmt: str) -> list[str]:
    # ponytail: downloads the whole object; use a ranged GET of the footer / first KB for large files.
    body = s3_client().get_object(Bucket=LEGACY_BUCKET, Key=key)["Body"].read()
    if fmt == "parquet":
        return pq.read_schema(io.BytesIO(body)).names
    first = body.split(b"\n", 1)[0].decode()
    return list(json.loads(first)) if fmt == "json" else first.split(",")


def _rows_of(key: str, fmt: str) -> int:
    body = s3_client().get_object(Bucket=LEGACY_BUCKET, Key=key)["Body"].read()
    if fmt == "parquet":
        return pq.ParquetFile(io.BytesIO(body)).metadata.num_rows
    lines = sum(1 for line in body.splitlines() if line.strip())
    return lines - 1 if fmt == "csv" else lines


def collect_object_store(bucket: str = LEGACY_BUCKET) -> list[Dataset]:
    """One dataset per top-level prefix, the usual layout of a Hive-style data lake."""
    groups: dict[str, list[dict]] = {}
    for obj in list_objects(bucket):
        if "/" in obj["Key"]:
            groups.setdefault(obj["Key"].split("/", 1)[0], []).append(obj)

    datasets = []
    for prefix, objs in sorted(groups.items()):
        formats, rows, part_keys = Counter(), 0, []
        for o in objs:
            fmt = o["Key"].rsplit(".", 1)[-1].lower()
            formats[fmt] += o["Size"]
            rows += _rows_of(o["Key"], fmt)
            for seg in o["Key"].split("/")[1:-1]:
                if "=" in seg and seg.split("=", 1)[0] not in part_keys:
                    part_keys.append(seg.split("=", 1)[0])
        main = formats.most_common(1)[0][0]
        cols = _columns_of(objs[0]["Key"], main)
        datasets.append(Dataset(
            id=f"s3:{bucket}/{prefix}", system="s3", kind="files", bytes=sum(formats.values()), rows=rows,
            files=len(objs), formats=dict(formats), columns=cols + part_keys, partition_keys=part_keys,
            pii_columns=[c for c in cols if PII_PATTERN.search(c)], semi_structured=main == "json"))
    return datasets


# ---------------------------------------------------------------- findings

def derive_findings(datasets: list[Dataset], small_file_bytes: int = 32 * 1024 * 1024) -> list[Finding]:
    """Rules that turn inventory facts into audit findings. Each rule encodes one audit heuristic."""
    findings = []

    def add(fid, domain, severity, summary, hits, **evidence):
        if hits:
            findings.append(Finding(fid, domain, severity, summary, [d.id for d in hits], evidence))

    files = [d for d in datasets if d.system == "s3"]
    lake_bytes = sum(d.bytes for d in files) or 1
    row_fmt = [d for d in files if d.main_format in ROW_FORMATS]
    # "Iceberg works best with columnar formats ... plan for conversion pipelines"
    add("F-ROW-FORMATS", "storage", "high", "Row-based or semi-structured files need conversion to Parquet",
        row_fmt, share_of_lake_bytes=round(sum(d.bytes for d in row_fmt) / lake_bytes, 3))
    # newer datasets already in Parquet can be adopted without a rewrite
    add("F-PARQUET-IN-PLACE", "storage", "low", "Parquet datasets can be registered in place (add_files)",
        [d for d in files if d.main_format == "parquet"])
    add("F-SMALL-FILES", "storage", "medium", "Many small files: plan compaction after migration",
        [d for d in files if d.files >= 4 and d.bytes / d.files < small_file_bytes],
        threshold_bytes=small_file_bytes)
    # upserts and deduplication need a key
    add("F-NO-PRIMARY-KEY", "processing", "high", "Tables without a primary key block upserts and dedup",
        [d for d in datasets if d.kind == "table" and d.has_primary_key is False])
    add("F-SEMI-STRUCTURED", "processing", "medium", "Semi-structured payloads need parsing in ingestion",
        [d for d in datasets if d.semi_structured])
    # residency, access control and audit trails for personal data
    add("F-PII", "governance", "high", "Personal data present: residency, RBAC and audit trail required",
        [d for d in datasets if d.pii_columns], columns={d.id: d.pii_columns for d in datasets if d.pii_columns})
    # semantic logic locked inside one tool causes drift
    add("F-WAREHOUSE-VIEWS", "consumption", "medium", "Business logic lives in warehouse views",
        [d for d in datasets if d.kind == "view"])
    total_reads = sum(d.reads for d in datasets) or 1
    add("F-HOT-DATASETS", "consumption", "medium", "Datasets carrying most of the read workload",
        [d for d in datasets if d.reads / total_reads >= 0.25],
        read_share={d.id: round(d.reads / total_reads, 3) for d in datasets if d.reads})
    return findings


def run_audit(conn, bucket: str = LEGACY_BUCKET, interviews: Path = INTERVIEWS) -> AuditReport:
    pg = collect_postgres(conn)
    workload = collect_workload(conn, pg)
    datasets = pg + collect_object_store(bucket)
    return AuditReport(
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        datasets=datasets, workload=workload, findings=derive_findings(datasets),
        stakeholder_findings=yaml.safe_load(interviews.read_text())["findings"])

"""Acceptance probes: executable checks that a candidate platform meets a requirement (requirements as
"clear, testable criteria for evaluating tools"). Each probe returns (passed, evidence)."""
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime

import requests
from pyiceberg.catalog import load_catalog

from common.lakehouse import REST_URI, S3_ENDPOINT, s3_client
from requirements_validation.requirements import Requirement, evaluate_metric


@dataclass
class Context:
    spark: object
    ns: str
    _py: object = field(default=None, repr=False)

    @property
    def pyiceberg(self):
        """Second, independent client of the same catalog: PyIceberg over the REST API."""
        if self._py is None:
            self._py = load_catalog("probe", **{
                "type": "rest", "uri": REST_URI, "s3.endpoint": S3_ENDPOINT, "s3.region": "us-east-1",
                "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),             # local dev default
                "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password")})
        return self._py

    def fresh_table(self, name: str, ddl: str) -> str:
        table = f"{self.ns}.{name}"
        self.spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        self.spark.sql(f"CREATE TABLE {table} ({ddl}) USING iceberg")
        return table


def s3_api(ctx: Context):
    """STO-02: the object store answers the S3 calls engines and ingestion tools make."""
    s3, key = s3_client(), f"_probe/{uuid.uuid4().hex}"
    s3.put_object(Bucket="warehouse", Key=key, Body=b"probe")
    body = s3.get_object(Bucket="warehouse", Key=key)["Body"].read()
    listed = [o["Key"] for o in s3.list_objects_v2(Bucket="warehouse", Prefix="_probe/").get("Contents", [])]
    s3.delete_object(Bucket="warehouse", Key=key)
    ok = body == b"probe" and key in listed
    return ok, {"put_get_list_delete": ok}


def rest_catalog(ctx: Context):
    """CAT-01: REST spec endpoints answer, and two different engines see the same table state."""
    config = requests.get(f"{REST_URI}/v1/config", timeout=10).json()
    table = ctx.fresh_table("probe_rest", "id BIGINT")
    ctx.spark.sql(f"INSERT INTO {table} VALUES (1), (2)")
    spark_snapshot = ctx.spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").first().snapshot_id
    listed = requests.get(f"{REST_URI}/v1/namespaces/{ctx.ns}/tables", timeout=10).json()
    py_snapshot = ctx.pyiceberg.load_table(table).current_snapshot().snapshot_id
    ok = ({"defaults", "overrides"} <= config.keys()
          and {"namespace": [ctx.ns], "name": "probe_rest"} in listed["identifiers"]
          and py_snapshot == spark_snapshot)
    return ok, {"config_keys": sorted(config), "spark_snapshot": spark_snapshot, "pyiceberg_snapshot": py_snapshot}


def schema_evolution_rollback(ctx: Context):
    """CAT-02: versioned schema evolution, rollback, and user attribution for all changes."""
    spark = ctx.spark
    table = ctx.fresh_table("probe_schema", "id BIGINT, amount DOUBLE")
    spark.createDataFrame([(1, 10.0)], "id BIGINT, amount DOUBLE") \
        .writeTo(table).option("snapshot-property.changed-by", "alice").append()
    first = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").first().snapshot_id
    count_snapshots = lambda: spark.sql(f"SELECT count(*) c FROM {table}.snapshots").first().c  # noqa: E731
    before = count_snapshots()
    spark.sql(f"ALTER TABLE {table} ADD COLUMN risk_score DOUBLE")
    schema_change_snapshots = count_snapshots() - before    # a commit record is where a user could be stored
    spark.createDataFrame([(2, 20.0, 0.7)], "id BIGINT, amount DOUBLE, risk_score DOUBLE") \
        .writeTo(table).option("snapshot-property.changed-by", "bob").append()
    spark.sql(f"CALL lake.system.rollback_to_snapshot('{table}', {first})")

    t = ctx.pyiceberg.load_table(table)
    snaps = spark.sql(f"SELECT summary['changed-by'] AS who FROM {table}.snapshots ORDER BY committed_at").collect()
    evidence = {
        "schema_versions": len(t.schemas()),
        "rows_after_rollback": spark.table(table).count(),
        "columns_after_rollback": spark.table(table).columns,
        "data_commits_attributed": [r.who for r in snaps],
        "schema_change_snapshots": schema_change_snapshots,
        # Without a snapshot, ALTER TABLE leaves only a new metadata.json, which has no user field.
        "schema_change_attributed": schema_change_snapshots > 0,
    }
    ok = (evidence["schema_versions"] == 2 and evidence["rows_after_rollback"] == 1
          and None not in evidence["data_commits_attributed"] and evidence["schema_change_attributed"])
    return ok, evidence


def idempotent_upsert(ctx: Context):
    """ING-01: duplicates and late events give one deterministic result, also when a batch is replayed."""
    spark = ctx.spark
    table = ctx.fresh_table("probe_upsert", "trade_id BIGINT, status STRING, event_ts TIMESTAMP")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'NEW', TIMESTAMP '2024-01-01 09:00:00')")
    batch = spark.createDataFrame([
        (1, "FILLED", datetime(2024, 1, 1, 9, 5)),       # newer version of trade 1
        (1, "PARTIAL", datetime(2024, 1, 1, 9, 3)),      # same entity again, older: must lose
        (1, "CANCELLED", datetime(2024, 1, 1, 8, 0)),    # late event older than the table state: ignored
        (2, "NEW", datetime(2024, 1, 1, 9, 1)),
        (2, "NEW", datetime(2024, 1, 1, 9, 1)),          # exact duplicate
    ], "trade_id BIGINT, status STRING, event_ts TIMESTAMP")
    batch.createOrReplaceTempView("probe_batch")
    merge = f"""
        MERGE INTO {table} t
        USING (SELECT trade_id, status, event_ts FROM (
                 SELECT *, row_number() OVER (PARTITION BY trade_id ORDER BY event_ts DESC, status) AS rn
                 FROM probe_batch) WHERE rn = 1) s
        ON t.trade_id = s.trade_id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *"""
    states = []
    for _ in range(2):                                    # replay the same batch
        spark.sql(merge)
        states.append(sorted(tuple(r) for r in spark.table(table).collect()))
    ok = states[0] == states[1] and [(r[0], r[1]) for r in states[0]] == [(1, "FILLED"), (2, "NEW")]
    return ok, {"state": [(r[0], r[1]) for r in states[1]], "replay_identical": states[0] == states[1]}


def identifier_fields(ctx: Context):
    """ING-04: the table schema declares its row identifier (Iceberg identifier fields)."""
    spark = ctx.spark
    table = ctx.fresh_table("probe_ident", "client_id INT NOT NULL, email STRING")
    spark.sql(f"ALTER TABLE {table} SET IDENTIFIER FIELDS client_id")
    ids = ctx.pyiceberg.load_table(table).schema().identifier_field_names()
    legacy = ctx.fresh_table("probe_ident_nullable", "client_id INT, email STRING")   # like crm.clients
    try:
        spark.sql(f"ALTER TABLE {legacy} SET IDENTIFIER FIELDS client_id")
        nullable_rejected = False
    except Exception as e:                                # noqa: BLE001 - Spark wraps the Iceberg error
        nullable_rejected = "not a required field" in str(e)
    return ids == {"client_id"} and nullable_rejected, {"identifier_fields": sorted(ids),
                                                        "nullable_column_rejected": nullable_rejected}


def python_access(ctx: Context):
    """CON-04: a Python client reads the same rows the SQL engine wrote, with filter pushdown."""
    spark = ctx.spark
    table = ctx.fresh_table("probe_python", "client_id BIGINT, segment STRING")
    spark.sql(f"INSERT INTO {table} SELECT id, IF(id % 4 = 0, 'premium', 'retail') FROM range(1000)")
    arrow = ctx.pyiceberg.load_table(table).scan(row_filter="segment == 'premium'").to_arrow()
    expected = spark.sql(f"SELECT count(*) c FROM {table} WHERE segment = 'premium'").first().c
    return arrow.num_rows == expected == 250, {"pyiceberg_rows": arrow.num_rows, "spark_rows": expected}


PROBES = {f.__name__: f for f in (s3_api, rest_catalog, schema_evolution_rollback,
                                  idempotent_upsert, identifier_fields, python_access)}


@dataclass
class Result:
    requirement: str
    method: str          # probe | metric | manual
    status: str          # pass | fail | manual | not-measured
    evidence: dict


def run_acceptance(reqs: list[Requirement], ctx: Context, metrics: dict | None = None) -> list[Result]:
    """Evaluate every requirement: run its probe, compare its metric, or list it for manual sign-off."""
    metrics, results = metrics or {}, []
    for r in reqs:
        a = r.acceptance
        if r.probe:
            ok, ev = PROBES[r.probe](ctx)
            results.append(Result(r.id, "probe", "pass" if ok else "fail", ev))
        elif "metric" in a:
            if a["metric"] not in metrics:
                results.append(Result(r.id, "metric", "not-measured", {"metric": a["metric"]}))
            else:
                ok = evaluate_metric(r, metrics[a["metric"]])
                results.append(Result(r.id, "metric", "pass" if ok else "fail",
                                      {"measured": metrics[a["metric"]], "target": f"{a['op']} {a['target']}"}))
        else:
            results.append(Result(r.id, "manual", "manual", {"check": a["manual"]}))
    return results

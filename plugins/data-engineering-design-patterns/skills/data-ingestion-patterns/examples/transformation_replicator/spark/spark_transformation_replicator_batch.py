"""Transformation Replicator: replicate a production dataset to staging without PII.

The job runs in the production environment and *pushes* the copy to staging. Between the read and the
write it applies the column rules of a PII contract (``pii_rules.json``):

    keep     -> column copied as is
    drop     -> data reduction, the column never leaves production (``drop``)
    hash     -> salted SHA-256, keeps joinability without exposing the raw value
    mask     -> keeps the first character and the e-mail domain
    nullify  -> column kept for schema compatibility, value removed
    expr     -> column-based transformation with a SQL expression

Two safety nets against desynchronisation (new columns appearing in production):
  * every source column must be classified, otherwise the job fails (``default_action: fail``) or drops it
    (``default_action: drop`` = allow-list mode);
  * ``--verify-only`` re-reads the staging copy and asserts no dropped column survived.

Optional row-level mapping (a ``map`` over rows): ``--row-mapping legacy_devices``
applies ``transform_device_row`` through ``mapInPandas``.

Text sources (JSON/CSV) are read with every value as string: no implicit date/number parsing that could
silently null out values.

Run:
  export PII_HASH_SALT=...   # from your secret store, never hardcoded
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_transformation_replicator_batch.py \
      --source-path s3://prod/devices --target-path s3://staging/devices --rules ../pii_rules.json
  python spark_transformation_replicator_batch.py --self-test     # pure-logic checks, no Spark needed
"""
import argparse
import json
import logging
import os
import sys
from typing import Dict, List, Tuple

log = logging.getLogger("transformation_replicator")

ALLOWED_ACTIONS = {"keep", "drop", "hash", "mask", "nullify", "expr"}
EMAIL_MASK_REGEX = r"^(.).*(@.*)$"


# ── Pure logic (unit-tested in _self_check) ────────────────────────────────────────────────────────

def validate_rules(rules: dict) -> None:
    if rules.get("default_action", "fail") not in {"fail", "drop"}:
        raise ValueError("default_action must be 'fail' or 'drop'")
    for column, spec in rules.get("columns", {}).items():
        action = spec.get("action")
        if action not in ALLOWED_ACTIONS:
            raise ValueError(f"{column}: unknown action {action!r}")
        if action == "expr" and not spec.get("expr"):
            raise ValueError(f"{column}: action 'expr' needs an 'expr' entry")


def plan_columns(source_columns: List[str], rules: dict) -> Tuple[List[Tuple[str, str, dict]], List[str]]:
    """Return (plan, unclassified). plan = [(column, action, spec)] in source order, drops excluded."""
    specs = rules.get("columns", {})
    plan, unclassified = [], []
    for column in source_columns:
        spec = specs.get(column)
        if spec is None:
            unclassified.append(column)
            continue
        if spec["action"] != "drop":
            plan.append((column, spec["action"], spec))
    if unclassified and rules.get("default_action", "fail") == "fail":
        raise ValueError(f"Unclassified columns in source, update the PII contract first: {unclassified}")
    return plan, unclassified


def forbidden_columns(rules: dict) -> List[str]:
    return [c for c, s in rules.get("columns", {}).items() if s["action"] == "drop"]


def transform_device_row(row: Dict) -> Dict:
    """Row-level rule: legacy 1.x devices lose the first name character, version invalidated."""
    version = row.get("version") or ""
    if version.startswith("1."):
        full_name = row.get("full_name") or ""
        return {**row, "full_name": full_name[1:], "version": "invalid"}
    return row


def _self_check() -> None:
    rules = {"default_action": "fail", "columns": {
        "id": {"action": "keep"}, "ip": {"action": "drop"}, "email": {"action": "mask"},
        "name": {"action": "expr", "expr": "UPPER(name)"}}}
    validate_rules(rules)
    plan, unclassified = plan_columns(["id", "ip", "email", "name"], rules)
    assert [p[0] for p in plan] == ["id", "email", "name"] and unclassified == []
    try:
        plan_columns(["id", "new_phone"], rules)
        raise AssertionError("unclassified column must fail")
    except ValueError:
        pass
    plan, unclassified = plan_columns(["id", "new_phone"], {**rules, "default_action": "drop"})
    assert [p[0] for p in plan] == ["id"] and unclassified == ["new_phone"]
    try:
        validate_rules({"columns": {"x": {"action": "expr"}}})
        raise AssertionError("expr without expression must fail")
    except ValueError:
        pass
    assert forbidden_columns(rules) == ["ip"]
    assert transform_device_row({"version": "1.2", "full_name": "Xbox"}) == {"version": "invalid", "full_name": "box"}
    assert transform_device_row({"version": "2.0", "full_name": "Xbox"})["full_name"] == "Xbox"
    import re
    assert re.sub(EMAIL_MASK_REGEX, r"\1***\2", "alice@example.com") == "a***@example.com"
    print("self-test OK")


# ── Spark part ─────────────────────────────────────────────────────────────────────────────────────

def build_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("transformation-replicator")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def read_source(spark, path: str, fmt: str, partition_filter: str):
    reader = spark.read
    if fmt == "json":
        df = reader.option("primitivesAsString", "true").json(path)     # no implicit typing
    elif fmt == "csv":
        df = reader.option("header", "true").option("inferSchema", "false").csv(path)
    else:
        df = reader.format("delta").load(path)
    return df.where(partition_filter) if partition_filter else df


def apply_plan(df, plan, salt: str):
    from pyspark.sql import functions as F
    out = []
    for column, action, spec in plan:
        c = F.col(f"`{column}`")
        if action == "keep":
            out.append(c)
        elif action == "hash":
            out.append(F.sha2(F.concat(F.lit(salt), c.cast("string")), 256).alias(column))
        elif action == "mask":
            out.append(F.regexp_replace(c.cast("string"), EMAIL_MASK_REGEX, "$1***$2").alias(column))
        elif action == "nullify":
            out.append(F.lit(None).cast(df.schema[column].dataType).alias(column))
        elif action == "expr":
            out.append(F.expr(spec["expr"]).alias(column))
    return df.select(*out)


def apply_row_mapping(df, mapping: str):
    if mapping != "legacy_devices":
        raise ValueError(f"unknown row mapping {mapping}")
    missing = {"version", "full_name"} - set(df.columns)
    if missing:
        raise ValueError(f"row mapping legacy_devices needs columns {missing}")
    import pandas as pd

    def mapper(batches):
        for pdf in batches:
            yield pd.DataFrame([transform_device_row(r) for r in pdf.to_dict("records")], columns=pdf.columns)

    return df.mapInPandas(mapper, schema=df.schema)


def source_version(spark, path: str, fmt: str):
    if fmt != "delta":
        return None
    from delta.tables import DeltaTable
    return DeltaTable.forPath(spark, path).history(1).collect()[0]["version"]


def replicate(spark, args, rules) -> None:
    salt = os.environ.get("PII_HASH_SALT")
    if not salt and any(s["action"] == "hash" for s in rules["columns"].values()):
        raise RuntimeError("PII_HASH_SALT is not set; refusing to hash with an empty salt")

    df = read_source(spark, args.source_path, args.source_format, args.partition_filter)
    plan, unclassified = plan_columns(df.columns, rules)
    if unclassified:
        log.warning("Dropping unclassified columns (allow-list mode): %s", unclassified)
    log.info("Plan: %s", [(c, a) for c, a, _ in plan])

    replica = apply_plan(df, plan, salt or "")
    if args.row_mapping:
        replica = apply_row_mapping(replica, args.row_mapping)

    version = source_version(spark, args.source_path, args.source_format)
    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata",
                   json.dumps({"replicated_from": args.source_path, "source_version": version,
                               "rules_dataset": rules.get("dataset")}))
    writer = replica.write.format("delta").mode("overwrite")
    if args.partition_filter:
        writer = writer.option("replaceWhere", args.partition_filter)     # idempotent partial refresh
    writer.save(args.target_path)
    log.info("Replicated %s (version %s) -> %s", args.source_path, version, args.target_path)


def verify(spark, args, rules) -> None:
    target = spark.read.format("delta").load(args.target_path)
    leaked = sorted(set(target.columns) & set(forbidden_columns(rules)))
    if leaked:
        raise RuntimeError(f"PII columns present in staging copy: {leaked}")
    source = read_source(spark, args.source_path, args.source_format, args.partition_filter)
    tgt = target.where(args.partition_filter) if args.partition_filter else target
    src_count, tgt_count = source.count(), tgt.count()
    if src_count != tgt_count:
        raise RuntimeError(f"Row count mismatch: source={src_count} staging={tgt_count}")
    log.info("Verification passed: %d rows, no forbidden columns", tgt_count)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--source-path")
    p.add_argument("--target-path")
    p.add_argument("--rules", help="PII contract JSON (ideally exported from the data catalog)")
    p.add_argument("--source-format", choices=["delta", "json", "csv"], default="delta")
    p.add_argument("--partition-filter", default="", help="e.g. \"event_date = '2024-01-01'\"")
    p.add_argument("--row-mapping", choices=["legacy_devices"], default=None)
    p.add_argument("--verify-only", action="store_true")
    p.add_argument("--self-test", action="store_true")
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)
    if args.self_test:
        _self_check()
        return 0
    if not (args.source_path and args.target_path and args.rules):
        raise SystemExit("--source-path, --target-path and --rules are required")
    with open(args.rules, encoding="utf-8") as f:
        rules = json.load(f)
    validate_rules(rules)

    spark = build_spark()
    try:
        (verify if args.verify_only else replicate)(spark, args, rules)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Proxy on Spark: write-once versions behind one access point.

The three implementation scenarios (view, manifest, versioned), selected with --mode:

  view       process -> write devices_internal_<run_ts> (mode errorifexists)
                     -> CREATE OR REPLACE VIEW dp.devices AS SELECT * FROM <that table>
  manifest   --step load:     process -> write <base>/versions/<run_ts>/ (errorifexists)
             --step manifest: list the files of the newest complete version and
                              publish <base>/_manifest.json atomically
             (separate steps: a failed manifest never forces reprocessing the data)
  versioned  process -> overwrite a regular Delta table; every overwrite is a new
             table version readable with VERSION AS OF. No permissions to remove,
             but history only lives as long as the log/file retention allows.

The run timestamp (--run-ts, default now in UTC) is the run start time, not the
logical date: a rerun must write a NEW version instead of rewriting an old one.

Immutability is enforced below Spark: no write/delete grants on old tables or
prefixes, and WORM storage (S3 Object Lock, Azure Blob immutability policies,
GCS bucket lock / object holds) for the versions/ prefix. mode("errorifexists")
only protects against this job's own mistakes.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    spark_versioned_tables_proxy.py --mode view --input-path /data/raw/devices.csv
  ... --mode manifest --step load     --input-path ... --base-path /lake/devices
  ... --mode manifest --step manifest --base-path /lake/devices
  ... --mode versioned --input-path ...
  python spark_versioned_tables_proxy.py --self-test
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark_proxy")

DATABASE = "dp"
VIEW = f"{DATABASE}.devices"
VERSION_RE = re.compile(r"^\d{8}_\d{6}$")
# Long enough for the legal retention of the versioned approach; VACUUM honours it.
VERSIONED_TABLE_PROPS = {
    "delta.logRetentionDuration": "interval 3650 days",
    "delta.deletedFileRetentionDuration": "interval 3650 days",
}


# --------------------------------------------------------------------------- pure helpers
def version_suffix(run_start: datetime) -> str:
    return run_start.strftime("%Y%m%d_%H%M%S")


def internal_table_name(suffix: str) -> str:
    if not VERSION_RE.match(suffix):
        raise ValueError(f"bad version suffix {suffix!r}")
    return f"{DATABASE}.devices_internal_{suffix}"


def latest_complete_version(entries: dict[str, bool]) -> str:
    """entries: version dir name -> has _SUCCESS marker. Returns newest complete one."""
    complete = sorted(v for v, ok in entries.items() if ok and VERSION_RE.match(v))
    if not complete:
        raise ValueError("no complete version to publish")
    return complete[-1]


def manifest_document(version: str, location: str, files: list[str], fmt: str = "parquet") -> dict:
    if not files:
        raise ValueError(f"version {version} has no data files")
    return {"dataset": "devices", "version": version, "location": location, "format": fmt,
            "files": sorted(files), "published_at": datetime.now(timezone.utc).isoformat()}


def _self_check() -> None:
    s = version_suffix(datetime(2024, 1, 8, 3, 15, 0))
    assert s == "20240108_031500" and internal_table_name(s).endswith("_20240108_031500")
    assert latest_complete_version({"20240108_031500": True, "20240109_031500": False,
                                    "_tmp": True}) == "20240108_031500"
    doc = manifest_document(s, "/lake/devices/versions/" + s, ["b.parquet", "a.parquet"])
    assert doc["files"] == ["a.parquet", "b.parquet"]
    for bad in (lambda: internal_table_name("x;DROP"), lambda: latest_complete_version({}),
                lambda: manifest_document(s, "/x", [])):
        try:
            bad()
            raise AssertionError("expected ValueError")
        except ValueError:
            pass
    # atomic local publish
    with tempfile.TemporaryDirectory() as d:
        target = os.path.join(d, "_manifest.json")
        publish_manifest_local(target, doc)
        publish_manifest_local(target, {**doc, "version": "20240109_031500"})
        with open(target, encoding="utf-8") as fh:
            assert json.load(fh)["version"] == "20240109_031500"
    print("spark_versioned_tables_proxy self-check OK")


def publish_manifest_local(target: str, doc: dict) -> None:
    tmp = f"{target}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2)
    os.replace(tmp, target)  # atomic on POSIX and Windows for the same volume


# --------------------------------------------------------------------------- Spark side
def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("proxy-versioned-devices").getOrCreate()


def process(spark, input_path: str):
    """The dataset generation itself: a full devices snapshot."""
    from pyspark.sql import functions as F
    df = (spark.read.option("header", "true").option("delimiter", ";").csv(input_path)
          .dropDuplicates(["type", "version"])
          .withColumn("loaded_at", F.current_timestamp()))
    if df.isEmpty():
        raise ValueError(f"{input_path} produced an empty snapshot; not publishing it")
    return df


def run_view(spark, args) -> None:
    table = internal_table_name(args.run_ts)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    # errorifexists: a version is written once; a name clash fails instead of overwriting.
    process(spark, args.input_path).write.format("delta").mode("errorifexists").saveAsTable(table)
    spark.sql(f"CREATE OR REPLACE VIEW {VIEW} AS SELECT * FROM {table}")
    log.info("%s now points to %s", VIEW, table)


def run_manifest_load(spark, args) -> None:
    location = f"{args.base_path.rstrip('/')}/versions/{args.run_ts}"
    # Spark writes _SUCCESS at job commit: the manifest step only trusts such versions.
    process(spark, args.input_path).write.mode("errorifexists").parquet(location)
    log.info("Version written to %s; run --step manifest to publish it", location)


def _hadoop_fs(spark, path: str):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    return jvm, conf, p, p.getFileSystem(conf)


def run_manifest_publish(spark, args) -> None:
    base = args.base_path.rstrip("/")
    jvm, conf, versions_path, fs = _hadoop_fs(spark, f"{base}/versions")
    entries = {}
    for status in fs.listStatus(versions_path):
        if status.isDirectory():
            name = status.getPath().getName()
            entries[name] = fs.exists(jvm.org.apache.hadoop.fs.Path(status.getPath(), "_SUCCESS"))
    version = latest_complete_version(entries)
    location = f"{base}/versions/{version}"
    files = list(spark.read.parquet(location).inputFiles())
    doc = manifest_document(version, location, files)
    target = f"{base}/_manifest.json"

    if base.startswith("file:"):  # local runs: os.replace is atomic
        publish_manifest_local(target[len("file:"):].replace("///", "/", 1), doc)
    else:
        # Write a temp file, then rename with OVERWRITE: atomic on HDFS. On object stores
        # rename is copy+delete, so readers should tolerate a missing manifest briefly.
        tmp = jvm.org.apache.hadoop.fs.Path(f"{target}.tmp")
        out = fs.create(tmp, True)
        out.write(bytearray(json.dumps(doc, indent=2).encode("utf-8")))
        out.close()
        fc = jvm.org.apache.hadoop.fs.FileContext.getFileContext(tmp.toUri(), conf)
        rename_opts = spark.sparkContext._gateway.new_array(jvm.org.apache.hadoop.fs.Options.Rename, 1)
        rename_opts[0] = jvm.org.apache.hadoop.fs.Options.Rename.OVERWRITE
        fc.rename(tmp, jvm.org.apache.hadoop.fs.Path(target), rename_opts)
    log.info("Manifest %s now lists version %s (%d files)", target, version, len(files))
    # "Remove read access from the previous files" belongs to the storage
    # layer (bucket policy / ACL on versions/<previous>), not to this job.


def run_versioned(spark, args) -> None:
    from delta.tables import DeltaTable
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    table = f"{DATABASE}.devices_versioned"
    process(spark, args.input_path).write.format("delta").mode("overwrite") \
        .option("userMetadata", f"run_ts={args.run_ts}").saveAsTable(table)
    props = ", ".join(f"'{k}' = '{v}'" for k, v in VERSIONED_TABLE_PROPS.items())
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({props})")
    last = DeltaTable.forName(spark, table).history(1).collect()[0]
    log.info("%s overwritten as version %s; older snapshots: SELECT * FROM %s VERSION AS OF <n>",
             table, last["version"], table)


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser(description="Proxy pattern on Spark (view / manifest / versioned)")
    p.add_argument("--mode", choices=["view", "manifest", "versioned"])
    p.add_argument("--step", choices=["load", "manifest"], default="load", help="manifest mode only")
    p.add_argument("--input-path")
    p.add_argument("--base-path", default="/lake/devices")
    p.add_argument("--run-ts", default=version_suffix(datetime.now(timezone.utc)),
                   help="run start time YYYYmmdd_HHMMSS (pass the orchestrator's run start)")
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args(argv)
    if args.self_test:
        _self_check()
        return
    if not args.mode:
        p.error("--mode is required")
    needs_input = not (args.mode == "manifest" and args.step == "manifest")
    if needs_input and not args.input_path:
        p.error("--input-path is required for this mode/step")
    spark = _spark()
    if args.mode == "view":
        run_view(spark, args)
    elif args.mode == "versioned":
        run_versioned(spark, args)
    elif args.step == "load":
        run_manifest_load(spark, args)
    else:
        run_manifest_publish(spark, args)


if __name__ == "__main__":
    main(sys.argv[1:])

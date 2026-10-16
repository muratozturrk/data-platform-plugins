"""Passthrough Replicator: copy a production dataset to dev/staging exactly as is.

Problem: the reference dataset is loaded in production from a non-idempotent third-party API,
so replaying the loader in staging would give different data. The lower environments need the same bytes.

Two modes, both "extract and load" only, no interpretation of the data:
  text   - ``spark.read.text`` -> ``write.text``. Lines are copied untouched, no JSON
           parsing, no type conversion. File count/names are NOT preserved (Spark decides the layout).
  files  - Hadoop ``FileUtil.copy`` of the whole directory: same file names, same bytes, and the table
           metadata comes along (``_delta_log``, ``_SUCCESS``). Mandatory for Delta/Iceberg tables: copying only
           the Parquet files of a Delta table does not give you a table.

The job is meant to run in the PRODUCTION environment and push to the lower one (push, not pull).
Both modes write to a temporary sibling directory and swap it in at the end, so the target is never half
copied, and verify the copy (line count for text, file count + byte size for files).

Run:
  spark-submit spark_passthrough_file_replicator.py --source s3a://prod/devices/date=2024-01-01 \
      --target s3a://staging/devices/date=2024-01-01 --mode text
  spark-submit spark_passthrough_file_replicator.py --source /lake/prod/devices --target /lake/dev/devices --mode files
  python spark_passthrough_file_replicator.py --self-test
"""
import argparse
import logging
import sys
import uuid

log = logging.getLogger("passthrough_file_replicator")


# ── Pure logic ─────────────────────────────────────────────────────────────────────────────────────

def choose_mode(requested: str, has_table_log: bool) -> str:
    """Refuse the text mode on a table format: it would replicate data files without their commit log."""
    if requested == "text" and has_table_log:
        raise ValueError("Source is a Delta/Iceberg table (has a commit log); use --mode files")
    return requested


def summaries_match(src: dict, dst: dict) -> bool:
    return src["files"] == dst["files"] and src["bytes"] == dst["bytes"]


def _self_check() -> None:
    assert choose_mode("files", True) == "files"
    assert choose_mode("text", False) == "text"
    try:
        choose_mode("text", True)
        raise AssertionError("text mode on a Delta table must fail")
    except ValueError:
        pass
    assert summaries_match({"files": 3, "bytes": 10}, {"files": 3, "bytes": 10})
    assert not summaries_match({"files": 3, "bytes": 10}, {"files": 2, "bytes": 10})
    print("self-test OK")


# ── Hadoop FS helpers (through the Spark JVM gateway) ──────────────────────────────────────────────

class HadoopFS:
    def __init__(self, spark):
        self.jvm = spark.sparkContext._jvm
        self.conf = spark.sparkContext._jsc.hadoopConfiguration()

    def _fs(self, path):
        return self.jvm.org.apache.hadoop.fs.Path(path).getFileSystem(self.conf)

    def path(self, p):
        return self.jvm.org.apache.hadoop.fs.Path(p)

    def exists(self, p) -> bool:
        return self._fs(p).exists(self.path(p))

    def delete(self, p) -> None:
        if self.exists(p):
            self._fs(p).delete(self.path(p), True)

    def rename(self, src, dst) -> None:
        if not self._fs(src).rename(self.path(src), self.path(dst)):
            raise RuntimeError(f"rename {src} -> {dst} failed")

    def copy_tree(self, src, dst) -> None:
        # ponytail: single-threaded driver copy; switch to DistCp / cloud replication for TB-scale datasets
        ok = self.jvm.org.apache.hadoop.fs.FileUtil.copy(
            self._fs(src), self.path(src), self._fs(dst), self.path(dst), False, True, self.conf)
        if not ok:
            raise RuntimeError(f"copy {src} -> {dst} failed")

    def summary(self, p) -> dict:
        s = self._fs(p).getContentSummary(self.path(p))
        return {"files": s.getFileCount(), "bytes": s.getLength()}


# ── Replication ────────────────────────────────────────────────────────────────────────────────────

def replicate_text(spark, source: str, tmp: str) -> int:
    lines = spark.read.text(source)              # one string column `value`, nothing parsed
    lines.write.mode("overwrite").text(tmp)
    src_count, dst_count = lines.count(), spark.read.text(tmp).count()
    if src_count != dst_count:
        raise RuntimeError(f"line count mismatch: source={src_count} copy={dst_count}")
    return dst_count


def replicate_files(fs: HadoopFS, source: str, tmp: str) -> dict:
    fs.copy_tree(source, tmp)
    src, dst = fs.summary(source), fs.summary(tmp)
    if not summaries_match(src, dst):
        raise RuntimeError(f"copy differs from source: {src} vs {dst}")
    return dst


def run(args) -> None:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"passthrough-replicator-{args.mode}").getOrCreate()
    fs = HadoopFS(spark)
    try:
        if not fs.exists(args.source):
            raise FileNotFoundError(args.source)
        has_log = fs.exists(f"{args.source}/_delta_log") or fs.exists(f"{args.source}/metadata")
        mode = choose_mode(args.mode, has_log)

        tmp = f"{args.target.rstrip('/')}.__replica_{uuid.uuid4().hex[:8]}"
        fs.delete(tmp)
        result = replicate_text(spark, args.source, tmp) if mode == "text" else replicate_files(fs, args.source, tmp)

        # Swap: equivalent of `aws s3 sync --delete`, objects absent from the source disappear from the target.
        # On object stores rename = copy + delete; the window is short but readers of the lower env may see it.
        backup = f"{args.target.rstrip('/')}.__previous"
        fs.delete(backup)
        if fs.exists(args.target):
            fs.rename(args.target, backup)
        fs.rename(tmp, args.target)
        fs.delete(backup)
        log.info("Replicated %s -> %s (%s): %s", args.source, args.target, mode, result)
    finally:
        spark.stop()


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source")
    p.add_argument("--target")
    p.add_argument("--mode", choices=["text", "files"], default="files")
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args(argv)
    if args.self_test:
        _self_check()
        return 0
    if not (args.source and args.target):
        p.error("--source and --target are required")
    run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())

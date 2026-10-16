"""Streaming Reprocessor - rewind a Structured Streaming job (PySpark).

Spark cannot be submitted "from checkpoint N" (Flink can, with -s). Two safe strategies:

  new-checkpoint  : fresh checkpointLocation + startingTimestamp on the Kafka source.
                    Only for STATELESS jobs (state starts empty).
  restore-copy    : copy the checkpoint up to a valid batch N into a NEW location
                    (offsets/commits <= N, state versions <= N+1) and restart from it.
                    The original checkpoint stays untouched as a backup.
The riskiest option, deleting newer files in place, is not shown.

Sink must be idempotent (MERGE / keyed overwrite): reprocessing re-emits rows.

Run:
  spark-submit ... reprocess_from_past.py new-checkpoint --from-ts-ms 1717200000000
  spark-submit ... reprocess_from_past.py restore-copy --last-valid-batch 41
Env:  KAFKA_BOOTSTRAP, CHECKPOINT_DIR (current), local/POSIX checkpoint paths for restore-copy
"""
import argparse
import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
CHECKPOINT_DIR = Path(os.environ.get("CHECKPOINT_DIR", "/tmp/dp/checkpoints/sessions"))


def copy_checkpoint_until(src: Path, dst: Path, last_valid_batch: int) -> None:
    """Copy metadata + batches <= N. State store files are versioned N+1 for batch N."""
    if dst.exists():
        raise FileExistsError(f"{dst} exists - pick a new location")
    shutil.copytree(src, dst, ignore=lambda d, names: [n for n in names if _is_newer(Path(d), n, last_valid_batch)])


def _is_newer(directory: Path, name: str, last_valid_batch: int) -> bool:
    stem = name.lstrip(".").split(".")[0]          # "42", ".42.crc", "43.delta", "43.zip", "43.snapshot"
    # directories like state/0/<partition> are numeric too: only version FILES are filtered
    if not stem.isdigit() or (directory / name).is_dir() or "sources" in directory.parts:
        return False
    version = int(stem)
    in_state = "state" in directory.parts
    return version > (last_valid_batch + 1 if in_state else last_valid_batch)


def start_job(spark: SparkSession, checkpoint: str, starting_ts_ms: int | None) -> None:
    reader = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", "visits"))
    if starting_ts_ms is not None:  # only used when the checkpoint is empty
        reader = reader.option("startingTimestamp", str(starting_ts_ms))
    visits = reader.load().selectExpr("CAST(key AS STRING) AS visit_id", "CAST(value AS STRING) AS payload")
    (visits.writeStream.format("console").option("checkpointLocation", checkpoint)
     .start().awaitTermination())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("strategy", choices=["new-checkpoint", "restore-copy"])
    parser.add_argument("--from-ts-ms", type=int)
    parser.add_argument("--last-valid-batch", type=int)
    args = parser.parse_args()
    session = SparkSession.builder.appName("sessions_reprocessing").getOrCreate()
    if args.strategy == "new-checkpoint":
        start_job(session, f"{CHECKPOINT_DIR}_reprocess_{args.from_ts_ms}", args.from_ts_ms)
    else:
        target = Path(f"{CHECKPOINT_DIR}_restored_{args.last_valid_batch}")
        copy_checkpoint_until(CHECKPOINT_DIR, target, args.last_valid_batch)
        start_job(session, str(target), None)

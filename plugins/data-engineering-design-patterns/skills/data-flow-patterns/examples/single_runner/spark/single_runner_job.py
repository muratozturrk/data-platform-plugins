"""Single Runner -- PySpark incremental job with an in-job sequential guard.

For runners without a native concurrency=1 setting (cron, a bare spark-submit, some
schedulers) the job enforces the Single Runner itself:
  1. Readiness Marker on the PREVIOUS run: refuse to start if run N-1 has no _SUCCESS
     (the in-job equivalent of Airflow's depends_on_past);
  2. an exclusive lease lock: created atomically (O_CREAT|O_EXCL), kept alive by a
     heartbeat thread (mtime refresh), and taken over when STALE (heartbeat older than
     --lock-ttl) so a crashed run (OOM, killed driver) doesn't block the pipeline forever.
     Takeover is race-safe: the stale lock is atomically renamed away first, so only one
     contender can win it, then the new lock is created with O_EXCL again.
  3. release only if we still own the lock (a run whose lease was taken over must not
     delete the newer owner's lock).
Then it merges the previous state with today's events (incremental sessionization-like) and
overwrites only its own date partition (idempotent on retry).

Run:
    spark-submit single_runner_job.py --base /tmp/dp/sessions --date 2024-01-02 [--first-run]
    python single_runner_job.py --self-test        # lock logic only, no Spark needed
Local-FS lock/markers; on object stores use a conditional put (S3 If-None-Match, GCS
ifGenerationMatch) or a DB row lock with the same lease/TTL semantics.
Requires: pyspark>=3.3.
"""
import argparse
import datetime as dt
import json
import logging
import os
import socket
import sys
import tempfile
import threading
import time
import uuid

logger = logging.getLogger("single_runner")


class LockHeldError(RuntimeError):
    pass


class LeaseLock:
    """File lease: exclusive create + heartbeat + stale takeover after `ttl_seconds`."""

    def __init__(self, path: str, ttl_seconds: int = 3600, heartbeat_seconds: int = 60):
        if heartbeat_seconds * 2 >= ttl_seconds:
            raise ValueError("heartbeat must be well below the TTL (< ttl/2)")
        self.path = path
        self.ttl = ttl_seconds
        self.heartbeat = heartbeat_seconds
        self.owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._thread = None

    def _create(self) -> bool:
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as f:
            json.dump({"owner": self.owner, "acquired_at": time.time()}, f)
        return True

    def _age(self) -> float:
        return time.time() - os.path.getmtime(self.path)

    def current_owner(self):
        try:
            with open(self.path) as f:
                return json.load(f).get("owner")
        except (FileNotFoundError, ValueError):
            return None

    def acquire(self) -> None:
        if self._create():
            return self._start_heartbeat()
        try:
            age = self._age()
        except FileNotFoundError:  # released between our two calls -> retry once
            if self._create():
                return self._start_heartbeat()
            raise LockHeldError(f"{self.path} re-acquired by another run")
        if age < self.ttl:
            raise LockHeldError(f"{self.path} held by {self.current_owner()} (heartbeat {age:.0f}s ago)")
        # Stale: atomically move it away; os.rename succeeds for only one contender.
        tombstone = f"{self.path}.stale.{uuid.uuid4().hex}"
        try:
            os.rename(self.path, tombstone)
        except FileNotFoundError:
            raise LockHeldError(f"{self.path}: another run took over the stale lock first")
        logger.warning("took over stale lock %s (age %.0fs > ttl %ss)", self.path, age, self.ttl)
        os.remove(tombstone)
        if not self._create():
            raise LockHeldError(f"{self.path} created by another run during takeover")
        self._start_heartbeat()

    def _start_heartbeat(self) -> None:
        def beat():
            while not self._stop.wait(self.heartbeat):
                if self.current_owner() != self.owner:
                    logger.error("lease on %s lost to %s", self.path, self.current_owner())
                    return
                os.utime(self.path, None)
        self._thread = threading.Thread(target=beat, daemon=True, name="lock-heartbeat")
        self._thread.start()

    def release(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self.current_owner() == self.owner:
            os.remove(self.path)
        else:
            logger.error("not releasing %s: owned by %s", self.path, self.current_owner())

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


def run_job(base: str, date: str, first_run: bool, lock_ttl: int) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    day = dt.date.fromisoformat(date)
    prev_state = f"{base}/state/date={day - dt.timedelta(days=1)}"
    new_state = f"{base}/state/date={day}"

    if not first_run and not os.path.exists(f"{prev_state}/_SUCCESS"):
        sys.exit(f"Previous run {prev_state} not completed -- run dates sequentially")

    os.makedirs(base, exist_ok=True)
    try:
        lock = LeaseLock(f"{base}/_LOCK", ttl_seconds=lock_ttl, heartbeat_seconds=max(5, lock_ttl // 10))
        lock.acquire()
    except LockHeldError as error:
        sys.exit(f"Single Runner refuses to start: {error}")
    try:
        spark = SparkSession.builder.appName("single-runner").getOrCreate()
        # Stand-in for today's input; replace with the real incremental read.
        today = spark.createDataFrame([("u1", 3), ("u2", 1)], "user_id STRING, visits INT")
        if first_run:
            merged = today
        else:
            previous = spark.read.parquet(prev_state)
            merged = (previous.unionByName(today).groupBy("user_id")
                      .agg(F.sum("visits").alias("visits")))
        merged.write.mode("overwrite").parquet(new_state)  # Spark writes _SUCCESS on commit
        logger.info("wrote %s", new_state)
        spark.stop()
    finally:
        lock.release()


def _self_check() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "_LOCK")
        first = LeaseLock(path, ttl_seconds=30, heartbeat_seconds=5)
        first.acquire()
        second = LeaseLock(path, ttl_seconds=30, heartbeat_seconds=5)
        try:
            second.acquire()
            raise AssertionError("fresh lock must not be taken over")
        except LockHeldError:
            pass
        # Simulate a crash: stop heartbeating and age the lock beyond the TTL.
        first._stop.set()
        first._thread.join()
        old = time.time() - 120
        os.utime(path, (old, old))
        second.acquire()
        assert second.current_owner() == second.owner
        first.release()  # crashed owner coming back must NOT delete the new lock
        assert os.path.exists(path)
        second.release()
        assert not os.path.exists(path)
    print("self-test OK")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--base")
    parser.add_argument("--date")
    parser.add_argument("--first-run", action="store_true")
    parser.add_argument("--lock-ttl", type=int, default=3600,
                        help="seconds without heartbeat after which the lock is stale")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return _self_check()
    if not args.base or not args.date:
        parser.error("--base and --date are required")
    run_job(args.base, args.date, args.first_run, args.lock_ttl)


if __name__ == "__main__":
    main()

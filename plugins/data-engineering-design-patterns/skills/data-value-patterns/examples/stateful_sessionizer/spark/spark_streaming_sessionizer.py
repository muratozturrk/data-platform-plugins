"""Stateful Sessionizer -- PySpark Structured Streaming.

Visits arrive in Kafka within seconds. The Spark state store plays the role of the
Incremental Sessionizer's pending-sessions storage: it keeps every in-flight
session (one per visit_id) between micro-batches and is checkpointed to durable
storage. Per micro-batch and key:

  1. create the session, or resume it from the state store;
  2. accumulate the new visits (pages ordered by event time, replays deduplicated);
  3. if the state timed out, emit the completed session and remove the state;
     otherwise update the state and move the event-time timeout forward
     (base watermark + gap). Optionally emit the partial session too
     (is_completed=false) for consumers that need freshness.

Expiration base = the current watermark, falling back to the batch's max event
time in the first micro-batch (watermark == 0). This closes sessions as soon as
the job progresses past them.

Two modes:
  --mode arbitrary       applyInPandasWithState (default, flexible gap logic)
  --mode session-window  session_window() aggregation (low-code alternative)

flatMapGroupsWithState/mapGroupsWithState are Scala/Java only; PySpark uses
applyInPandasWithState (Spark 3.4+).

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
      spark_streaming_sessionizer.py --mode arbitrary
  python spark_streaming_sessionizer.py --self-test      # pure logic, no Spark needed
Env: KAFKA_BOOTSTRAP, VISITS_TOPIC, SESSIONS_PATH, CHECKPOINT_DIR, SESSION_GAP_MINUTES,
     WATERMARK_DELAY, EMIT_PARTIAL_SESSIONS. Prereqs: pyspark>=3.4, pandas, pyarrow, delta-spark.
"""
from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("stateful_sessionizer")


@dataclass(frozen=True)
class SessionizerConfig:
    kafka_bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    visits_topic: str = os.environ.get("VISITS_TOPIC", "visits")
    sessions_path: str = os.environ.get("SESSIONS_PATH", "/data/gold/sessions")
    checkpoint_dir: str = os.environ.get("CHECKPOINT_DIR", "/data/checkpoints/stateful_sessionizer")
    gap_ms: int = int(os.environ.get("SESSION_GAP_MINUTES", "10")) * 60 * 1000
    watermark_delay: str = os.environ.get("WATERMARK_DELAY", "1 minute")
    emit_partial_sessions: bool = os.environ.get("EMIT_PARTIAL_SESSIONS", "false").lower() == "true"
    trigger_interval: str = os.environ.get("TRIGGER_INTERVAL", "30 seconds")


CONFIG = SessionizerConfig()

# Kept as DDL strings so the module imports without pyspark (self-test).
EVENT_DDL = "visit_id STRING, user_id STRING, page STRING, event_time TIMESTAMP"
PAGES_DDL = "ARRAY<STRUCT<page: STRING, event_time_as_ms: BIGINT>>"
OUTPUT_DDL = (f"session_id STRING, visit_id STRING, user_id STRING, start_time TIMESTAMP, "
              f"end_time TIMESTAMP, visited_pages {PAGES_DDL}, duration_in_milliseconds BIGINT, "
              f"is_completed BOOLEAN")
STATE_DDL = f"visits {PAGES_DDL}, user_id STRING"


# -- Pure session logic (no Spark imports) ------------------------------------
def to_epoch_ms(series: pd.Series) -> list[int]:
    """Timestamp column -> epoch milliseconds, independent of pandas' datetime unit."""
    return ((pd.to_datetime(series) - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)).tolist()


def merge_visits(visits_so_far: list, new_visits: list) -> list[tuple[str, int]]:
    """Accumulation: union, dedupe replayed (page, time) pairs, order by event time.

    Checkpointing gives at-least-once, so a restarted batch can feed the same
    events twice; the dedupe keeps the session idempotent.
    """
    unique = {(v[0], int(v[1])) for v in list(visits_so_far) + list(new_visits)}
    return sorted(unique, key=lambda v: (v[1], v[0] or ""))


def build_session(visit_id: str, user_id: Optional[str], visits: list, is_completed: bool) -> dict:
    """Finalization: in-flight state -> public output record.

    session_id is deterministic (visit id + first event time); never wall-clock,
    so a replay after a restart produces the same key.
    """
    ordered = merge_visits([], visits)
    start_ms, end_ms = ordered[0][1], ordered[-1][1]
    return {
        "session_id": f"{visit_id}-{start_ms}",
        "visit_id": visit_id,
        "user_id": user_id,
        "start_time": pd.Timestamp(start_ms, unit="ms"),
        "end_time": pd.Timestamp(end_ms, unit="ms"),
        "visited_pages": [{"page": p, "event_time_as_ms": t} for p, t in ordered],
        "duration_in_milliseconds": end_ms - start_ms,
        "is_completed": is_completed,
    }


def expiration_base_ms(watermark_ms: int, batch_max_event_ms: int) -> int:
    """No watermark yet in the first micro-batch -> use event time."""
    return batch_max_event_ms if watermark_ms == 0 else watermark_ms


def make_state_function(gap_ms: int, emit_partial: bool):
    """Returns the applyInPandasWithState function (map_visits_to_session)."""

    def map_visits_to_session(key: Any, input_rows: Iterable[pd.DataFrame],
                              current_state) -> Iterator[pd.DataFrame]:
        visit_id = key[0]

        if current_state.hasTimedOut:
            visits, user_id = current_state.get
            current_state.remove()
            yield pd.DataFrame([build_session(visit_id, user_id, list(visits), is_completed=True)])
            return

        watermark_ms = current_state.getCurrentWatermarkMs()
        batch_max_ms, user_id, new_visits = 0, None, []
        for df in input_rows:
            if df.empty:
                continue
            times = to_epoch_ms(df["event_time"])
            batch_max_ms = max([batch_max_ms] + times)
            known_users = df["user_id"].dropna()
            if user_id is None and not known_users.empty:
                user_id = known_users.iloc[0]
            new_visits += list(zip(df["page"].tolist(), times))

        visits_so_far = []
        if current_state.exists:
            visits_so_far, stored_user = current_state.get
            user_id = stored_user or user_id
        visits_for_state = merge_visits(visits_so_far, new_visits)
        if not visits_for_state:
            return

        current_state.update((visits_for_state, user_id))
        # ponytail: unbounded pages per visit; cap or summarize the array if bots create huge visits.
        current_state.setTimeoutTimestamp(expiration_base_ms(watermark_ms, batch_max_ms) + gap_ms)

        if emit_partial:
            yield pd.DataFrame([build_session(visit_id, user_id, visits_for_state, is_completed=False)])

    return map_visits_to_session


# -- Spark wiring ---------------------------------------------------------------
def build_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("stateful-sessionizer")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "16")   # = number of state store partitions; fixed after first run
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .getOrCreate())


def read_visits(spark, cfg: SessionizerConfig):
    from pyspark.sql import functions as F
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
            .option("subscribe", cfg.visits_topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(F.from_json(F.col("value").cast("string"), EVENT_DDL).alias("v")).select("v.*")
            .filter("visit_id IS NOT NULL AND event_time IS NOT NULL"))


def sessions_arbitrary(visits, cfg: SessionizerConfig):
    from pyspark.sql.streaming.state import GroupStateTimeout
    return (visits.withWatermark("event_time", cfg.watermark_delay)
            .groupBy("visit_id")
            .applyInPandasWithState(make_state_function(cfg.gap_ms, cfg.emit_partial_sessions),
                                    outputStructType=OUTPUT_DDL, stateStructType=STATE_DDL,
                                    outputMode="update", timeoutConf=GroupStateTimeout.EventTimeTimeout))


def sessions_window(visits, cfg: SessionizerConfig):
    """Session window: gap duration per key, closed by the watermark (append mode)."""
    from pyspark.sql import functions as F
    gap = f"{cfg.gap_ms // 60000} minutes"
    return (visits.withWatermark("event_time", cfg.watermark_delay)
            .groupBy(F.session_window("event_time", gap).alias("w"), "visit_id")
            .agg(F.first("user_id", ignorenulls=True).alias("user_id"),
                 F.min("event_time").alias("start_time"), F.max("event_time").alias("end_time"),
                 F.array_sort(F.collect_set(F.struct(F.expr("unix_millis(event_time)").alias("t"), "page")))
                  .alias("ordered"))
            .select(F.concat_ws("-", "visit_id", F.expr("unix_millis(start_time)").cast("string")).alias("session_id"),
                    "visit_id", "user_id", "start_time", "end_time",
                    F.transform("ordered", lambda x: F.struct(x["page"].alias("page"),
                                                              x["t"].alias("event_time_as_ms")))
                     .alias("visited_pages"),
                    (F.expr("unix_millis(end_time)") - F.expr("unix_millis(start_time)")).alias("duration_in_milliseconds"),
                    F.lit(True).alias("is_completed")))


def make_writer(cfg: SessionizerConfig):
    """Idempotent sink: MERGE on session_id, so replayed micro-batches and
    partial->completed transitions overwrite instead of duplicating."""

    def write_batch(batch_df, batch_id: int) -> None:
        from delta.tables import DeltaTable
        spark = batch_df.sparkSession
        batch_df = batch_df.dropDuplicates(["session_id", "is_completed"])
        if batch_df.isEmpty():
            return
        if not DeltaTable.isDeltaTable(spark, cfg.sessions_path):
            batch_df.write.format("delta").mode("append").save(cfg.sessions_path)
        else:
            (DeltaTable.forPath(spark, cfg.sessions_path).alias("t")
             .merge(batch_df.alias("s"), "t.session_id = s.session_id")
             .whenMatchedUpdateAll(condition="t.is_completed = false")   # never downgrade a final session
             .whenNotMatchedInsertAll()
             .execute())
        log.info("batch=%d sessions=%d completed=%d", batch_id, batch_df.count(),
                 batch_df.filter("is_completed").count())

    return write_batch


def run(mode: str, cfg: SessionizerConfig) -> None:
    spark = build_spark()
    visits = read_visits(spark, cfg)
    sessions = sessions_arbitrary(visits, cfg) if mode == "arbitrary" else sessions_window(visits, cfg)
    query = (sessions.writeStream
             .outputMode("update" if mode == "arbitrary" else "append")
             .foreachBatch(make_writer(cfg))
             .option("checkpointLocation", f"{cfg.checkpoint_dir}/{mode}")
             .trigger(processingTime=cfg.trigger_interval)
             .start())
    log.info("Stateful sessionizer started | mode=%s gap_ms=%d watermark=%s partial=%s",
             mode, cfg.gap_ms, cfg.watermark_delay, cfg.emit_partial_sessions)
    query.awaitTermination()


# -- Self-check -------------------------------------------------------------------
class _FakeState:
    """Minimal GroupState stand-in for testing the state function without Spark."""

    def __init__(self, watermark_ms=0, value=None, timed_out=False):
        self.watermark_ms, self.value, self.hasTimedOut, self.timeout = watermark_ms, value, timed_out, None

    exists = property(lambda self: self.value is not None)
    get = property(lambda self: self.value)

    def update(self, v): self.value = v
    def remove(self): self.value = None
    def setTimeoutTimestamp(self, ts): self.timeout = ts
    def getCurrentWatermarkMs(self): return self.watermark_ms


def _self_check() -> None:
    fn = make_state_function(gap_ms=10 * 60_000, emit_partial=False)
    t0 = pd.Timestamp("2024-01-01 10:00:00")
    batch = pd.DataFrame({"visit_id": ["A", "A"], "user_id": ["u1", "u1"], "page": ["home", "p1"],
                          "event_time": [t0, t0 + pd.Timedelta(minutes=1)]})
    t0_ms = to_epoch_ms(pd.Series([t0]))[0]

    # 1st micro-batch: no watermark -> base = max event time (10:01) + 10 min.
    st = _FakeState(watermark_ms=0)
    assert list(fn(("A",), [batch], st)) == []
    assert st.timeout == t0_ms + 60_000 + 600_000 and len(st.value[0]) == 2

    # Replay of the same events (at-least-once) does not duplicate pages; base = watermark now.
    st.watermark_ms = t0_ms + 7 * 60_000
    assert list(fn(("A",), [batch], st)) == [] and len(st.value[0]) == 2
    assert st.timeout == st.watermark_ms + 600_000

    # Timeout -> completed session emitted, state removed, deterministic id.
    st.hasTimedOut = True
    out = list(fn(("A",), [], st))[0].iloc[0]
    assert out["is_completed"] and not st.exists
    assert out["session_id"] == f"A-{t0_ms}" and out["duration_in_milliseconds"] == 60_000
    assert [p["page"] for p in out["visited_pages"]] == ["home", "p1"]

    # Partial emission mode flags the in-flight session.
    partial = list(make_state_function(600_000, True)(("A",), [batch], _FakeState()))[0].iloc[0]
    assert not partial["is_completed"]
    assert expiration_base_ms(0, 5) == 5 and expiration_base_ms(9, 5) == 9
    print("self-check OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["arbitrary", "session-window"], default="arbitrary")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_check()
    else:
        run(args.mode, CONFIG)

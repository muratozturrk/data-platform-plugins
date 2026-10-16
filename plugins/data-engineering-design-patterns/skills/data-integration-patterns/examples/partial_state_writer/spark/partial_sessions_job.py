"""Partial State Writer - emit in-progress sessions with is_final (PySpark applyInPandasWithState).

Sessions last hours, but the fraud team needs them now. The stateful function:
  * on new input   -> updates the session and emits it with is_final = False
                      every EMIT_EVERY new visits (delivery frequency is a business choice);
  * on state timeout (30 min inactivity) -> emits the closed session with is_final = True.
This is the Spark 3.4+ equivalent of Spark 4's transformWithStateInPandas, where
handleInputRows emits partial results and handleExpiredTimer final ones.

Writing semantics: partial updates are UPSERTED with MERGE on session_id (a plain append
would leave many versions per session); a final row is never replaced by a partial one.

Run:  spark-submit --packages io.delta:delta-spark_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 partial_sessions_job.py
Env:  KAFKA_BOOTSTRAP, SESSIONS_TABLE_PATH
"""
import os
from typing import Iterator

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TABLE_PATH = os.environ.get("SESSIONS_TABLE_PATH", "/tmp/dp/sessions")
EMIT_EVERY = 5
INACTIVITY_MS = 30 * 60 * 1000
OUTPUT_SCHEMA = "session_id STRING, user_id STRING, start_ms LONG, end_ms LONG, visits LONG, is_final BOOLEAN"
STATE_SCHEMA = "session_key LONG, start_ms LONG, end_ms LONG, visits LONG, last_emitted LONG"


def session_row(user_id: str, session_key: int, start: int, end: int, visits: int,
                is_final: bool) -> pd.DataFrame:
    # session_key = first event time seen; stable even if a late event moves start_ms earlier,
    # so every partial version MERGEs into the same row.
    return pd.DataFrame({"session_id": [f"{user_id}-{session_key}"], "user_id": [user_id], "start_ms": [start],
                         "end_ms": [end], "visits": [visits], "is_final": [is_final]})


def update_session(key: tuple, batches: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    (user_id,) = key
    if state.hasTimedOut:
        session_key, start, end, visits, _ = state.get
        state.remove()
        yield session_row(user_id, session_key, start, end, visits, is_final=True)
        return
    session_key, start, end, visits, last_emitted = state.get if state.exists else (None, None, None, 0, 0)
    for pdf in batches:
        if session_key is None:
            session_key = int(pdf.event_time_ms.iloc[0])
        start = int(pdf.event_time_ms.min()) if start is None else min(start, int(pdf.event_time_ms.min()))
        end = int(pdf.event_time_ms.max()) if end is None else max(end, int(pdf.event_time_ms.max()))
        visits += len(pdf)
    if visits - last_emitted >= EMIT_EVERY or last_emitted == 0:
        last_emitted = visits
        yield session_row(user_id, session_key, start, end, visits, is_final=False)
    state.update((session_key, start, end, visits, last_emitted))
    state.setTimeoutDuration(INACTIVITY_MS)


def upsert_sessions(spark: SparkSession):
    def write(batch: DataFrame, batch_id: int) -> None:
        if not DeltaTable.isDeltaTable(spark, TABLE_PATH):
            batch.limit(0).write.format("delta").save(TABLE_PATH)
        (DeltaTable.forPath(spark, TABLE_PATH).alias("t")
         .merge(batch.alias("s"), "t.session_id = s.session_id")
         .whenMatchedUpdateAll(condition="t.is_final = false")
         .whenNotMatchedInsertAll().execute())
    return write


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("partial_sessions")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    visits = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", "visits").load()
              .select(F.from_json(F.col("value").cast("string"), "user_id STRING, event_time_ms LONG").alias("v"))
              .select("v.*"))
    sessions = visits.groupBy("user_id").applyInPandasWithState(
        update_session, outputStructType=OUTPUT_SCHEMA, stateStructType=STATE_SCHEMA,
        outputMode="update", timeoutConf=GroupStateTimeout.ProcessingTimeTimeout)
    (sessions.writeStream.outputMode("update").foreachBatch(upsert_sessions(spark))
     .option("checkpointLocation", f"{TABLE_PATH}/_checkpoint").start().awaitTermination())

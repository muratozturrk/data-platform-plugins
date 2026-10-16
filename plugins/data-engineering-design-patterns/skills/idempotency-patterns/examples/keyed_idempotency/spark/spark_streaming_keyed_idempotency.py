"""
Keyed Idempotency -- Spark Structured Streaming sessions with append-time keys.

Visits from Kafka are grouped per user into sessions. The session key is derived
from IMMUTABLE attributes only: (user_id, earliest broker append time). The
Kafka `timestamp` column is the append time when the topic uses
`message.timestamp.type=LogAppendTime` (a producer-set
CreateTime is less reliable). Event time is mutable for key generation: a late
event (09:55 arriving after 09:56) would change "the first event" after a restart
and create a second session. Append time only grows, so a
restarted job recomputes the same key and the key-based store overwrites the row.

Sink: ScyllaDB/Cassandra table -- writes are upserts by
PRIMARY KEY(session_id, user_id), so replays rewrite, never duplicate:

    CREATE TABLE dp.sessions (
      session_id BIGINT, user_id BIGINT, pages LIST<TEXT>,
      ingestion_time TIMESTAMP,
      PRIMARY KEY(session_id, user_id));

Why not `hash(str(min_append_time))`: Python's
built-in hash() of str is salted per process (PYTHONHASHSEED), so every
executor restart would generate different session IDs -- the opposite of
idempotent. `session_id()` below uses SHA-256 truncated to 63 bits instead.

Kinesis variant: only the input changes,
    spark.readStream.format("kinesis")...load()
      .selectExpr("CAST(data AS STRING) AS value",
                  "approximateArrivalTimestamp AS timestamp")

Run (Spark 3.4+ for applyInPandasWithState):
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    spark_streaming_keyed_idempotency.py
Env: KAFKA_BROKERS, VISITS_TOPIC, CASSANDRA_HOST, CASSANDRA_USER,
     CASSANDRA_PASSWORD, CHECKPOINT_PATH, SESSION_GAP_SECONDS (default 50).
Self-test (no Spark needed):  python spark_streaming_keyed_idempotency.py --self-test
"""
from __future__ import annotations

import hashlib
import logging
import os
import sys
from typing import Iterable, Iterator, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("keyed_idempotency_sessions")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
VISITS_TOPIC = os.environ.get("VISITS_TOPIC", "visits")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/tmp/dp/keyed/checkpoint")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "dp")
CASSANDRA_TABLE = os.environ.get("CASSANDRA_TABLE", "sessions")
SESSION_GAP_MS = int(os.environ.get("SESSION_GAP_SECONDS", "50")) * 1000

# State = (min_append_time_ms, pages, max_event_time_ms)
STATE_SCHEMA = "min_append_time LONG, pages ARRAY<STRING>, max_event_time LONG"
OUTPUT_SCHEMA = ("session_id LONG, user_id LONG, pages ARRAY<STRING>, "
                 "ingestion_time TIMESTAMP")


# -- Pure logic (unit-testable without Spark) -------------------------------
def session_id(user_id: int, min_append_time_ms: int) -> int:
    """Deterministic 63-bit session key from immutable attributes.

    Stable across processes, executors and restarts (unlike built-in hash()).
    63 bits keeps it a positive BIGINT for Cassandra/ScyllaDB.
    """
    digest = hashlib.sha256(f"{user_id}|{min_append_time_ms}".encode()).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def merge_state(state: Optional[tuple], batch_min_append_ms: int,
                batch_pages: list[str], batch_max_event_ms: int) -> tuple:
    """Accumulates a micro-batch into the session state.

    The append time in a Kafka partition only increases, so the first value is
    normally kept. min() still protects against multi-partition
    topics where a user's records are spread across partitions.
    """
    if state is None:
        return (batch_min_append_ms, list(batch_pages), batch_max_event_ms)
    min_append, pages, max_event = state
    return (min(min_append, batch_min_append_ms),
            list(pages) + list(batch_pages),
            max(max_event, batch_max_event_ms))


def session_row(user_id: int, state: tuple) -> dict:
    min_append, pages, _ = state
    return {"session_id": session_id(user_id, min_append), "user_id": user_id,
            "pages": list(pages), "ingestion_time_ms": min_append}


# -- Stateful mapping function -----------------------------------------------
def map_visits_to_session(key: tuple, rows: Iterable, state) -> Iterator:
    """applyInPandasWithState function (event-time timeout).

    On timeout: emit the final session keyed by (user_id, min append time).
    Otherwise: accumulate pages and push the timeout to max_event_time + gap.
    """
    import pandas as pd

    user_id = int(key[0])
    if state.hasTimedOut:
        current = state.get
        state.remove()
        row = session_row(user_id, (current[0], list(current[1]), current[2]))
        yield pd.DataFrame([{
            "session_id": row["session_id"], "user_id": row["user_id"],
            "pages": row["pages"],
            "ingestion_time": pd.Timestamp(row["ingestion_time_ms"], unit="ms"),
        }])
        return

    current = state.get if state.exists else None
    if current is not None:
        current = (current[0], list(current[1]), current[2])
    for pdf in rows:
        if pdf.empty:
            continue
        pdf = pdf.sort_values(["append_time", "event_time_ms"])
        current = merge_state(current, int(pdf["append_time"].min()),
                              pdf["page"].astype(str).tolist(),
                              int(pdf["event_time_ms"].max()))
    if current is not None:
        state.update(current)
        state.setTimeoutTimestamp(current[2] + SESSION_GAP_MS)
    yield pd.DataFrame(columns=["session_id", "user_id", "pages", "ingestion_time"])


# -- Sink ---------------------------------------------------------------------
def write_sessions(batch_df, batch_id: int) -> None:
    """Append into Cassandra/ScyllaDB = upsert by primary key.

    If the job fails after this write but before the checkpoint commit, the
    batch is replayed and writes the same (session_id, user_id) rows again.
    """
    (batch_df.write.format("org.apache.spark.sql.cassandra")
     .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE)
     .mode("append").save())
    log.info("batch %d written to %s.%s", batch_id, CASSANDRA_KEYSPACE, CASSANDRA_TABLE)


def main() -> None:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout

    builder = (SparkSession.builder.appName("keyed-idempotency-sessions")
               .config("spark.cassandra.connection.host",
                       os.environ.get("CASSANDRA_HOST", "localhost")))
    if os.environ.get("CASSANDRA_USER"):
        builder = (builder
                   .config("spark.cassandra.auth.username", os.environ["CASSANDRA_USER"])
                   .config("spark.cassandra.auth.password", os.environ["CASSANDRA_PASSWORD"]))
    spark = builder.getOrCreate()

    input_data = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                  .option("subscribe", VISITS_TOPIC)
                  .option("startingOffsets", "earliest")
                  .load())

    # Value + broker timestamp (= append time with LogAppendTime).
    visits = (input_data.selectExpr("CAST(value AS STRING) AS value", "timestamp")
              .select(F.from_json(F.col("value"),
                                  "user_id LONG, page STRING, event_time TIMESTAMP")
                      .alias("visit"), F.col("timestamp"))
              .select("visit.user_id", "visit.page", "visit.event_time",
                      F.expr("unix_millis(timestamp)").alias("append_time"))
              .filter(F.col("user_id").isNotNull() & F.col("event_time").isNotNull())
              .withWatermark("event_time", "10 seconds"))

    sessions = (visits
                .withColumn("event_time_ms", F.expr("unix_millis(event_time)"))
                .groupBy(F.col("user_id"))
                .applyInPandasWithState(map_visits_to_session,
                                        outputStructType=OUTPUT_SCHEMA,
                                        stateStructType=STATE_SCHEMA,
                                        outputMode="append",
                                        timeoutConf=GroupStateTimeout.EventTimeTimeout))

    query = (sessions.writeStream
             .foreachBatch(write_sessions)
             .option("checkpointLocation", CHECKPOINT_PATH)
             .outputMode("append")
             .start())
    log.info("streaming sessions from %s -> %s.%s", VISITS_TOPIC,
             CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
    query.awaitTermination()


def _self_check() -> None:
    # Same immutable input -> same key, in any process.
    assert session_id(1, 1_700_000_000_000) == session_id(1, 1_700_000_000_000)
    assert session_id(1, 1_700_000_000_000) != session_id(2, 1_700_000_000_000)
    assert 0 <= session_id(10**12, 2**40) < 2**63
    # A late event (earlier event time, later append time) must not
    # change the key.
    s = merge_state(None, 1000, ["home"], 57_000)      # appended 09:57
    s = merge_state(s, 2000, ["cart"], 55_000)          # late event, appended later
    assert s[0] == 1000 and s[1] == ["home", "cart"] and s[2] == 57_000
    replayed = merge_state(merge_state(None, 1000, ["home"], 57_000), 2000, ["cart"], 55_000)
    assert session_row(1, s)["session_id"] == session_row(1, replayed)["session_id"]
    # Records from another partition with an earlier append time win.
    assert merge_state(s, 500, ["x"], 1)[0] == 500
    print("spark_streaming_keyed_idempotency self-check OK")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        _self_check()
    else:
        main()

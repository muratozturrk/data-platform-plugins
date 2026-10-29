"""Spark Structured Streaming against Polaris. Typical examples end with
query.awaitTermination(), which never returns; here every query is bounded (processAllAvailable or
a fixed run time) and stopped. Console sinks except where a result has to be counted."""
import shutil
import time

import pytest
from pyspark.sql.functions import current_timestamp, lit

from conftest import jerr, record

WRITE_SNIPPET = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

input_stream = (
    spark.readStream
        .format("rate")
        .option("rowsPerSecond", 10)
        .load()
        .withColumn("name", lit("Simulated Name"))
        .withColumn("event_date", current_timestamp())
)
"""


def rate_stream(spark):
    return (spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            .withColumn("name", lit("Simulated Name"))
            .withColumn("event_date", current_timestamp()))


def run_for(query, seconds):
    deadline = time.time() + seconds
    while time.time() < deadline and query.isActive:
        time.sleep(1)
    error = query.exception()
    query.stop()
    return error


def read_events(spark, **options):
    reader = spark.readStream.format("iceberg").option("stream-from-timestamp", "1640995200000")
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load("polaris.db.events")


def test_streaming_read_fails_on_overwrite_snapshots(spark, checks, measure):
    """polaris.db.events carries the MERGE (overwrite) and DELETE snapshots of dataframe/."""
    query = (read_events(spark).select("id", "name", "event_date")
             .writeStream.format("console").outputMode("append").start())
    with pytest.raises(Exception) as err:
        query.processAllAvailable()
    query.stop()
    assert "overwrite" in str(err.value).lower()
    record(checks, "Streaming read: readStream ... stream-from-timestamp ... console", "fails",
           "fails on a table with MERGE/DELETE history", "add the skip options of the next snippet")
    measure("read_error", jerr(err.value))


def test_streaming_read_with_skip_options(spark, checks, measure):
    df = read_events(spark, **{"streaming-skip-overwrite-snapshots": "true",
                               "streaming-skip-delete-snapshots": "true"})
    query = df.select("id", "name", "event_date").writeStream.format("memory").queryName("events_feed") \
        .outputMode("append").start()
    query.processAllAvailable()
    query.stop()
    got = sorted(r.id for r in spark.sql("SELECT id FROM events_feed").collect())
    # Only append snapshots are replayed: the first create (1, 2, 3) and the later append (5, 6).
    # The MERGE insert of id 4 lives in an overwrite snapshot and is skipped.
    assert got == [1, 2, 3, 5, 6]
    record(checks, "Streaming read with streaming-skip-overwrite/delete-snapshots", "ok", "as-is",
           "rows added by MERGE (id 4) are silently skipped, deleted rows (id 1) are replayed")
    measure("skip_options_rows", got)


def test_write_snippet_misses_lit_import(spark, checks):
    with pytest.raises(NameError, match="lit"):
        exec(WRITE_SNIPPET, {"spark": spark})
    record(checks, "Streaming write: rate source snippet", "fails", "NameError",
           "import lit (only current_timestamp is imported)")


def test_streaming_write_into_events_has_incompatible_schema(spark, checks, measure):
    shutil.rmtree("/tmp/checkpoints", ignore_errors=True)
    with pytest.raises(Exception) as err:
        query = (rate_stream(spark)
                 .selectExpr("value AS id", "name", "event_date")
                 .writeStream
                 .format("iceberg")
                 .outputMode("append")
                 .option("checkpointLocation", "/tmp/checkpoints/events")
                 .toTable("polaris.db.events"))
        query.processAllAvailable()
    assert "event_date" in str(err.value)
    record(checks, "Streaming write: .toTable('polaris.db.events')", "fails",
           "TIMESTAMP into the STRING event_date of the DataFrame section",
           "write to a table whose event_date is TIMESTAMP")
    measure("write_error", jerr(err.value))


def test_streaming_write_to_a_new_table(spark, measure):
    """The same query into a table that does not exist yet: Spark 3.5 toTable creates it."""
    query = (rate_stream(spark).selectExpr("value AS id", "name", "event_date")
             .writeStream.format("iceberg").outputMode("append").trigger(processingTime="2 seconds")
             .option("checkpointLocation", "/tmp/checkpoints/stream_events")
             .toTable("polaris.db.stream_events"))
    assert run_for(query, 25) is None
    rows = spark.table("polaris.db.stream_events").count()
    files = spark.table("polaris.db.stream_events.files").count()
    snaps = spark.table("polaris.db.stream_events.snapshots").count()
    assert rows > 0 and files >= snaps
    measure("stream_write", {"rows": rows, "data_files": files, "snapshots": snaps,
                                 "avg_rows_per_file": round(rows / files, 1)})


def test_fanout_snippet_creates_an_unpartitioned_table(spark, checks, measure):
    query = (rate_stream(spark)
             .selectExpr("value AS id", "name", "event_date")
             .writeStream
             .format("iceberg")
             .outputMode("append")
             .option("fanout-enabled", "true")
             .option("checkpointLocation", "/tmp/checkpoints/partitioned_events")
             .toTable("polaris.db.partitioned_events"))
    assert run_for(query, 10) is None
    partition_cols = spark.table("polaris.db.partitioned_events.partitions").columns
    assert "partition" not in partition_cols
    record(checks, "Streaming write with fanout-enabled to polaris.db.partitioned_events", "ok",
           "runs, but toTable creates the table unpartitioned", "create the table with PARTITIONED BY first")
    measure("fanout_table_partition_columns", partition_cols)


@pytest.mark.parametrize("fanout", [False, True])
def test_fanout_on_a_really_partitioned_table(spark, fanout, measure):
    table = f"polaris.db.bucketed_{'fanout' if fanout else 'plain'}"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, name STRING, event_date TIMESTAMP) USING iceberg "
              "PARTITIONED BY (bucket(8, id))")
    writer = (rate_stream(spark).selectExpr("value AS id", "name", "event_date")
              .writeStream.format("iceberg").outputMode("append").trigger(processingTime="2 seconds")
              .option("checkpointLocation", f"/tmp/checkpoints/{table}"))
    if fanout:
        writer = writer.option("fanout-enabled", "true")
    error = run_for(writer.toTable(table), 12)
    # A common claim: partitioned streaming writes need sorted input or fanout. Iceberg 1.5.2 on
    # Spark 3.5 asks Spark for a hash distribution on micro-batches too, so both variants succeed.
    assert error is None, jerr(error)
    rows = spark.table(table).count()
    files = spark.table(f"{table}.files").count()
    snaps = spark.table(f"{table}.snapshots").count()
    assert rows > 0 and files <= 8 * snaps            # at most one file per bucket per commit
    measure(f"bucketed_{'fanout' if fanout else 'no_fanout'}",
            {"rows": rows, "data_files": files, "snapshots": snaps})

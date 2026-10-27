"""Reliable Kafka -> Iceberg ingestion: delivery guarantees, idempotency,
fail-fast vs controlled casting vs dead-letter queue, and schema drift handling.

Patterns follow the Spark Structured Streaming guide (foreachBatch, checkpoints) and the
Iceberg Spark streaming docs: https://iceberg.apache.org/docs/latest/spark-structured-streaming/
"""
import re
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F

from common.lakehouse import read_kafka

PAYMENT_DDL = ("event_id BIGINT, account_id BIGINT, amount DECIMAL(12,2), currency STRING, "
               "event_ts TIMESTAMP, channel STRING")
REQUIRED = ("event_id", "account_id", "amount", "event_ts")   # fail-fast fields
_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,63}$")


def payments(n: int, start_id: int = 0) -> list[dict]:
    t0 = datetime(2025, 1, 10)
    return [{"event_id": i, "account_id": i % 97, "amount": f"{(i % 500) + 0.99:.2f}",
             "currency": "EUR", "event_ts": (t0 + timedelta(seconds=i)).isoformat(),
             "channel": ["web", "app", "pos"][i % 3]}
            for i in range(start_id, start_id + n)]


def create_tables(spark: SparkSession, target: str, dlq: str | None = None) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({PAYMENT_DDL}) USING iceberg "
              f"PARTITIONED BY (days(event_ts))")
    if dlq:
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {dlq} (
            topic STRING, kafka_partition INT, kafka_offset BIGINT, raw_value STRING,
            error STRING, epoch_id BIGINT) USING iceberg""")


# --- 1. Native Iceberg sink: exactly-once through checkpoint + snapshot summary ------------
def start_native_sink(spark, topic, target, checkpoint, **kafka_opts):
    parsed = (read_kafka(spark, topic, **kafka_opts)
              .select(F.from_json(F.col("value").cast("string"), PAYMENT_DDL).alias("d"))
              .select("d.*"))
    return (parsed.writeStream.format("iceberg").outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(target))


# --- 2. foreachBatch sinks: the guarantee is whatever the batch function makes it ----------
def plain_append_handler(target):
    """At-least-once: a replayed batch is appended a second time."""
    def handle(batch: DataFrame, epoch_id: int) -> None:
        (batch.select(F.from_json(F.col("value").cast("string"), PAYMENT_DDL).alias("d"))
         .select("d.*").writeTo(target).append())
    return handle


def table_columns(spark, table) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema.fields]


def validate(batch: DataFrame, columns, required=REQUIRED) -> DataFrame:
    """Parse every payload as map<string,string>, then cast to the table schema.

    Required fields fail (missing or not castable). Optional fields use controlled casting:
    try_cast turns a bad value into NULL instead of failing the record.
    """
    m = F.col("m")
    df = batch.select("topic", F.col("partition").alias("kafka_partition"),
                      F.col("offset").alias("kafka_offset"),
                      F.col("value").cast("string").alias("raw_value"))
    df = df.withColumn("m", F.from_json("raw_value", "map<string,string>"))
    checks = [F.when(m.isNull(), F.lit("malformed json"))]
    typed = []
    for name, dtype in columns:
        raw = m.getItem(name)
        value = F.expr(f"try_cast(m['{name}'] AS {dtype})")
        typed.append(value.alias(name))
        if name in required:
            checks.append(F.when(m.isNotNull() & raw.isNull(), F.lit(f"missing {name}")))
            checks.append(F.when(raw.isNotNull() & value.isNull(),
                                 F.concat(F.lit(f"invalid {name}: "), raw)))
    error = F.concat_ws("; ", *checks)
    return df.select("*", *typed).withColumn("error", F.when(error != "", error))


def new_fields(batch_checked: DataFrame, known: set[str]) -> list[str]:
    keys = (batch_checked.where("m IS NOT NULL").select(F.explode(F.map_keys("m")).alias("k"))
            .distinct().collect())
    return sorted(r.k for r in keys if r.k not in known)


def reliable_handler(target, dlq, mode="dlq", evolve_schema=True, metrics=None):
    """Idempotent consumer with validation, DLQ and schema drift handling.

    mode="dlq":       invalid records go to the DLQ table, the stream keeps running.
    mode="fail_fast": any invalid record fails the micro-batch; nothing is committed.
    """
    assert mode in ("dlq", "fail_fast")

    def handle(batch: DataFrame, epoch_id: int) -> None:
        spark = batch.sparkSession
        batch = batch.persist()
        columns = table_columns(spark, target)
        drift = new_fields(validate(batch, columns), {n for n, _ in columns})
        added = [f for f in drift if _IDENT.match(f)]
        if added and evolve_schema:
            # New source fields land as STRING; a later, deliberate ALTER can widen the type.
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS ("
                      + ", ".join(f"`{f}` STRING" for f in added) + ")")
            columns = table_columns(spark, target)
        checked = validate(batch, columns).persist()
        bad = checked.where("error IS NOT NULL")
        n_bad = bad.count()
        if n_bad and mode == "fail_fast":
            first = bad.select("error").first().error
            raise ValueError(f"epoch {epoch_id}: {n_bad} invalid records, first: {first}")

        names = [n for n, _ in columns]
        good = checked.where("error IS NULL").select(*names).dropDuplicates(["event_id"])
        good.createOrReplaceTempView("incoming_payments")
        # Idempotent consumer: a replayed batch matches existing keys and inserts nothing.
        spark.sql(f"""MERGE INTO {target} t USING incoming_payments s ON t.event_id = s.event_id
                      WHEN NOT MATCHED THEN INSERT *""")
        if n_bad:
            (bad.select("topic", "kafka_partition", "kafka_offset", "raw_value", "error",
                        F.lit(epoch_id).cast("bigint").alias("epoch_id"))
             .createOrReplaceTempView("incoming_rejects"))
            spark.sql(f"""MERGE INTO {dlq} d USING incoming_rejects s
                          ON d.topic = s.topic AND d.kafka_partition = s.kafka_partition
                             AND d.kafka_offset = s.kafka_offset
                          WHEN NOT MATCHED THEN INSERT *""")
        if metrics is not None:
            metrics.append({"epoch_id": epoch_id, "records": checked.count(), "rejected": n_bad,
                            "new_fields": added if evolve_schema else [],
                            "ignored_fields": [] if evolve_schema else drift})
        checked.unpersist()
        batch.unpersist()
    return handle


def start_foreach_batch(spark, topic, handler, checkpoint, **kafka_opts):
    return (read_kafka(spark, topic, **kafka_opts).writeStream
            .foreachBatch(handler)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start())

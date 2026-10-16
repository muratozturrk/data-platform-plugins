"""Wrapper -- keep the raw record untouched and put computed fields next to it.

Visits come from several providers; consumers want derived values in one
place, operators want the original values for debugging. The envelope holds
the raw part plus computed attributes (business ones such as is_connected,
technical ones such as job_version in processing_context).

Layouts for tables (--layout):
  computed_nested  1: raw columns flat, computed ones in a `decorated` struct
  raw_nested       2: computed columns flat, raw row in a `raw` struct
  flat             3: everything flat, names prefixed raw_* / computed_*
  two_tables       4: raw table + computed table, joined by a deterministic wrap_id
Streaming (--stream): Kafka value rewritten as JSON {raw_data, processing_context}
, batch_version = micro-batch id.

Idempotency: batch output is a Delta table partitioned by event_date and
overwritten with replaceWhere for the processed date, so a rerun replaces
its own partition only. The job writes a metrics JSON (row count + checksum
of the input rows and of the raw part re-read from the output) that
airflow/wrapper_dag.py uses to prove the raw part was not modified.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_record_envelope_wrapper.py \
      --input /data/bronze/visits/date=2024-06-01 --output /data/silver/visits_wrapped \
      --event-date 2024-06-01 --layout raw_nested --job-version v1.0.3 --batch-version 42 \
      --metrics-file /data/metrics/wrapper/2024-06-01.json [--engine sql]
  spark-submit --packages ...kafka... spark_record_envelope_wrapper.py --stream
"""
import argparse
import json
import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("wrapper")

LAYOUTS = ("computed_nested", "raw_nested", "flat", "two_tables")

# Implementation 1 in SQL -- raw columns + NAMED_STRUCT of computed values
SQL_COMPUTED_NESTED = """
SELECT *, NAMED_STRUCT(
  'is_connected', CASE WHEN context.user.connected_since IS NULL THEN false ELSE true END,
  'page_referral_key', CONCAT_WS('-', page, context.referral)
) AS decorated
FROM input_visits
"""
# Implementation 2 in SQL -- computed values first-class, raw row in a struct
SQL_RAW_NESTED = """
SELECT
  CASE WHEN context.user.connected_since IS NULL THEN false ELSE true END AS is_connected,
  CONCAT_WS('-', page, context.referral) AS page_referral_key,
  STRUCT(*) AS raw
FROM input_visits
"""


def computed_columns() -> dict:
    """Business attributes derived from the raw record. One definition for every layout."""
    return {
        "is_connected": F.col("context.user.connected_since").isNotNull(),
        "page_referral_key": F.concat_ws("-", F.col("page"), F.col("context.referral")),
    }


def processing_context(job_version: str, batch_version: str) -> Column:
    """Technical attributes from the execution context."""
    return F.struct(F.lit(job_version).alias("job_version"),
                    F.lit(batch_version).alias("batch_version"),
                    F.current_timestamp().alias("processing_time"))


def wrap(visits: DataFrame, layout: str, ctx: Column) -> dict:
    """Return {table_suffix: DataFrame}; two_tables yields two outputs."""
    raw_cols = visits.columns
    computed = computed_columns()
    if layout == "computed_nested":
        decorated = F.struct(*[c.alias(n) for n, c in computed.items()])
        return {"": visits.withColumn("decorated", decorated).withColumn("processing_context", ctx)}
    if layout == "raw_nested":
        return {"": visits.select(*[c.alias(n) for n, c in computed.items()],
                                  F.struct(*raw_cols).alias("raw"), ctx.alias("processing_context"))}
    if layout == "flat":
        # prefixes are the only thing telling consumers what is raw
        return {"": visits.select(*[F.col(c).alias(f"raw_{c}") for c in raw_cols],
                                  *[c.alias(f"computed_{n}") for n, c in computed.items()],
                                  ctx.alias("computed_processing_context"))}
    if layout == "two_tables":
        keyed = visits.withColumn("wrap_id", F.sha2(F.to_json(F.struct(*raw_cols)), 256))
        return {"_raw": keyed.select("wrap_id", *raw_cols),
                "_computed": keyed.select("wrap_id", *[c.alias(n) for n, c in computed.items()],
                                          ctx.alias("processing_context"))}
    raise ValueError(f"unknown layout {layout!r}, expected one of {LAYOUTS}")


def wrap_sql(spark: SparkSession, visits: DataFrame, layout: str, ctx: Column) -> dict:
    visits.createOrReplaceTempView("input_visits")
    queries = {"computed_nested": SQL_COMPUTED_NESTED, "raw_nested": SQL_RAW_NESTED}
    if layout not in queries:
        raise ValueError("--engine sql supports computed_nested and raw_nested only")
    return {"": spark.sql(queries[layout]).withColumn("processing_context", ctx)}


def raw_part(df: DataFrame, layout: str, raw_cols: list) -> DataFrame:
    """Rebuild the original rows from a wrapped output (for the integrity check)."""
    if layout == "computed_nested":
        return df.select(*raw_cols)
    if layout == "raw_nested":
        return df.select("raw.*")
    if layout == "flat":
        return df.select(*[F.col(f"raw_{c}").alias(c) for c in raw_cols])
    return df.select(*raw_cols)  # two_tables: the _raw table


def fingerprint(df: DataFrame, cols: list) -> dict:
    """Row count + order-independent checksum; decimal sum avoids long overflow."""
    row = df.select(F.count("*").alias("rows"), F.sum(
        F.xxhash64(F.to_json(F.struct(*cols))).cast("decimal(38,0)")).alias("checksum")).first()
    return {"rows": row["rows"], "checksum": str(row["checksum"] or 0)}


def write_partition(df: DataFrame, path: str, event_date: str) -> None:
    (df.withColumn("event_date", F.lit(event_date).cast("date"))
       .write.format("delta").mode("overwrite")
       .option("replaceWhere", f"event_date = '{event_date}'")
       .partitionBy("event_date").save(path))


def run_batch(spark: SparkSession, a: argparse.Namespace) -> None:
    visits = spark.read.json(a.input)
    if visits.rdd.isEmpty():
        raise ValueError(f"no input rows in {a.input}")
    raw_cols = visits.columns
    ctx = processing_context(a.job_version, a.batch_version)
    outputs = wrap_sql(spark, visits, a.layout, ctx) if a.engine == "sql" else wrap(visits, a.layout, ctx)
    for suffix, df in outputs.items():
        write_partition(df, a.output + suffix, a.event_date)
        log.info("wrote %s%s for %s (layout=%s)", a.output, suffix, a.event_date, a.layout)

    raw_path = a.output + ("_raw" if a.layout == "two_tables" else "")
    written = spark.read.format("delta").load(raw_path).filter(F.col("event_date") == a.event_date)
    metrics = {"event_date": a.event_date, "layout": a.layout, "job_version": a.job_version,
               "batch_version": a.batch_version,
               "input": fingerprint(visits, raw_cols),
               "raw_output": fingerprint(raw_part(written, a.layout, raw_cols), raw_cols)}
    os.makedirs(os.path.dirname(a.metrics_file) or ".", exist_ok=True)
    with open(a.metrics_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f)
    log.info("metrics: %s", metrics)


def wrap_kafka_value(kafka_df: DataFrame, job_version: str, batch_version: int) -> DataFrame:
    """The raw value travels unchanged inside the new JSON value."""
    return kafka_df.select("key", F.to_json(F.struct(
        F.col("value").cast("string").alias("raw_data"),
        processing_context(job_version, str(batch_version)).alias("processing_context"))).alias("value"))


def run_stream(spark: SparkSession, a: argparse.Namespace) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Kafka sink is not transactional: a replayed batch_id re-sends -> consumers dedup
        (wrap_kafka_value(batch_df, a.job_version, batch_id).write.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("topic", os.environ.get("OUTPUT_TOPIC", "visits-wrapped")).save())

    (spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", os.environ.get("INPUT_TOPIC", "visits")).load()
        .writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", os.environ.get("CHECKPOINT_DIR", "/data/checkpoints/visits_wrapped"))
        .start().awaitTermination())


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--stream", action="store_true")
    p.add_argument("--input")
    p.add_argument("--output")
    p.add_argument("--event-date")
    p.add_argument("--metrics-file")
    p.add_argument("--layout", choices=LAYOUTS, default="computed_nested")
    p.add_argument("--engine", choices=("dataframe", "sql"), default="dataframe")
    p.add_argument("--job-version", default=os.environ.get("JOB_VERSION", "dev"))
    p.add_argument("--batch-version", default="0")
    a = p.parse_args()
    if not a.stream and not all([a.input, a.output, a.event_date, a.metrics_file]):
        p.error("batch mode needs --input, --output, --event-date and --metrics-file")
    spark = (SparkSession.builder.appName("wrapper")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    run_stream(spark, a) if a.stream else run_batch(spark, a)


if __name__ == "__main__":
    main()

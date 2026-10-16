"""Local Sequencer -- PySpark (data processing layer).

The job is a sequence of named steps where each step's output DataFrame is the next
step's input (read -> validate -> enrich -> write). A *restart boundary* is placed after
the most expensive step: the costly intermediate dataset is materialized once, so a
failure in the final transformation is retried without recomputing it.

Run:
    spark-submit local_sequencer_job.py --input /tmp/dp/visits --work /tmp/dp/work \
        --output /tmp/dp/visits_enriched --stage all|intermediate|final
`--stage final` replays only the last step from the materialized intermediate dataset
(what an orchestrator task split would give you for free).
Requires: pyspark>=3.3.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_input(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(
        "visit_id STRING, user_id STRING, page STRING, event_time TIMESTAMP"
    ).json(path)


def validate(visits: DataFrame) -> DataFrame:
    return visits.filter("visit_id IS NOT NULL AND event_time IS NOT NULL").dropDuplicates(
        ["visit_id", "event_time"]
    )


def build_sessions(visits: DataFrame) -> DataFrame:
    # Expensive step (shuffle + aggregation) -> the restart boundary goes right after it.
    return visits.groupBy("visit_id", "user_id").agg(
        F.min("event_time").alias("start_time"),
        F.max("event_time").alias("end_time"),
        F.collect_list("page").alias("pages"),
    )


def enrich(sessions: DataFrame) -> DataFrame:
    return sessions.withColumn(
        "duration_seconds", F.col("end_time").cast("long") - F.col("start_time").cast("long")
    ).withColumn("pages_count", F.size("pages"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--work", required=True, help="private location for the intermediate dataset")
    parser.add_argument("--output", required=True)
    parser.add_argument("--stage", choices=["all", "intermediate", "final"], default="all")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("local-sequencer").getOrCreate()
    intermediate_path = f"{args.work}/sessions"

    if args.stage in ("all", "intermediate"):
        input_dataset = read_input(spark, args.input)
        valid_dataset = validate(input_dataset)
        sessions = build_sessions(valid_dataset)
        sessions.write.mode("overwrite").parquet(intermediate_path)

    if args.stage in ("all", "final"):
        sessions = spark.read.parquet(intermediate_path)
        enrich(sessions).write.mode("overwrite").parquet(args.output)

    spark.stop()


if __name__ == "__main__":
    main()

"""Fine-Grained Tracker - row-level lineage in Kafka headers (Spark Structured Streaming).

Two jobs:
  visits_decorator_job: visits -> visits-decorated, headers = job_name, job_version, batch_version
  visits_reducer_job  : visits-decorated -> visits-reduced, same headers + parent_lineage
                        (the upstream headers serialized as JSON)
Any row in visits-reduced can be traced to the job/version/micro-batch that produced
it and to its upstream producer. Column-level lineage comes for free from the
OpenLineage listener (see ../../dataset_tracker/spark) via the columnLineage facet.

Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 row_level_lineage.py decorator|reducer
Env:  KAFKA_BOOTSTRAP, JOB_VERSION (e.g. git sha)
"""
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
JOB_VERSION = os.environ.get("JOB_VERSION", "v1.0.0")


def lineage_headers(job_name: str, batch_number: int) -> list:
    return [
        F.struct(F.lit("job_version").alias("key"), F.lit(JOB_VERSION).cast("binary").alias("value")),
        F.struct(F.lit("job_name").alias("key"), F.lit(job_name).cast("binary").alias("value")),
        F.struct(F.lit("batch_version").alias("key"), F.lit(str(batch_number)).cast("binary").alias("value")),
    ]


def writer(job_name: str, output_topic: str, with_parent: bool):
    def write(batch: DataFrame, batch_number: int) -> None:
        headers = lineage_headers(job_name, batch_number)
        if with_parent:
            headers.append(F.struct(F.lit("parent_lineage").alias("key"),
                                    F.to_json(F.col("headers")).cast("binary").alias("value")))
        (batch.withColumn("headers", F.array(*headers)).select("key", "value", "headers")
         .write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
         .option("topic", output_topic).save())
    return write


def run(job: str) -> None:
    spark = SparkSession.builder.appName(f"visits_{job}_job").getOrCreate()
    input_topic, output_topic = {"decorator": ("visits", "visits-decorated"),
                                 "reducer": ("visits-decorated", "visits-reduced")}[job]
    source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", input_topic).option("includeHeaders", "true").load())
    # ... decoration / reduction logic is irrelevant for lineage ...
    (source.writeStream.foreachBatch(writer(f"visits_{job}_job", output_topic, with_parent=job == "reducer"))
     .option("checkpointLocation", f"/tmp/dp/checkpoints/visits_{job}").start().awaitTermination())


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "decorator")

"""Vertical Partitioner (data removal) - PySpark Structured Streaming + Delta Lake.

Splits each visit event into two vertical parts:
  * visits without the PII user context  -> topic `visits_no_pii` (key = visit_id)
  * the immutable user context (PII)     -> topic `user_context`  (key = user_id)
A second query MERGEs `user_context` into a Delta table so each user's PII is
stored exactly once. A removal request then deletes ONE row (+ VACUUM) instead
of rewriting every visit.

Run:
  export KAFKA_BOOTSTRAP=localhost:9092 USERS_TABLE=/tmp/dp/users CHECKPOINT_DIR=/tmp/dp/chk
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
      vertical_partitioner_job.py split|merge|delete <user_id>
"""
import os
import sys

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
USERS_TABLE = os.environ.get("USERS_TABLE", "/tmp/dp/users")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/dp/chk")

VISIT_SCHEMA = """visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
  context STRUCT<referral: STRING, user: STRUCT<login: STRING, birthday: DATE, personal_id: STRING>>"""

spark = configure_spark_with_delta_pip(
    SparkSession.builder.appName("vertical-partitioner")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()


def write_topic(df: DataFrame, topic: str) -> None:
    df.write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", topic).save()


def split_visit_attributes(visits: DataFrame, batch_id: int) -> None:
    visits.persist()  # read once, project twice
    without_user = (visits.filter("user_id IS NOT NULL AND context.user.login IS NOT NULL")
                    .withColumn("context", F.col("context").dropFields("user"))
                    .select(F.col("visit_id").alias("key"), F.to_json(F.struct("*")).alias("value")))
    write_topic(without_user, "visits_no_pii")
    user_context = (visits.selectExpr("context.user.*", "user_id")
                    .select(F.col("user_id").alias("key"), F.to_json(F.struct("*")).alias("value")))
    write_topic(user_context, "user_context")
    visits.unpersist()


def save_most_recent_user_context(context: DataFrame, batch_id: int) -> None:
    deduplicated = context.dropDuplicates(["user_id"]).alias("new")
    if not DeltaTable.isDeltaTable(spark, USERS_TABLE):
        deduplicated.write.format("delta").save(USERS_TABLE)
        return
    (DeltaTable.forPath(spark, USERS_TABLE).alias("current")
     .merge(deduplicated, "current.user_id = new.user_id")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def read_topic(topic: str) -> DataFrame:
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic).option("startingOffsets", "earliest").load())


def run_split() -> None:
    visits = read_topic("visits_raw").select(F.from_json(F.col("value").cast("string"), VISIT_SCHEMA).alias("v")).select("v.*")
    (visits.writeStream.foreachBatch(split_visit_attributes)
     .option("checkpointLocation", f"{CHECKPOINT_DIR}/split").start().awaitTermination())


def run_merge() -> None:
    schema = "login STRING, birthday DATE, personal_id STRING, user_id STRING"
    users = read_topic("user_context").select(F.from_json(F.col("value").cast("string"), schema).alias("u")).select("u.*")
    (users.writeStream.foreachBatch(save_most_recent_user_context)
     .option("checkpointLocation", f"{CHECKPOINT_DIR}/merge").start().awaitTermination())


def delete_user(user_id: str) -> None:
    table = DeltaTable.forPath(spark, USERS_TABLE)
    table.delete(F.col("user_id") == user_id)  # column expr, no string interpolation
    # Without VACUUM the PII is still readable through time travel.
    # Retention must be <= your legal removal delay; 0h needs the safety check disabled.
    retention_hours = float(os.environ.get("VACUUM_RETENTION_HOURS", "168"))
    table.vacuum(retention_hours)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "split"
    if mode == "split":
        run_split()
    elif mode == "merge":
        run_merge()
    elif mode == "delete":
        delete_user(sys.argv[2])
    else:
        raise SystemExit(f"unknown mode {mode}")

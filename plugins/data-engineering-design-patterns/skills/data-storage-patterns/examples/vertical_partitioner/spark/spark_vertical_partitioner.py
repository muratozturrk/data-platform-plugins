"""Vertical Partitioner - PySpark + Delta Lake.

Splits each visit row into three tables joined back by `visit_id`:
  * visits     - mutable, per-event attributes (event_time, page)
  * users      - immutable user context (written once per visit, deduplicated)
  * technical  - immutable technical context (browser, device)
`persist()` avoids reading the input twice for the three writes.

Run:
    pip install pyspark delta-spark
    python spark_vertical_partitioner.py /tmp/dp/vertical
"""
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

VISIT_SCHEMA = """visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
  context STRUCT<referral: STRING,
                 user: STRUCT<ip: STRING, login: STRING>,
                 technical: STRUCT<browser: STRING, browser_version: STRING, device_type: STRING>>"""


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("vertical-partitioner")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main(base_dir: str) -> None:
    spark = build_spark()
    raw = [
        ("v1", "2024-07-01 09:00:00", "u1", "home", ("google", ("1.1.1.1", "ann"), ("firefox", "120", "pc"))),
        ("v1", "2024-07-01 09:01:00", "u1", "blog", ("google", ("1.1.1.1", "ann"), ("firefox", "120", "pc"))),
        ("v2", "2024-07-01 09:05:00", "u2", "home", (None, ("2.2.2.2", "bob"), ("safari", "17", "mobile"))),
    ]
    visits = (spark.createDataFrame(raw, VISIT_SCHEMA.replace("TIMESTAMP", "STRING"))
              .withColumn("event_time", F.to_timestamp("event_time")))
    visits.persist()

    # Mutable part: one row per event, immutable attributes removed.
    (visits.drop("user_id")
     .withColumn("context", F.col("context").dropFields("user", "technical"))
     .write.mode("overwrite").format("delta").save(f"{base_dir}/visits"))

    # Immutable parts: stored once per visit_id.
    (visits.selectExpr("visit_id", "user_id", "context.user.*").dropDuplicates(["visit_id"])
     .write.mode("overwrite").format("delta").save(f"{base_dir}/users"))
    (visits.selectExpr("visit_id", "context.technical.*").dropDuplicates(["visit_id"])
     .write.mode("overwrite").format("delta").save(f"{base_dir}/technical"))
    visits.unpersist()

    # Consumers recombine via the shared key (or expose this as a view / Dataset Materializer).
    def load(name):
        return spark.read.format("delta").load(f"{base_dir}/{name}")
    full = load("visits").join(load("users"), "visit_id").join(load("technical"), "visit_id")
    full.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dp/vertical")

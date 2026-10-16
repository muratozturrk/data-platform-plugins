"""Constraints Enforcer - Delta Lake NOT NULL + CHECK constraints, PySpark.

Creates a Delta table with type + nullability constraints, adds a value constraint
(event_time never in the future), appends a valid batch, then shows that a batch
with one bad row is rejected entirely (all-or-nothing). Finally shows the optional
pre-filter: evaluate the same predicate in the job and dispatch violators.

Run: spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_delta_constraints.py
Env: WAREHOUSE_DIR (default /tmp/dp/warehouse).
"""
import datetime as dt
import os

from pyspark.sql import SparkSession, functions as F

WAREHOUSE = os.environ.get("WAREHOUSE_DIR", "/tmp/dp/warehouse")
spark = (SparkSession.builder.appName("constraints-enforcer")
         .config("spark.sql.warehouse.dir", WAREHOUSE)
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

spark.sql("""
CREATE TABLE IF NOT EXISTS default.visits (
  visit_id STRING NOT NULL,
  event_time TIMESTAMP NOT NULL,
  page STRING
) USING delta
""")
existing = {r["key"] for r in spark.sql("SHOW TBLPROPERTIES default.visits").collect()}
if "delta.constraints.event_time_not_in_the_future" not in existing:
    spark.sql("""ALTER TABLE default.visits ADD CONSTRAINT event_time_not_in_the_future
                 CHECK (event_time < NOW() + INTERVAL '1 SECOND')""")

now = dt.datetime.now()
valid = spark.createDataFrame([("v1", now - dt.timedelta(minutes=5), "home.html")],
                              "visit_id STRING, event_time TIMESTAMP, page STRING")
valid.writeTo("default.visits").append()

bad = spark.createDataFrame([("v2", now - dt.timedelta(minutes=1), "a.html"),
                             ("v3", now + dt.timedelta(days=1), "future.html"),  # breaks CHECK
                             (None, now, "no_id.html")],                          # breaks NOT NULL
                            "visit_id STRING, event_time TIMESTAMP, page STRING")
try:
    bad.writeTo("default.visits").append()
except Exception as error:  # DELTA_VIOLATE_CONSTRAINT_WITH_VALUES / DELTA_NOT_NULL_CONSTRAINT_VIOLATED
    print(f"Rejected whole batch, v2 not written either: {str(error).splitlines()[0]}")

# Optional: same rules evaluated in the job to dispatch violators instead of failing everything.
rule = F.col("visit_id").isNotNull() & F.col("event_time").isNotNull() & (F.col("event_time") < F.current_timestamp())
bad.filter(rule).writeTo("default.visits").append()
bad.filter(~F.coalesce(rule, F.lit(False))).write.mode("append").format("delta").save(f"{WAREHOUSE}/visits_rejected")

spark.table("default.visits").show(truncate=False)

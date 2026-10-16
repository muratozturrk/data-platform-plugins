"""Schema Compatibility Enforcer - implicit enforcement with Delta Lake, PySpark.

Delta Lake validates every write against the table schema. A producer adding an
unexpected `ad_id` column (or changing a type) fails with
`AnalysisException: A schema mismatch detected when writing to the Delta table`.
Accepting the evolution is an explicit, reviewed opt-in via `mergeSchema`.

Run: spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_delta_schema_enforcement.py
Env: TABLE_DIR (default /tmp/dp/visits_schema_enforced).
"""
import os

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

TABLE_DIR = os.environ.get("TABLE_DIR", "/tmp/dp/visits_schema_enforced")
spark = (SparkSession.builder.appName("schema-compatibility-enforcer")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         # keep automatic evolution OFF so enforcement is the default
         .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
         .getOrCreate())

initial = spark.createDataFrame([("v1", "home", 1700000000)], "visit_id STRING, page STRING, event_time LONG")
initial.write.format("delta").mode("overwrite").save(TABLE_DIR)

evolved = spark.createDataFrame([("v2", "about", 1700000100, "ad-7")],
                                "visit_id STRING, page STRING, event_time LONG, ad_id STRING")
try:
    evolved.write.format("delta").mode("append").save(TABLE_DIR)
except AnalysisException as error:
    print("Rejected by implicit enforcement:", str(error).splitlines()[0])

retyped = spark.createDataFrame([("v3", "contact", "2024-01-01T00:00:00")],
                                "visit_id STRING, page STRING, event_time STRING")
try:
    retyped.write.format("delta").mode("append").save(TABLE_DIR)
except AnalysisException as error:
    print("Type change rejected:", str(error).splitlines()[0])

# Explicit, reviewed opt-in for an additive (forward-compatible) change only.
evolved.write.format("delta").mode("append").option("mergeSchema", "true").save(TABLE_DIR)
spark.read.format("delta").load(TABLE_DIR).printSchema()

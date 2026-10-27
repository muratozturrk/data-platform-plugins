
from pyspark.sql import SparkSession

def get_test_spark_session() -> SparkSession:
    """
    Lightweight Spark session for testing.
    - master("local[2]") → 2 threads, no cluster needed
    - adaptiveQueryExecution disabled → deterministic plans for testing
    """
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("PropertyBasedTesting")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")  # small for tests
        .config("spark.ui.enabled", "false")           # no web UI noise
        .getOrCreate()
    )
"""Dataset Tracker - OpenLineage listener on a PySpark job.

The OpenLineageSparkListener inspects each executed plan and sends START/COMPLETE
run events with input and output datasets to the lineage backend (Marquez here).
The job joins `orders` with `users` into `orders_with_users`; Marquez then shows
orders + users -> job -> orders_with_users. The same events carry the
columnLineage facet used by the Fine-Grained Tracker.

Run:
    docker run -p 5000:5000 -p 3000:3000 marquezproject/marquez   # or the Marquez compose file
    spark-submit openlineage_spark_job.py
Env:  OPENLINEAGE_URL (default http://localhost:5000), OPENLINEAGE_NAMESPACE (team/domain name)
Deps: pyspark 3.5 (Scala 2.12), io.openlineage:openlineage-spark (pulled via spark.jars.packages)
"""
import os

from pyspark.sql import SparkSession


def create_spark_session_with_open_lineage(app_name: str) -> SparkSession:
    return (SparkSession.builder.master("local[*]").appName(app_name)
            .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
            .config("spark.openlineage.transport.type", "http")
            .config("spark.openlineage.transport.url", os.environ.get("OPENLINEAGE_URL", "http://localhost:5000"))
            # Namespace per team/domain makes ownership visible in the dependency graph.
            .config("spark.openlineage.namespace", os.environ.get("OPENLINEAGE_NAMESPACE", "orders_team"))
            .config("spark.jars.packages", "io.openlineage:openlineage-spark_2.12:1.21.1")
            .getOrCreate())


def main() -> None:
    spark = create_spark_session_with_open_lineage("orders_with_users_builder")
    base = "/tmp/dp/lineage"
    spark.createDataFrame([(1, 10, 99.5), (2, 11, 15.0)], "order_id INT, user_id INT, amount DOUBLE") \
        .write.mode("overwrite").parquet(f"{base}/orders")
    spark.createDataFrame([(10, "Ana"), (11, "Bob")], "id INT, first_name STRING") \
        .write.mode("overwrite").parquet(f"{base}/users")

    orders = spark.read.parquet(f"{base}/orders").alias("o")
    users = spark.read.parquet(f"{base}/users").alias("u")
    (orders.join(users, users.id == orders.user_id)
     .select("o.order_id", "o.amount", "u.first_name")
     .write.mode("overwrite").parquet(f"{base}/orders_with_users"))
    spark.stop()  # flushes the last lineage events


if __name__ == "__main__":
    main()

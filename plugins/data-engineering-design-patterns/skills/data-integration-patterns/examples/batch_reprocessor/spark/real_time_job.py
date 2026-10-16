"""Batch Reprocessor - the real-time job using the shared logic (Kafka -> Kafka).

Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        --py-files visits_logic.py real_time_job.py
Env:  KAFKA_BOOTSTRAP
"""
import os

from pyspark.sql import SparkSession

from visits_logic import extract_columns, to_kafka_records

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("visits_real_time").getOrCreate()
    input_data = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
                  .option("subscribe", "visits").load())
    records = to_kafka_records(extract_columns(input_data), origin="real_time")
    (records.writeStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
     .option("topic", "visits-cleaned").option("checkpointLocation", "/tmp/dp/checkpoints/visits_cleaned")
     .start().awaitTermination())

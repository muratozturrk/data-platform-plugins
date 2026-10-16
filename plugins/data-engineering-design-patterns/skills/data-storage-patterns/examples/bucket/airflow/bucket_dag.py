"""Bucket - Airflow 2.x TaskFlow orchestrating bucketed tables.

1. Registers (once) an Athena external table bucketed by user_id in Spark format
   (Athena applies bucketing at read time only - it cannot INSERT INTO a bucketed table).
2. Daily, submits the Spark job that writes the bucketed files (spark_bucket.py style).
3. Guards the bucketing contract: fails if the table's bucket count drifted
   (a mismatch silently disables pruning/shuffle elimination for joins).

Run: put in dags/; requires apache-airflow-providers-amazon and
apache-airflow-providers-apache-spark. AWS credentials come from the
`aws_default` connection / environment, never from code.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BUCKETS = 50
OUTPUT_LOCATION = "s3://dp-athena-results/"

ATHENA_DDL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS dp.visits_bucketed (
  visit_id STRING, user_id STRING, page STRING, event_time TIMESTAMP)
CLUSTERED BY (`user_id`) INTO {BUCKETS} BUCKETS
STORED AS PARQUET
LOCATION 's3://dp-lake/visits_bucketed/'
TBLPROPERTIES ('bucketing_format' = 'spark')
"""


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     tags=["dp", "storage", "bucket"])
def bucket():
    register_table = AthenaOperator(task_id="register_bucketed_table", query=ATHENA_DDL,
                                    database="dp", output_location=OUTPUT_LOCATION)

    write_buckets = SparkSubmitOperator(
        task_id="write_bucketed_files",
        application="/opt/jobs/spark_bucket.py",
        application_args=["--buckets", str(BUCKETS), "--date", "{{ ds }}"],
        conn_id="spark_default")

    @task
    def check_bucket_contract():
        import boto3
        table = boto3.client("glue").get_table(DatabaseName="dp", Name="visits_bucketed")["Table"]
        actual = table["StorageDescriptor"].get("NumberOfBuckets")
        if actual != BUCKETS:
            raise ValueError(f"Bucket count drift: expected {BUCKETS}, got {actual}. "
                             "Changing it requires a full backfill.")

    register_table >> write_buckets >> check_bucket_contract()


bucket()

# compactor_dag.py
#
# Schedules and orchestrates the Delta Lake Compactor.
# Runs nightly after all ingestion jobs complete.
# Responsibilities:
#   1. Run OPTIMIZE on Bronze and Silver tables
#   2. Run VACUUM to reclaim storage (weekly only — not every night)
#   3. Collect and log file stats before and after
#   4. Alert if any table's avg file size remains unhealthy after compaction
#
# Requirements:
#   pip install apache-airflow apache-airflow-providers-apache-spark

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    "owner":           "data-engineering",
    "retries":         1,
    "retry_delay":     timedelta(minutes=10),
    "email_on_failure": True,
}

TABLES = [
    {"name": "orders_bronze",   "path": "/data/raw/orders"},
    {"name": "orders_silver",   "path": "/data/silver/orders"},
    {"name": "customers_silver","path": "/data/silver/customers"},
]

# Alert if avg file size after compaction is still below this
HEALTHY_AVG_MB_THRESHOLD = 100


def should_run_vacuum(**context):
    """
    VACUUM is expensive and only needed weekly.
    Run it on Sundays (weekday=6).
    """
    if context["execution_date"].weekday() == 6:
        return "run_vacuum"
    return "skip_vacuum"


def check_post_compaction_health(**context):
    """
    Queries Delta table stats after compaction and alerts if any table
    still has an unhealthy average file size.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("CompactionHealthCheck") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    unhealthy = []
    for table in TABLES:
        detail = spark.sql(
            f"DESCRIBE DETAIL delta.`{table['path']}`"
        ).collect()[0]

        num_files  = detail["numFiles"]
        size_bytes = detail["sizeInBytes"]
        avg_mb     = (size_bytes / num_files / 1024 / 1024) \
                     if num_files > 0 else 0

        print(f"{table['name']}: files={num_files}, avg={avg_mb:.1f} MB")

        if avg_mb < HEALTHY_AVG_MB_THRESHOLD:
            unhealthy.append(
                f"{table['name']}: avg={avg_mb:.1f} MB "
                f"(threshold={HEALTHY_AVG_MB_THRESHOLD} MB)"
            )

    spark.stop()

    if unhealthy:
        raise ValueError(
            f"Post-compaction health check failed for:\n"
            + "\n".join(unhealthy)
        )

    print("All tables healthy after compaction.")


def log_compaction_audit(**context):
    """Writes compaction run metadata to the audit table."""
    hook = PostgresHook(postgres_conn_id="target_dwh")
    hook.run("""
        CREATE TABLE IF NOT EXISTS compaction_audit_log (
            id           SERIAL PRIMARY KEY,
            dag_id       TEXT,
            run_id       TEXT,
            ran_vacuum   BOOLEAN,
            completed_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    ran_vacuum = (context["execution_date"].weekday() == 6)
    hook.run("""
        INSERT INTO compaction_audit_log (dag_id, run_id, ran_vacuum)
        VALUES (%s, %s, %s)
    """, parameters=(context["dag"].dag_id, context["run_id"], ran_vacuum))
    print("Compaction audit record written.")


with DAG(
    dag_id="compactor_delta_tables",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # Run nightly at 04:00 — after all ingestion jobs (01:00) and
    # transformation jobs (02:00) have completed
    schedule_interval="0 4 * * *",
    catchup=False,
    tags=["compaction", "maintenance", "delta"],
) as dag:

    t1 = SparkSubmitOperator(
        task_id="run_optimize",
        application="spark_compactor_delta_optimize.py",
        conn_id="spark_default",
        packages="io.delta:delta-core_2.12:2.4.0",
        conf={
            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    )

    t2 = BranchPythonOperator(
        task_id="should_run_vacuum",
        python_callable=should_run_vacuum,
    )

    t3 = SparkSubmitOperator(
        task_id="run_vacuum",
        application="spark_compactor_delta_optimize.py",
        application_args=["--vacuum"],
        conn_id="spark_default",
        packages="io.delta:delta-core_2.12:2.4.0",
    )

    t4 = EmptyOperator(
        task_id="skip_vacuum",
    )

    t5 = PythonOperator(
        task_id="check_post_compaction_health",
        python_callable=check_post_compaction_health,
        trigger_rule="none_failed_min_one_success",
    )

    t6 = PythonOperator(
        task_id="log_compaction_audit",
        python_callable=log_compaction_audit,
    )

    t1 >> t2 >> [t3, t4] >> t5 >> t6
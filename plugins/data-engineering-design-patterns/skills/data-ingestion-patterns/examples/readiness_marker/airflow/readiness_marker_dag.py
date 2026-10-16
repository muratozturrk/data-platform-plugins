# readiness_marker_dag.py
#
# Demonstrates the full Readiness Marker lifecycle in Airflow:
#   Producer DAG  — writes data + marker after successful completion
#   Consumer DAG  — uses sensors to wait for marker before processing
#
# Two DAGs in one file for clarity. In production, split into separate files.
#
# Requirements:
#   pip install apache-airflow apache-airflow-providers-apache-spark
#   pip install apache-airflow-providers-postgres

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import json

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
}


# ── Custom DB Readiness Sensor ─────────────────────────────────────────────────

class DatasetReadinessSensor(BaseSensorOperator):
    """
    Custom Airflow sensor that polls the dataset_readiness DB table
    and returns True when the expected dataset+date marker is found.

    Also validates that:
      - row_count > 0 (non-empty dataset)
      - schema_hash matches expected hash (detects upstream schema drift)
    """

    template_fields = ("partition_date",)   # renders "{{ ds }}"; without it the sensor polls the literal

    def __init__(
        self,
        dataset:              str,
        partition_date:       str,
        postgres_conn_id:     str = "pipeline_state_db",
        expected_min_rows:    int = 1,
        expected_schema_hash: str = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dataset              = dataset
        self.partition_date       = partition_date
        self.postgres_conn_id     = postgres_conn_id
        self.expected_min_rows    = expected_min_rows
        self.expected_schema_hash = expected_schema_hash

    def poke(self, context) -> bool:
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        row = hook.get_first("""
            SELECT row_count, schema_hash, written_at
            FROM dataset_readiness
            WHERE dataset = %s
              AND partition_date = %s
        """, parameters=(self.dataset, self.partition_date))

        if row is None:
            self.log.info(
                "No marker yet for %s/%s — waiting...",
                self.dataset, self.partition_date
            )
            return False

        row_count, schema_hash, written_at = row

        # Validate row count
        if row_count < self.expected_min_rows:
            raise ValueError(
                f"Marker found but row_count={row_count} < "
                f"expected_min_rows={self.expected_min_rows}. "
                f"Upstream data may be empty."
            )

        # Validate schema hash if expected hash is provided
        if self.expected_schema_hash and schema_hash != self.expected_schema_hash:
            raise ValueError(
                f"Schema drift detected for {self.dataset}! "
                f"Expected hash={self.expected_schema_hash}, "
                f"got hash={schema_hash}. "
                f"Review upstream schema changes before proceeding."
            )

        self.log.info(
            "Marker found | dataset=%s | date=%s | rows=%d | written_at=%s",
            self.dataset, self.partition_date, row_count, written_at,
        )
        return True


# ── Producer DAG ───────────────────────────────────────────────────────────────
# Runs the Silver transformation and writes the readiness marker on success.

def write_readiness_marker_task(**context):
    """
    Called as the final task of the producer DAG.
    Writes the DB readiness marker after the Spark job confirms success.
    """
    hook = PostgresHook(postgres_conn_id="pipeline_state_db")
    ds   = context["ds"]

    # Read row count from the Silver table (written by Spark job)
    silver_count = hook.get_first(f"""
        SELECT COUNT(*) FROM delta.`/data/silver/orders`
        WHERE DATE(updated_at) = '{ds}'
    """)[0]

    # Upsert readiness marker
    hook.run("""
        INSERT INTO dataset_readiness
            (dataset, partition_date, batch_id, row_count, schema_hash)
        VALUES
            ('orders_silver', %s, %s, %s, 'placeholder_hash')
        ON CONFLICT (dataset, partition_date)
        DO UPDATE SET
            row_count  = EXCLUDED.row_count,
            batch_id   = EXCLUDED.batch_id,
            written_at = NOW()
    """, parameters=(ds, context["run_id"], silver_count))

    print(f"Readiness marker written: orders_silver/{ds}, rows={silver_count}")


with DAG(
    dag_id="producer_silver_orders",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["producer", "silver", "readiness"],
) as producer_dag:

    run_spark = SparkSubmitOperator(
        task_id     = "run_transformation_spark",
        application = "orders_silver_job.py",   # your processing job
        application_args = ["--date", "{{ ds }}"],
        conn_id     = "spark_default",
        packages    = "io.delta:delta-core_2.12:2.4.0",
    )

    write_marker = PythonOperator(
        task_id         = "write_readiness_marker",
        python_callable = write_readiness_marker_task,
    )

    # Marker is written ONLY after Spark job succeeds
    run_spark >> write_marker


# ── Consumer DAG ───────────────────────────────────────────────────────────────
# Waits for the Silver readiness marker before running the Gold aggregation.

def run_gold_aggregation(**context):
    """Placeholder for the Gold layer aggregation logic."""
    print(f"Running Gold aggregation for {context['ds']}")
    # ... actual aggregation logic here ...


with DAG(
    dag_id="consumer_gold_orders",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # Scheduled like the producer; the sensor (not the clock) decides when processing actually starts
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["consumer", "gold", "readiness"],
) as consumer_dag:

    # Sensor: wait up to 4 hours for the Silver marker
    wait_for_silver = DatasetReadinessSensor(
        task_id              = "wait_for_silver_orders",
        dataset              = "orders_silver",
        partition_date       = "{{ ds }}",
        postgres_conn_id     = "pipeline_state_db",
        expected_min_rows    = 1000,    # alert if Silver has fewer than 1000 rows
        poke_interval        = 120,     # check every 2 minutes
        timeout              = 14400,   # fail after 4 hours
        mode                 = "reschedule",  # release worker slot between pokes
    )

    run_gold = PythonOperator(
        task_id         = "run_gold_aggregation",
        python_callable = run_gold_aggregation,
    )

    wait_for_silver >> run_gold
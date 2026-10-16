"""Distributed Aggregator -- Airflow orchestration of the daily and weekly visit cubes.

  wait_for_visits (readiness marker) >> daily_cube >> is_week_end >> weekly_cube

- daily_cube: spark_distributed_aggregator_batch.py --view daily --date {{ ds }}. It joins the
  JSON visits with the PostgreSQL devices table and cubes (country, device_type).
  The JDBC credentials come from the `dp_postgres` Airflow connection and are passed as
  env vars (Airflow masks connection passwords in rendered fields), never written in code.
- weekly_cube runs on the last day of the ISO week (Sunday) and rolls the 7 daily cubes
  up. It fails if one is missing, so depends_on_past keeps the daily chain complete.
- Both views overwrite only their own partition (Delta replaceWhere), so retries and
  backfills are idempotent. A daily backfill must be followed by the week's weekly run.
- SALT_BUCKETS (Variable `visits_cube_salt_buckets`) enables two-stage salted aggregation
  when one axis value (e.g. a dominant country) makes a reducer straggle.

Prereqs: Airflow 2.4+, apache-airflow-providers-apache-spark + postgres, connections
`spark_default`, `dp_postgres`, `fs_default`.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

JOB = "/opt/jobs/distributed_aggregator/spark_distributed_aggregator_batch.py"
PACKAGES = "io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.3"
VISITS_PATH = "{{ var.value.get('visits_silver_path', '/data/silver/visits') }}"
ENV = {
    "VISITS_PATH": VISITS_PATH,
    "DAILY_CUBE_PATH": "{{ var.value.get('visits_cube_daily_path', '/data/gold/visits_cube_daily') }}",
    "WEEKLY_CUBE_PATH": "{{ var.value.get('visits_cube_weekly_path', '/data/gold/visits_cube_weekly') }}",
    "SALT_BUCKETS": "{{ var.value.get('visits_cube_salt_buckets', '0') }}",
    "DEVICES_JDBC_URL": "jdbc:postgresql://{{ conn.dp_postgres.host }}:{{ conn.dp_postgres.port }}"
                        "/{{ conn.dp_postgres.schema }}",
    "DEVICES_DB_USER": "{{ conn.dp_postgres.login }}",
    "DEVICES_DB_PASSWORD": "{{ conn.dp_postgres.password }}",
}


def cube_job(view: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=f"{view}_cube",
        application=JOB,
        application_args=["--view", view, "--date", "{{ ds }}", "--explain"],
        conn_id="spark_default",
        packages=PACKAGES,
        env_vars=ENV,
        name=f"visits-cube-{view}",
    )


@dag(
    dag_id="distributed_aggregator_visits_cube",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "depends_on_past": True,
                  "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["data-value", "aggregation", "distributed-aggregator"],
)
def distributed_aggregator_visits_cube():
    wait_for_visits = FileSensor(
        task_id="wait_for_visits",
        filepath=VISITS_PATH + "/date={{ ds }}/_SUCCESS",
        fs_conn_id="fs_default",
        mode="reschedule",
        poke_interval=300,
        timeout=8 * 3600,
    )

    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def is_week_end(data_interval_start=None) -> bool:
        return data_interval_start.isoweekday() == 7

    wait_for_visits >> cube_job("daily") >> is_week_end() >> cube_job("weekly")


distributed_aggregator_visits_cube()

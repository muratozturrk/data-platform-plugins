# external_trigger_dag.py
#
# Two DAGs demonstrating the External Trigger pattern in Airflow:
#
# DAG 1: trigger_receiver_dag
#   - Exposes a REST API endpoint (via Airflow's built-in API)
#   - Receives external trigger calls and creates DAG runs
#   - Validates the trigger payload before starting the pipeline
#
# DAG 2: triggered_pipeline_dag
#   - Designed to be triggered externally, not on a schedule
#   - Reads trigger parameters from dag_run.conf
#   - Processes the specified partition_date
#
# Requirements:
#   pip install apache-airflow apache-airflow-providers-apache-spark
#   pip install apache-airflow-providers-http

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import DagRun
from airflow.api.common.trigger_dag import trigger_dag
from datetime import datetime, timedelta
import json
import logging

log = logging.getLogger(__name__)

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
}


# ── DAG 1: Trigger Receiver ────────────────────────────────────────────────────
# This DAG is triggered by an external system via the Airflow REST API.
# External system calls:
#   POST /api/v1/dags/trigger_receiver_dag/dagRuns
#   Body: {"conf": {"dataset": "orders", "partition_date": "2024-01-10",
#                   "source_system": "orders_api", "row_count": 94821}}

def validate_trigger_payload(**context):
    """
    Validates the trigger payload received from the external system.
    Fails the DAG run if required fields are missing or invalid.
    """
    conf = context["dag_run"].conf or {}

    required_fields = ["dataset", "partition_date", "source_system"]
    missing = [f for f in required_fields if f not in conf]

    if missing:
        raise ValueError(
            f"Invalid trigger payload. Missing required fields: {missing}. "
            f"Received: {json.dumps(conf)}"
        )

    # Validate partition_date format
    try:
        datetime.strptime(conf["partition_date"], "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Invalid partition_date format: {conf['partition_date']}. "
            f"Expected YYYY-MM-DD."
        )

    # Validate row_count if provided
    row_count = conf.get("row_count", 0)
    if row_count == 0:
        log.warning(
            "Trigger received with row_count=0 for dataset=%s date=%s. "
            "Proceeding but this may indicate an empty upstream dataset.",
            conf["dataset"], conf["partition_date"]
        )

    log.info(
        "Trigger validated | dataset=%s | date=%s | source=%s | rows=%s",
        conf["dataset"], conf["partition_date"],
        conf["source_system"], row_count,
    )

    # Push validated config to XCom for downstream tasks
    context["ti"].xcom_push(key="validated_conf", value=conf)


def route_to_pipeline(**context):
    """
    Routes the trigger to the appropriate processing DAG based on dataset name.
    Triggers a child DAG run with the full conf payload.
    """
    conf    = context["ti"].xcom_pull(
        task_ids="validate_trigger", key="validated_conf"
    )
    dataset = conf["dataset"]

    # Routing table: dataset → target DAG
    dag_routing = {
        "orders":    "triggered_pipeline_orders",
        "customers": "triggered_pipeline_customers",
        "products":  "triggered_pipeline_products",
    }

    target_dag = dag_routing.get(dataset)
    if target_dag is None:
        raise ValueError(
            f"No pipeline registered for dataset: {dataset}. "
            f"Registered datasets: {list(dag_routing.keys())}"
        )

    # Create a DAG run for the target pipeline
    run_id = f"triggered__{conf['partition_date']}__{conf['source_system']}"
    trigger_dag(
        dag_id    = target_dag,
        run_id    = run_id,
        conf      = conf,
        replace_microseconds = False,
    )

    log.info(
        "Triggered DAG | dag_id=%s | run_id=%s | partition_date=%s",
        target_dag, run_id, conf["partition_date"],
    )


with DAG(
    dag_id        = "trigger_receiver_dag",
    default_args  = default_args,
    start_date    = datetime(2024, 1, 1),
    schedule_interval = None,       # Never scheduled — only triggered via API
    catchup       = False,
    tags          = ["trigger", "receiver", "orchestration"],
) as trigger_receiver_dag:

    t1 = PythonOperator(
        task_id         = "validate_trigger",
        python_callable = validate_trigger_payload,
    )

    t2 = PythonOperator(
        task_id         = "route_to_pipeline",
        python_callable = route_to_pipeline,
    )

    t1 >> t2


# ── DAG 2: Triggered Pipeline ──────────────────────────────────────────────────
# This DAG is triggered by the receiver DAG (or directly by external systems).
# It reads the partition_date from dag_run.conf and processes it.

def validate_run_conf(**context):
    """
    Reads and validates dag_run.conf passed from the external trigger.
    Pushes processing parameters to XCom.
    """
    conf           = context["dag_run"].conf or {}
    partition_date = conf.get("partition_date")
    source_system  = conf.get("source_system", "unknown")
    row_count      = conf.get("row_count", 0)

    if not partition_date:
        raise ValueError(
            "dag_run.conf missing 'partition_date'. "
            "This DAG must be triggered with a conf payload."
        )

    log.info(
        "Triggered pipeline starting | date=%s | source=%s | rows=%s",
        partition_date, source_system, row_count,
    )

    context["ti"].xcom_push(key="partition_date", value=partition_date)
    context["ti"].xcom_push(key="source_system",  value=source_system)
    context["ti"].xcom_push(key="expected_rows",  value=row_count)


def post_run_validation(**context):
    """
    After the Spark job completes, validates the output matches
    the row count from the trigger payload.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    partition_date = context["ti"].xcom_pull(
        task_ids="validate_conf", key="partition_date"
    )
    expected_rows = context["ti"].xcom_pull(
        task_ids="validate_conf", key="expected_rows"
    )

    hook = PostgresHook(postgres_conn_id="target_dwh")
    actual_rows = hook.get_first(f"""
        SELECT COUNT(*) FROM delta.`/data/silver/orders`
        WHERE DATE(updated_at) = '{partition_date}'
    """)[0]

    if expected_rows > 0:
        ratio = actual_rows / expected_rows
        if ratio < 0.95:
            raise ValueError(
                f"Row count mismatch after trigger processing! "
                f"Expected ~{expected_rows}, got {actual_rows} "
                f"(ratio={ratio:.2%}). "
                f"Investigate Silver table for partition {partition_date}."
            )

    log.info(
        "Post-trigger validation passed | expected=%d | actual=%d",
        expected_rows, actual_rows,
    )


with DAG(
    dag_id        = "triggered_pipeline_orders",
    default_args  = default_args,
    start_date    = datetime(2024, 1, 1),
    schedule_interval = None,       # Never on a schedule — externally triggered only
    catchup       = False,
    tags          = ["triggered", "silver", "orders"],
) as triggered_pipeline_dag:

    t1 = PythonOperator(
        task_id         = "validate_conf",
        python_callable = validate_run_conf,
    )

    t2 = SparkSubmitOperator(
        task_id          = "run_transformation",
        application      = "orders_silver_job.py",   # your processing job
        application_args = [
            "--date", "{{ ti.xcom_pull(task_ids='validate_conf', key='partition_date') }}"
        ],
        conn_id          = "spark_default",
        packages         = "io.delta:delta-core_2.12:2.4.0",
    )

    t3 = PythonOperator(
        task_id         = "post_run_validation",
        python_callable = post_run_validation,
    )

    t1 >> t2 >> t3
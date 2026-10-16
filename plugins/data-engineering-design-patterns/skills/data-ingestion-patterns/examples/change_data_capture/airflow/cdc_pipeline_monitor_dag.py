# cdc_pipeline_monitor_dag.py
# Airflow DAG that:
#   1. Checks Debezium connector health every 5 minutes
#   2. Checks Kafka consumer lag for the Spark streaming job
#   3. Triggers a batch reconciliation Spark job if lag exceeds threshold
#   4. Alerts on any connector failure
#
# Requirements:
#   pip install apache-airflow apache-airflow-providers-http
#   pip install apache-airflow-providers-apache-spark

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
}

CONNECT_URL      = "http://kafka-connect:8083"
CONNECTOR_NAME   = "orders-cdc-connector"
LAG_THRESHOLD    = 50_000   # alert if consumer is > 50k messages behind
CONSUMER_GROUP   = "cdc-spark-streaming"
TOPIC            = "dbserver1.public.orders"


# ── Task functions ─────────────────────────────────────────────────────────────

def check_connector_health(**context):
    """
    Calls the Kafka Connect REST API to verify the Debezium connector
    is in RUNNING state. Raises an exception (which fails the task and
    triggers an alert) if it is in any other state.
    """
    url = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    status = response.json()
    connector_state = status["connector"]["state"]
    task_states = [t["state"] for t in status["tasks"]]

    print(f"Connector state: {connector_state}")
    print(f"Task states: {task_states}")

    if connector_state != "RUNNING":
        raise RuntimeError(
            f"Debezium connector is {connector_state}, expected RUNNING. "
            f"Full status: {json.dumps(status, indent=2)}"
        )

    failed_tasks = [s for s in task_states if s != "RUNNING"]
    if failed_tasks:
        raise RuntimeError(
            f"{len(failed_tasks)} connector task(s) not RUNNING: {failed_tasks}"
        )

    print("Connector health check passed.")


def check_consumer_lag(**context):
    """
    Checks the Kafka consumer group lag for the Spark streaming job.
    Pushes the lag value to XCom so the branch operator can decide
    whether to trigger a reconciliation.

    In production, use the Kafka AdminClient or a metrics exporter
    (Burrow, Confluent Control Center) for this. This simplified version
    uses the Kafka REST Proxy if available.
    """
    try:
        url = f"http://kafka-rest:8082/consumers/{CONSUMER_GROUP}/instances"
        response = requests.get(url, timeout=10)

        if response.status_code == 404:
            print("Consumer group not found — Spark streaming may not be running.")
            context["ti"].xcom_push(key="consumer_lag", value=0)
            return

        lag = response.json().get("lag", 0)
        print(f"Consumer lag: {lag}")
        context["ti"].xcom_push(key="consumer_lag", value=lag)

    except Exception as e:
        print(f"Could not fetch lag (non-critical): {e}")
        context["ti"].xcom_push(key="consumer_lag", value=0)


def branch_on_lag(**context):
    """
    If consumer lag exceeds threshold, trigger the batch reconciliation DAG.
    Otherwise skip and proceed normally.
    """
    lag = context["ti"].xcom_pull(
        task_ids="check_consumer_lag",
        key="consumer_lag"
    ) or 0

    print(f"Lag check: {lag} messages behind (threshold: {LAG_THRESHOLD})")

    if lag > LAG_THRESHOLD:
        print(f"Lag {lag} exceeds threshold — triggering reconciliation.")
        return "trigger_reconciliation"
    else:
        print("Lag within threshold — no action needed.")
        return "lag_ok"


def restart_failed_connector(**context):
    """
    Attempts to restart a failed Debezium connector task.
    Called automatically if check_connector_health fails.
    """
    url = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/tasks/0/restart"
    response = requests.post(url, timeout=10)

    if response.status_code == 204:
        print("Connector task restarted successfully.")
    else:
        print(f"Restart returned {response.status_code}: {response.text}")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="cdc_pipeline_monitor",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",   # every 5 minutes
    catchup=False,
    tags=["cdc", "monitoring", "debezium"],
) as dag:

    t1 = PythonOperator(
        task_id="check_connector_health",
        python_callable=check_connector_health,
    )

    t2 = PythonOperator(
        task_id="check_consumer_lag",
        python_callable=check_consumer_lag,
    )

    t3 = BranchPythonOperator(
        task_id="branch_on_lag",
        python_callable=branch_on_lag,
    )

    t4 = TriggerDagRunOperator(
        task_id="trigger_reconciliation",
        trigger_dag_id="cdc_batch_reconciliation",   # separate DAG
        wait_for_completion=False,
    )

    t5 = EmptyOperator(task_id="lag_ok")

    t6 = PythonOperator(
        task_id="restart_connector_on_failure",
        python_callable=restart_failed_connector,
        trigger_rule="one_failed",   # only runs if t1 fails
    )

    t1 >> t2 >> t3 >> [t4, t5]
    t1 >> t6
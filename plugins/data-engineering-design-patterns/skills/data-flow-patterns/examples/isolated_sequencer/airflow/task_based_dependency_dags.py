"""Isolated Sequencer -- task-based dependency between two DAGs.

Producer `devices_loader_tb` ends with an ExternalTaskMarker pointing at the consumer's
sensor: clearing (backfilling) the producer with "Recursive" also clears the consumer run.
Consumer `devices_aggregator_tb` starts with an ExternalTaskSensor that waits for the
producer's marker task of the same logical date.

Trade-off vs data-based: explicit and backfill-aware, but tightly coupled -- renaming the
consumer DAG/task breaks the producer; both DAGs must share the same schedule (or use
`execution_delta` / `execution_date_fn` to align them).

The alternative push style (`TriggerDagRunOperator`) is also shown: it suits the case where
there is no shared dataset (e.g. the consumer only sends notifications).

Run: put in dags/, unpause both DAGs, `airflow dags test devices_loader_tb 2024-01-01`.
Requires: apache-airflow>=2.6.
"""
import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

START = pendulum.datetime(2024, 1, 1, tz="UTC")
SCHEDULE = "@daily"  # must match on both sides for the sensor to find the same logical date


@dag(dag_id="devices_loader_tb", start_date=START, schedule=SCHEDULE, catchup=False)
def devices_loader_tb():
    @task
    def load_new_devices_to_internal_storage():
        print("loading devices")

    success_execution_marker = ExternalTaskMarker(
        task_id="trigger_downstream_consumers",
        external_dag_id="devices_aggregator_tb",
        external_task_id="downstream_trigger_sensor",
    )

    load_new_devices_to_internal_storage() >> success_execution_marker


@dag(dag_id="devices_aggregator_tb", start_date=START, schedule=SCHEDULE, catchup=False)
def devices_aggregator_tb():
    parent_dag_sensor = ExternalTaskSensor(
        task_id="downstream_trigger_sensor",
        external_dag_id="devices_loader_tb",
        external_task_id="trigger_downstream_consumers",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],  # fail fast instead of waiting until timeout
        mode="reschedule",
        timeout=6 * 3600,
    )

    @task
    def load_data_to_table():
        print("loading table")

    @task
    def refresh_aggregates():
        print("refreshing aggregates")

    parent_dag_sensor >> load_data_to_table() >> refresh_aggregates()


@dag(dag_id="devices_loader_push", start_date=START, schedule=SCHEDULE, catchup=False)
def devices_loader_push():
    @task
    def load_new_devices():
        print("loading devices")

    # Push trigger: consumer DAG has schedule=None and runs only when the producer asks.
    notify_consumer = TriggerDagRunOperator(
        task_id="trigger_notifications_dag",
        trigger_dag_id="devices_notifications",
        execution_date="{{ logical_date }}",  # Airflow 3: rename to logical_date=
        reset_dag_run=True,  # idempotent on producer retries/backfills
        wait_for_completion=False,
    )
    load_new_devices() >> notify_consumer


@dag(dag_id="devices_notifications", start_date=START, schedule=None, catchup=False)
def devices_notifications():
    @task
    def send_notifications():
        print("notify users about new devices")

    send_notifications()


devices_loader_tb()
devices_aggregator_tb()
devices_loader_push()
devices_notifications()

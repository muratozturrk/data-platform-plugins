"""Exclusive Choice -- Apache Airflow.

A condition-evaluator task (`@task.branch`, the TaskFlow form of BranchPythonOperator)
returns the task_id of the ONLY branch to run; the other branch is marked skipped.
Scenario: from the migration date onwards the pipeline runs the new Delta job; backfills
for earlier logical dates still run the legacy CSV job -- one DAG, full run history kept.

The condition is metadata-based (logical date), so it costs nothing. A common child after
the branches must use trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS, otherwise the skipped branch
propagates the skip to it.

Alternative without branching: two DAGs bounded by start_date/end_date.

Run: `airflow dags test devices_exclusive_choice 2024-01-01` (legacy) and `... 2024-02-05` (new).
Requires: apache-airflow>=2.6.
"""
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

MIGRATION_DATE = pendulum.datetime(2024, 2, 3, tz="UTC")


@dag(
    dag_id="devices_exclusive_choice",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def devices_exclusive_choice():
    @task
    def wait_for_input():
        print("input ready")

    @task.branch(task_id="format_router")
    def get_output_format_route() -> str:
        logical_date = get_current_context()["logical_date"]
        if logical_date >= MIGRATION_DATE:
            return "load_job_trigger_delta"
        return "load_job_trigger_csv"

    @task(task_id="load_job_trigger_delta")
    def load_delta():
        print("spark-submit parallel_split_job.py --format delta ...")

    @task(task_id="load_job_trigger_csv")
    def load_csv():
        print("spark-submit parallel_split_job.py --format csv ...")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish_dataset_ready():
        print("notify consumers")

    router = get_output_format_route()
    wait_for_input() >> router >> [load_delta(), load_csv()] >> publish_dataset_ready()


devices_exclusive_choice()

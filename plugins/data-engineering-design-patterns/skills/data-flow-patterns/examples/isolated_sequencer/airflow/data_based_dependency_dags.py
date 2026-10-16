"""Isolated Sequencer -- data-based dependency between two physically separated DAGs.

Producer team: `devices_loader` writes the dataset + a readiness marker.
Consumer team: `devices_aggregator` waits for that marker (Readiness Marker pattern) and
does NOT know which DAG produced it -> loose coupling; the only contract is the path/marker.

Two flavors are shown:
  1. FileSensor on the marker path (works across Airflow instances / teams / tools).
  2. Airflow Dataset scheduling (same Airflow instance only): the consumer is scheduled
     when the producer task that declares `outlets=[DEVICES]` succeeds.

Run: put the file in dags/, set env DEVICES_DIR and INPUT_DEVICES_FILE, trigger devices_loader.
Requires: apache-airflow>=2.6.
"""
import os
import shutil

import pendulum
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.filesystem import FileSensor

DEVICES_DIR = os.environ.get("DEVICES_DIR", "/tmp/dp/devices")
INPUT_DEVICES_FILE = os.environ.get("INPUT_DEVICES_FILE", "/tmp/dp/input/devices.csv")
DEVICES = Dataset(f"file://{DEVICES_DIR}")
START = pendulum.datetime(2024, 1, 1, tz="UTC")


@dag(dag_id="devices_loader", start_date=START, schedule="@daily", catchup=False)
def devices_loader():
    wait_for_input = FileSensor(task_id="input_data_sensor", filepath=INPUT_DEVICES_FILE,
                                mode="reschedule", timeout=3600)

    @task(outlets=[DEVICES])
    def load_new_devices_to_internal_storage():
        ctx = get_current_context()
        partitioned_dir = f"{DEVICES_DIR}/{ctx['ds_nodash']}"
        os.makedirs(partitioned_dir, exist_ok=True)
        shutil.copyfile(INPUT_DEVICES_FILE, f"{partitioned_dir}/dataset.csv")
        # Marker written LAST: consumers must never see a half-written dataset.
        open(f"{partitioned_dir}/_SUCCESS", "w").close()

    wait_for_input >> load_new_devices_to_internal_storage()


@dag(dag_id="devices_aggregator", start_date=START, schedule="@daily", catchup=False)
def devices_aggregator():
    # Dependency enforcer: failing here means the provider didn't deliver as agreed.
    input_data_sensor = FileSensor(
        task_id="input_data_sensor",
        filepath=DEVICES_DIR + "/{{ ds_nodash }}/_SUCCESS",
        mode="reschedule",
        timeout=6 * 3600,
    )

    @task
    def load_data_to_table():
        ctx = get_current_context()
        print(f"Loading {DEVICES_DIR}/{ctx['ds_nodash']}/dataset.csv")

    @task
    def refresh_aggregates():
        print("Refreshing aggregates")

    input_data_sensor >> load_data_to_table() >> refresh_aggregates()


@dag(dag_id="devices_aggregator_dataset_scheduled", start_date=START, schedule=[DEVICES],
     catchup=False)
def devices_aggregator_dataset_scheduled():
    @task
    def refresh_aggregates():
        # Runs after every successful update of DEVICES; no polling sensor needed.
        print("Refreshing aggregates for the latest devices partition")

    refresh_aggregates()


devices_loader()
devices_aggregator()
devices_aggregator_dataset_scheduled()

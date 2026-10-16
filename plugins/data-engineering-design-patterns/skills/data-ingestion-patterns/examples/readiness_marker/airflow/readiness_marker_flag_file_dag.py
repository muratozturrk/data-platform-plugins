"""Readiness Marker with flag files.

devices_producer_flag_file
    The orchestrator creates the marker when the processing layer cannot: ``delete_dataset`` (clean rerun,
    so a stale marker never survives) >> ``generate_dataset`` >> ``create_readiness_file`` (``COMPLETED``),
    always the LAST task. The marker is written to a temp name and renamed, so it appears atomically.

devices_consumer_flag_file
    Pull semantics: waits for the ``COMPLETED`` marker of the producer DAG above.

devices_parquet_consumer_flag_file
    Waits for ``_SUCCESS``, written by Spark itself after all Parquet/Avro/JSON files of a write are
    committed (``mapreduce.fileoutputcommitter.marksuccessfuljobs``, on by default).
    ``mode="reschedule"`` releases the worker slot between pokes.

Airflow 2.4+. Paths are local/NFS for the demo; on object stores use the provider sensors
(S3KeySensor, GCSObjectExistenceSensor) with the same marker names.
"""
import os
import shutil
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

BASE_DIR = os.environ.get("DEVICES_BASE_DIR", "/data/devices")
DATASET_DIR = BASE_DIR + "/date={{ ds }}"
SPARK_OUTPUT_DIR = BASE_DIR + "-parquet/date={{ ds }}"


@dag(dag_id="devices_producer_flag_file", schedule="@daily", start_date=datetime(2024, 1, 1),
     catchup=False, default_args={"retries": 1}, tags=["readiness", "producer"])
def producer():

    @task
    def delete_dataset(ds=None) -> str:
        dataset_dir = f"{BASE_DIR}/date={ds}"
        shutil.rmtree(dataset_dir, ignore_errors=True)
        return dataset_dir

    @task
    def generate_dataset(dataset_dir: str) -> str:
        os.makedirs(dataset_dir, exist_ok=True)
        # processing omitted; replace with the real job (e.g. SparkSubmitOperator)
        with open(os.path.join(dataset_dir, "part-00000.json"), "w", encoding="utf-8") as f:
            f.write('{"type": "tv", "full_name": "TV 1", "version": "1.0"}\n')
        return dataset_dir

    @task
    def create_readiness_file(dataset_dir: str) -> str:
        tmp, final = f"{dataset_dir}/.COMPLETED.tmp", f"{dataset_dir}/COMPLETED"
        with open(tmp, "w", encoding="utf-8") as marker_file:
            marker_file.write("")
        os.replace(tmp, final)
        return final

    create_readiness_file(generate_dataset(delete_dataset()))


@dag(dag_id="devices_consumer_flag_file", schedule="@daily", start_date=datetime(2024, 1, 1),
     catchup=False, default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}, tags=["readiness", "consumer"])
def consumer():
    wait_for_completed = FileSensor(task_id="wait_for_completed_marker", filepath=f"{DATASET_DIR}/COMPLETED",
                                    mode="reschedule", poke_interval=300, timeout=6 * 3600)

    @task
    def consume(ds=None) -> int:
        files = [f for f in os.listdir(f"{BASE_DIR}/date={ds}") if not f.startswith((".", "_")) and f != "COMPLETED"]
        if not files:
            raise ValueError("marker present but no data files: producer contract broken")
        return len(files)

    wait_for_completed >> consume()


@dag(dag_id="devices_parquet_consumer_flag_file", schedule="@daily", start_date=datetime(2024, 1, 1),
     catchup=False, default_args={"retries": 1}, tags=["readiness", "consumer"])
def parquet_consumer():
    # dataset.write.mode('overwrite').format('parquet').save(...) writes _SUCCESS last
    wait_for_spark_success = FileSensor(task_id="wait_for_spark_success", filepath=f"{SPARK_OUTPUT_DIR}/_SUCCESS",
                                        mode="reschedule", poke_interval=300, timeout=6 * 3600)

    @task
    def consume_parquet(ds=None) -> int:
        return len([f for f in os.listdir(f"{BASE_DIR}-parquet/date={ds}") if f.endswith(".parquet")])

    wait_for_spark_success >> consume_parquet()


producer()
consumer()
parquet_consumer()

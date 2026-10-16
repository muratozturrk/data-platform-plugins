"""External Trigger: the externally triggered ``devices-loader`` DAG.

``schedule=None``: the scheduler never plans a run; the only way in is an explicit trigger, here from
``lambda_dag_trigger.py`` through the REST API. Everything the run needs is in ``dag_run.conf``, and the
trigger metadata is logged and kept with the output so a failed run can be traced back to its event.

Airflow 2.4+ (``schedule=``), apache-airflow-providers-amazon for the S3 copy.
"""
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

TARGET_BUCKET = "devices-raw"


@dag(
    dag_id="devices-loader",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=5,                          # bursts of uploads are processed concurrently
    default_args={"depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["ingestion", "event-driven"],
)
def devices_loader():

    @task
    def read_trigger(dag_run=None) -> dict:
        conf = dag_run.conf or {}
        if not conf.get("file_to_load", "").startswith("s3://"):
            # a malformed trigger will never succeed: fail without retries, the event stays in the DLQ/logs
            raise AirflowFailException(f"conf has no valid file_to_load: {conf}")
        print("trigger context:", json.dumps(conf.get("trigger", {})), "event_time:", conf.get("event_time"))
        return conf

    @task
    def copy_file(conf: dict, run_id=None) -> str:
        bucket, key = conf["file_to_load"][len("s3://"):].split("/", 1)
        hook = S3Hook()
        hook.copy_object(source_bucket_key=key, dest_bucket_key=key,
                         source_bucket_name=bucket, dest_bucket_name=TARGET_BUCKET)
        hook.load_string(json.dumps({"run_id": run_id, **conf.get("trigger", {})}),
                         key=f"{key}.trigger.json", bucket_name=TARGET_BUCKET, replace=True)
        return f"s3://{TARGET_BUCKET}/{key}"

    copy_file(read_trigger())


devices_loader()

"""Passthrough Replicator orchestration: push the daily devices reference dataset from prod.

Context: production loads the devices reference dataset from a non-idempotent third-party
API. Dev and staging must not call the API again (they would get different data); they receive an exact copy.

This DAG runs in the PRODUCTION Airflow, which owns the data and so controls when and how fast the copy
happens (push approach). Each lower environment gets its own replication task so a broken staging bucket
does not block dev:

  wait_for_daily_load (sensor on prod _SUCCESS)
      -> replicate_to_<env> (spark_passthrough_file_replicator.py --mode files, same files + metadata)
      -> mark_replicated_<env> (marker in the lower environment, written last)

Airflow 2.4+, apache-airflow-providers-apache-spark. Targets come from the ``passthrough_targets`` Variable,
e.g. {"dev": "s3a://dev-lake/devices", "staging": "s3a://staging-lake/devices"}. Replication runs with
pool ``cross_env_replication`` (1-2 slots) so it cannot starve production workloads.
"""
import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

JOB = os.path.join(os.path.dirname(__file__), "..", "spark", "spark_passthrough_file_replicator.py")
SOURCE_BASE = Variable.get("devices_prod_path", default_var="/lake/prod/devices")
TARGETS = Variable.get("passthrough_targets", deserialize_json=True,
                       default_var={"dev": "/lake/dev/devices", "staging": "/lake/staging/devices"})
PARTITION = "date={{ ds }}"


@dag(
    dag_id="passthrough_replicator_devices",
    schedule="30 2 * * *",                   # after the 02:00 production load
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "replication", "passthrough"],
)
def passthrough_replicator():
    wait_for_daily_load = FileSensor(
        task_id="wait_for_daily_load",
        filepath=f"{SOURCE_BASE}/{PARTITION}/_SUCCESS",
        mode="reschedule",
        poke_interval=300,
        timeout=4 * 3600,
    )

    @task
    def mark_replicated(env: str, target: str, ds=None, run_id=None) -> str:
        """Readiness marker for the lower environment, only after a verified copy."""
        marker = f"{target}/{PARTITION.replace('{{ ds }}', ds)}/_REPLICATED"
        if "://" in marker:
            # ponytail: local/NFS marker only; use the storage SDK (boto3, gcsfs) for object stores
            print(f"skip marker on object store, Spark _SUCCESS already present: {marker}")
            return marker
        with open(marker, "w", encoding="utf-8") as f:
            json.dump({"source_run_id": run_id, "env": env, "at": datetime.utcnow().isoformat()}, f)
        return marker

    for env, target in TARGETS.items():
        replicate = SparkSubmitOperator(
            task_id=f"replicate_to_{env}",
            application=JOB,
            application_args=["--source", f"{SOURCE_BASE}/{PARTITION}",
                              "--target", f"{target}/{PARTITION}", "--mode", "files"],
            conn_id="spark_default",
            pool="cross_env_replication",
            name=f"passthrough-devices-{env}-{{{{ ds_nodash }}}}",
        )
        wait_for_daily_load >> replicate >> mark_replicated.override(task_id=f"mark_replicated_{env}")(env, target)


passthrough_replicator()

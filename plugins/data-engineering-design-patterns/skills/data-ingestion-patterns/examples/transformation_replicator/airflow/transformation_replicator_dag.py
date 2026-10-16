"""Transformation Replicator orchestration: push a PII-free copy of prod data to staging.

The DAG lives in the PRODUCTION Airflow (push approach: the data owner controls frequency and throughput).
  1. wait_for_prod_dataset  - FileSensor on the production readiness marker (never copy a half-written dataset)
  2. validate_pii_contract  - the rules file must parse and use known actions; it is ideally exported from the
                              data catalog / data contract where sensitive fields are tagged (desync mitigation)
  3. replicate              - spark-submit spark_transformation_replicator_batch.py (fails on unclassified columns)
  4. verify_staging_copy    - same job with --verify-only: no forbidden column, same row count
  5. publish_staging_marker - readiness marker in staging written last, with source path and run id

Airflow 2.4+ (``schedule=``), apache-airflow-providers-apache-spark. Config through Airflow Variables:
  transformation_replicator_config = {"source_path": ..., "target_path": ..., "rules_path": ...,
                                      "marker_dir": ..., "spark_conn_id": "spark_prod"}
PII_HASH_SALT is injected into the Spark job from the secret backend via the connection / env, not here.
"""
import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

ALLOWED_ACTIONS = {"keep", "drop", "hash", "mask", "nullify", "expr"}
JOB = os.path.join(os.path.dirname(__file__), "..", "spark", "spark_transformation_replicator_batch.py")
CFG = Variable.get("transformation_replicator_config", deserialize_json=True, default_var={
    "source_path": "/data/prod/devices",
    "target_path": "/data/staging/devices",
    "rules_path": "/opt/airflow/contracts/devices_pii_rules.json",
    "marker_dir": "/data/staging/_markers/devices",
    "spark_conn_id": "spark_default",
})
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"


@dag(
    dag_id="transformation_replicator_devices_to_staging",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["ingestion", "replication", "pii"],
)
def transformation_replicator():
    wait_for_prod_dataset = FileSensor(
        task_id="wait_for_prod_dataset",
        filepath=f"{CFG['source_path']}/_READY",
        mode="reschedule",
        poke_interval=300,
        timeout=6 * 3600,
    )

    @task
    def validate_pii_contract() -> str:
        with open(CFG["rules_path"], encoding="utf-8") as f:
            rules = json.load(f)
        bad = {c: s.get("action") for c, s in rules.get("columns", {}).items()
               if s.get("action") not in ALLOWED_ACTIONS}
        if bad or not rules.get("columns"):
            raise ValueError(f"Invalid PII contract {CFG['rules_path']}: {bad or 'no columns'}")
        if rules.get("default_action", "fail") != "fail":
            # allow-list mode is acceptable, but make it visible in the logs of every run
            print("WARNING: unclassified columns will be dropped silently (default_action=drop)")
        return CFG["rules_path"]

    common_args = ["--source-path", CFG["source_path"], "--target-path", CFG["target_path"],
                   "--rules", CFG["rules_path"]]
    replicate = SparkSubmitOperator(
        task_id="replicate",
        application=JOB,
        application_args=common_args,
        conn_id=CFG["spark_conn_id"],
        packages=DELTA_PACKAGE,
        name="transformation-replicator-{{ ds_nodash }}",
    )
    verify_staging_copy = SparkSubmitOperator(
        task_id="verify_staging_copy",
        application=JOB,
        application_args=common_args + ["--verify-only"],
        conn_id=CFG["spark_conn_id"],
        packages=DELTA_PACKAGE,
    )

    @task
    def publish_staging_marker(ds=None, run_id=None) -> str:
        os.makedirs(CFG["marker_dir"], exist_ok=True)
        final = os.path.join(CFG["marker_dir"], f"{ds}.json")
        tmp = final + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"source": CFG["source_path"], "target": CFG["target_path"], "run_id": run_id,
                       "rules": CFG["rules_path"], "replicated_at": datetime.utcnow().isoformat()}, f)
        os.replace(tmp, final)          # atomic: readers never see a partial marker
        return final

    contract = validate_pii_contract()
    wait_for_prod_dataset >> contract >> replicate >> verify_staging_copy >> publish_staging_marker()


transformation_replicator()

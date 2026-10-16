"""Hybrid Source - multi-job variant orchestrated by Airflow 2.x.

    process_historical_data >> start_real_time_pipeline

* process_historical_data: runs on BIG compute (catch-up), as an availableNow streaming
  job so a retry resumes from its checkpoint instead of reloading everything.
* start_real_time_pipeline: FIRE-AND-FORGET - submits the long-running job in cluster
  mode without waiting for completion, so the Airflow worker slot is released.

historical_job.py / real_time_job.py = the two phases of ../spark/hybrid_source_job.py
split into separate files. Trigger manually (schedule=None) with conf {"switch_timestamp_ms": ...}.
Deps: apache-airflow>=2.6, apache-airflow-providers-apache-spark
"""
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS_DIR = "/opt/jobs/hybrid_source"
SWITCH = "{{ dag_run.conf.get('switch_timestamp_ms', '1762144261000') }}"


@dag(schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     params={"switch_timestamp_ms": "1762144261000"}, tags=["streaming", "backfill"])
def sessions_hybrid_source():
    process_historical_data = SparkSubmitOperator(
        task_id="process_historical_data", conn_id="spark_default",
        application=f"{JOBS_DIR}/historical_job.py",
        executor_cores=8, num_executors=20, executor_memory="16g",  # sized to catch up
        env_vars={"SWITCH_TIMESTAMP_MS": SWITCH}, retries=3)

    start_real_time_pipeline = BashOperator(
        task_id="start_real_time_pipeline",
        bash_command=(
            "spark-submit --master k8s://$K8S_API --deploy-mode cluster "
            "--conf spark.kubernetes.submission.waitAppCompletion=false "
            "--conf spark.executor.instances=4 "
            f"--conf spark.kubernetes.driverEnv.SWITCH_TIMESTAMP_MS={SWITCH} "
            f"local://{JOBS_DIR}/real_time_job.py"))

    process_historical_data >> start_real_time_pipeline


sessions_hybrid_source()

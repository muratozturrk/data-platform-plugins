"""Streaming Reprocessor - automate the risky manual steps (Airflow 2.x).

Manual checkpoint surgery is where human error corrupts state. This on-demand DAG:
  backup_checkpoint >> stop_streaming_job >> restart_from_past

* Flink (native): stop-with-savepoint is part of every release; restart from the savepoint
  taken BEFORE the buggy release with `flink run -s <savepoint>`.
* Spark: copies the checkpoint (backup), stops the job, restarts with the
  `restore-copy` or `new-checkpoint` strategy of ../spark/reprocess_from_past.py.

Trigger with params: {"engine": "flink"|"spark", "savepoint": "...", "last_valid_batch": 41}
Deps: apache-airflow>=2.6 (BashOperator only; replace with your deployment operators)
"""
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

CHECKPOINT_DIR = "/checkpoints/sessions"


@dag(schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     params={"engine": "spark", "savepoint": "", "last_valid_batch": 0}, tags=["reprocessing"])
def sessions_streaming_reprocessor():
    backup_checkpoint = BashOperator(
        task_id="backup_checkpoint",
        bash_command=f"cp -r {CHECKPOINT_DIR} {CHECKPOINT_DIR}_backup_{{{{ ts_nodash }}}}")

    stop_streaming_job = BashOperator(
        task_id="stop_streaming_job",
        bash_command=("{% if params.engine == 'flink' %}"
                      "flink stop --savepointPath /savepoints/sessions $(cat /run/sessions_job_id)"
                      "{% else %}kubectl delete sparkapplication sessions --wait=true{% endif %}"))

    restart_from_past = BashOperator(
        task_id="restart_from_past",
        bash_command=("{% if params.engine == 'flink' %}"
                      "flink run -d -s {{ params.savepoint }} -py /opt/jobs/stateful_flink_consumer.py"
                      "{% else %}spark-submit /opt/jobs/reprocess_from_past.py restore-copy "
                      "--last-valid-batch {{ params.last_valid_batch }}{% endif %}"))

    backup_checkpoint >> stop_streaming_job >> restart_from_past


sessions_streaming_reprocessor()

"""Checkpointer - checkpoint health monitor for a streaming job.

The checkpoint is the only thing that lets a streaming job restart where it
stopped, so it deserves monitoring like any dataset. Every 15 minutes this DAG
reads the Structured Streaming checkpoint of spark_streaming_checkpointer.py:
  * last planned batch (offsets/N) vs last committed batch (commits/N):
    a gap that does not close means a batch keeps failing;
  * age of the last commit file: no commit for longer than the threshold
    means the query is down or stuck - restart it, it resumes from the checkpoint;
  * consumer lag = Kafka end offsets - offsets of the last planned batch,
    i.e. how much the job would reprocess/catch up after a restart.

Requirements: apache-airflow>=2.6, kafka-python on the workers.
Env: DP_CHECKPOINT_DIR, KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

log = logging.getLogger(__name__)

CHECKPOINT_DIR = os.environ.get("DP_CHECKPOINT_DIR", "/tmp/dp/checkpointer/checkpoint")


def _latest(directory: Path) -> Optional[Path]:
    files = [p for p in directory.glob("*") if p.name.isdigit()]
    return max(files, key=lambda p: int(p.name)) if files else None


def read_checkpoint(checkpoint_dir: str) -> dict:
    root = Path(checkpoint_dir)
    offsets_file, commit_file = _latest(root / "offsets"), _latest(root / "commits")
    if offsets_file is None:
        raise AirflowFailException(f"no offsets in {checkpoint_dir}: query never started?")
    lines = [line for line in offsets_file.read_text().splitlines() if line.strip()]
    return {
        "planned_batch": int(offsets_file.name),
        "committed_batch": int(commit_file.name) if commit_file else None,
        "commit_age_s": time.time() - commit_file.stat().st_mtime if commit_file else None,
        "source_offsets": [json.loads(line) for line in lines[2:]],
    }


def kafka_lag(source_offsets: list) -> Dict[str, int]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    try:
        lag = {}
        for source in source_offsets:
            for topic, partitions in source.items():
                tps = {TopicPartition(topic, int(p)): o for p, o in partitions.items()}
                for tp, end in consumer.end_offsets(list(tps)).items():
                    lag[f"{tp.topic}-{tp.partition}"] = end - tps[tp]
        return lag
    finally:
        consumer.close()


@dag(
    dag_id="visits_checkpoint_monitor",
    schedule=dt.timedelta(minutes=15),
    start_date=dt.datetime(2024, 12, 1),
    catchup=False,
    max_active_runs=1,
    params={"max_commit_age_minutes": Param(10, type="integer"),
            "max_lag_records": Param(1_000_000, type="integer")},
    tags=["error-management", "checkpointer"],
)
def visits_checkpoint_monitor():
    @task
    def check_checkpoint(params: dict = None) -> dict:
        state = read_checkpoint(CHECKPOINT_DIR)
        state["lag"] = kafka_lag(state["source_offsets"])
        log.info("checkpoint state: %s", state)
        problems = []
        if state["commit_age_s"] is None or state["commit_age_s"] > params["max_commit_age_minutes"] * 60:
            problems.append("no recent commit: query stopped or stuck - restart it from the checkpoint")
        if state["committed_batch"] is not None and state["planned_batch"] - state["committed_batch"] > 1:
            problems.append(f"planned batch {state['planned_batch']} far ahead of commit "
                            f"{state['committed_batch']}")
        total_lag = sum(state["lag"].values())
        if total_lag > params["max_lag_records"]:
            problems.append(f"lag {total_lag} records")
        if problems:
            raise AirflowFailException("; ".join(problems))
        return state

    check_checkpoint()


visits_checkpoint_monitor()

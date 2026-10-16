# In-Place Overwriter examples

| File | Shows |
|---|---|
| `spark/in_place_overwriter_job.py` | `json` mode: text read, filter on `user_id`, write to staging, leak check, promote. `delta` mode: native `DELETE` + `VACUUM` |
| `kafka/tombstone_overwriter.py` | Scan a compacted, key-based topic to its end offsets and emit tombstones for keys of removed users |
| `airflow/in_place_overwriter_dag.py` | Weekly batched requests -> Spark rewrite to S3 staging (`PROMOTE=false`) -> validate -> promote to output |

Prerequisites: Spark 3.5 (+ `delta-spark` for delta mode, `hadoop-aws` for s3a), Kafka with `cleanup.policy=compact`, Airflow 2.x + boto3.
Env vars: `INPUT_DIR`, `STAGING_DIR`, `DELTA_TABLE`, `PROMOTE`, `KAFKA_BOOTSTRAP`, `TOPIC`, `BUCKET`, `OUTPUT_PREFIX`, `STAGING_PREFIX`, `SPARK_JOB`, `REQUESTS_FILE`.

Run: `spark-submit spark/in_place_overwriter_job.py json <user_id>`; `python kafka/tombstone_overwriter.py <user_id>`.

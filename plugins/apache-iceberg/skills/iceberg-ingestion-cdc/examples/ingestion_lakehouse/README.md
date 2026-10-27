# Ingestion lakehouse examples

One local streaming lakehouse (Kafka KRaft -> Spark Structured Streaming -> Iceberg REST catalog on
SeaweedFS S3) shared by five test modules. 40 tests, about 5 minutes on a fresh stack.

| File | Shows |
|---|---|
| `common/lakehouse.py` | Spark session on the REST catalog (`cache-enabled=false`), Kafka source, checkpoint helpers, `drop_last_commit()` crash simulation |
| `common/kafka_io.py` | Topic admin and an idempotent JSON producer (`enable.idempotence`), tombstones, keyed records |
| `reliable_ingestion/pipeline.py` | Native Iceberg streaming sink (exactly-once), plain `foreachBatch` append (at-least-once), idempotent MERGE consumer, fail-fast vs controlled casting vs DLQ table, schema drift -> `ALTER TABLE ADD COLUMNS` |
| `reliable_ingestion/test_reliable_ingestion.py` | Replay after a simulated crash: 1,500 rows stay 1,500 (native sink), 1,000 become 2,000 (plain append), 1,000 stay 1,000 (MERGE); 4 rejects with reasons in the DLQ |
| `ingestion_models/models.py` | Daily batch with `overwritePartitions` vs `append`, pinned `snapshot-id` input, `availableNow` vs `processingTime` triggers, commit/file stats from metadata tables |
| `ingestion_models/test_ingestion_models.py` | Idempotent reruns, offset-based restarts, `maxOffsetsPerTrigger` vs snapshots/files, produce-to-visible latency |
| `iceberg_writes/writes.py` | COW/MOR table setup, MERGE upsert, concurrent Spark inserts, an independent Java-API writer over py4j (append, validated overwrite), racing writers behind a barrier |
| `iceberg_writes/test_iceberg_writes.py` | Snapshot operation per write, MOR position deletes, no partial writes, `CommitFailedException` vs `commit.retry.num-retries`, `ValidationException`, lost update without validation |
| `kafka_spark_cdc/cdc_source.py` | Debezium-style PostgreSQL envelope simulator (`before`/`after`/`op`/`source.lsn`, tombstones, late stale updates) |
| `kafka_spark_cdc/cdc.py` | `foreachBatch` CDC: drop tombstones, latest change per key by LSN, one guarded `MERGE INTO`; partitioned append with `distribution-mode` / `fanout-enabled` |
| `kafka_spark_cdc/test_kafka_spark_cdc.py` | Convergence to source state, many changes per key, replay idempotency, late events ignored, delete files instead of rewrites, file counts per distribution mode |
| `requirements_evaluator/evaluator.py` | Pure-Python rules: requirements -> model, commit interval, tools, write pattern, delivery guarantee, guardrails, warnings |
| `requirements_evaluator/test_evaluator.py` | Six scenario fixtures plus edge cases (sub-5 s freshness, upserts vs Tableflow, non-replayable source, invalid input) |

## Prerequisites

- Docker with Compose v2, about 4 GB free memory.
- Images: `tabulario/spark-iceberg:3.5.5_1.8.1` (Spark 3.5.5, Iceberg 1.8.1) + `spark-sql-kafka-0-10` 3.5.5
  jars added at build time, `apache/kafka:3.9.1` (single KRaft node, 4 partitions per topic),
  `apache/iceberg-rest-fixture:1.8.1`, `chrislusf/seaweedfs`. Python: pytest 8.3.4, confluent-kafka 2.6.1.
- `config/s3.json` and the compose defaults hold **local-only dev credentials** (`admin`/`password`).
  Change both together, or export `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` and edit `s3.json` to match.

## How to run

```bash
docker compose -p iceberg-ingestion-lakehouse up -d --build
docker compose -p iceberg-ingestion-lakehouse exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-ingestion-lakehouse exec -w /home/iceberg/labs spark-iceberg python -m pytest -v kafka_spark_cdc
docker compose -p iceberg-ingestion-lakehouse down -v
```

The evaluator needs no cluster: `python -m pytest --noconftest requirements_evaluator` and
`python -m requirements_evaluator.evaluator` run anywhere with Python 3.10+.

Notes: on Git Bash for Windows prefix `exec` calls with `MSYS_NO_PATHCONV=1`. No host ports are
published. Checkpoints live under `/tmp/ingestion-checkpoints` in the Spark container; production
checkpoints belong on durable storage, one directory per query.

## Fixes

- Dockerfile base image pinned from `tabulario/spark-iceberg:latest` to `3.5.5_1.8.1`: the Kafka connector
  jars are Spark 3.5.5 / Scala 2.12 builds and break on a newer `latest`. If that tag is unavailable in
  your registry mirror, use a tag with Spark 3.5.5 and keep the jar versions in sync.
- `docker-compose.yml`: hardcoded S3 credentials replaced with `${AWS_ACCESS_KEY_ID:-admin}` /
  `${AWS_SECRET_ACCESS_KEY:-password}` (marked local dev defaults).
- Spark app names, compose project name and checkpoint root renamed to neutral names
  (`lakehouse-tests`, `iceberg-ingestion`, `/tmp/ingestion-checkpoints`); modules renamed to topic names and
  all imports / `pytest.ini` testpaths updated; scenario dict renamed `SCENARIOS` with plain keys.
- No Iceberg API changes were needed: write options (`distribution-mode`, `fanout-enabled`), read option
  `snapshot-id`, snapshot summary keys and the Java `OverwriteFiles` validation calls match Iceberg 1.8.x.

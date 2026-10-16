# Sidecar - examples

| File | Shows |
|------|-------|
| `spark/sidecar_job.py` | Schema harmonization + `unionByName` of two topics, heartbeat source, `foreachBatch` fan-out with `persist` and Delta `txnAppId`/`txnVersion` idempotency |
| `kafka/sidecar_consumer.py` | One consumer on two topics writing to two output topics; offsets committed only when required sinks succeed (`--self-test`) |
| `airflow/README.md` | Why no DAG: orchestration equivalents (fan-in / fan-out patterns in data-flow-patterns) |

Flink: side outputs (`OutputTag` + `get_side_output(...).sink_to(...)`) emit several differently shaped
results from the same state.
Prerequisites: Kafka on `localhost:9094`, Spark 3.5 + Delta 3.x.

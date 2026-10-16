# Online Observer examples

| File | Shows |
|---|---|
| `airflow/online_observer_dag.py` | Local Sequencer: sync pipeline followed by the offline observer's SQL, with an `all_done` sink that fails only if data generation failed. |
| `spark/spark_streaming_online_observer.py` | `foreachBatch` writes data, then one `foreachPartition` pass fills accumulators (invalid fields, counts, max offset per partition); optional sampling; errors isolated. |
| `kafka/kafka_online_observer.py` | Consume-transform-produce loop with inline counters, periodic observation records with own lag, isolated from delivery. `--self-check` runs offline asserts. |

Prerequisites: Airflow 2.x with common-sql/postgres providers (monitoring tables from `offline_observer`); Spark 3.5 + spark-sql-kafka; `confluent-kafka` and a `visits` topic.

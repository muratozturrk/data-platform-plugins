# Static Late Data Integrator: examples

| File | Shows |
| --- | --- |
| `airflow/static_late_integrator_dag.py` | `generate_backfilling_runs` + `SparkSubmitOperator.partial().expand()` over a fixed lookback, three placements (current first / late first / parallel), skipping lookback dates without changes |
| `spark/spark_static_late_integrator.py` | Recomputes one event-time partition with a `replaceWhere` overwrite; can also run current + lookback in one app; `plan-backfill` gives the minimal runs to restart |
| `kafka/kafka_static_lookback_reprocessor.py` | The same lookback over a topic: `offsets_for_times` seek, bounded read, per-date atomic overwrite |

Self-tests: `python spark/spark_static_late_integrator.py --self-test`, `python kafka/kafka_static_lookback_reprocessor.py --self-test`.
Prerequisites: Airflow 2.6+ with the Spark provider, PySpark + `delta-spark`, `kafka-python`.

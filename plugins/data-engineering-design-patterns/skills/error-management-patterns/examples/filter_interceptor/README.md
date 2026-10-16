# Filter Interceptor: examples

| File | Shows |
| --- | --- |
| `spark/spark_filter_interceptor.py` | `programmatic`: `FilterWithAccumulator` + `mapInPandas`. `sql`: `status_flag` CASE subquery + GROUP BY. Both write per-filter stats JSON |
| `kafka/kafka_filter_interceptor.py` | Streaming version: per-filter counters in tumbling processing-time windows, emitted to a stats topic |
| `airflow/filter_interceptor_dag.py` | Runs the job, compares per-filter shares with the median of past runs, fails and names the filter on a spike |

Self-tests: `python spark/spark_filter_interceptor.py --self-test`, `python kafka/kafka_filter_interceptor.py --self-test`.
Prerequisites: PySpark 3.5 (+ pandas/pyarrow) + `delta-spark`, `kafka-python`, Airflow 2.6+ with the Spark provider.

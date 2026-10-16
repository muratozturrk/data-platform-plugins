# Skew Detector - examples

| File | Shows |
|------|-------|
| `spark/volume_skew_guard.py` | Audit step: volume vs last successful run (stats table), seasonal exceptions, key skew STDDEV/AVG |
| `kafka/partition_skew_detector.py` | Partition storage skew from watermarks (`--self-test` included) |
| `airflow/volume_skew_dag.py` | FileSensor >> compare_volumes >> transform >> load; baseline = last successful run |

PostgreSQL partitioned table variant:

```sql
SELECT NOW() AS "time", (STDDEV(n_live_tup) / AVG(n_live_tup)) * 100 AS value
FROM pg_catalog.pg_stat_user_tables
WHERE relname != 'visits_all_range' AND relname LIKE 'visits_all_range_%';
```

Prerequisites: Spark 3.5 + Delta, Kafka on `localhost:9094`, Airflow 2.6+ with a `fs_default` connection.

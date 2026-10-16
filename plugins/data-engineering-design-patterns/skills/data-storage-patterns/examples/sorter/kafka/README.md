# Sorter - Kafka (not applicable as code)

Kafka has no sort keys: a partition is an append-only log ordered by **offset** (arrival order), not by a
column. You cannot ask the broker to keep records sorted by `event_time` or skip blocks by value range.

Equivalents:
- **Per-key ordering** comes from keying: all records with the same key go to one partition, so they are
  ordered relative to each other. That is the only "sort" guarantee Kafka gives.
- **Time-based skipping** uses the broker's timestamp index (`offsets_for_times`), see
  `../../metadata_enhancer/kafka/kafka_time_index_seek.py`.
- **Sorted storage for analytics** is done downstream: sink the topic to Delta/Iceberg/BigQuery and apply the
  Sorter there (`../spark/spark_sorter.py`, `../airflow/sorter_dag.py`).
- Event-time reordering inside a stream (buffer + watermark) belongs to stream processors (Spark, Flink),
  not to the broker.

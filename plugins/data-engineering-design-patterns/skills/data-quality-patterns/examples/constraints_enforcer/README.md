# Constraints Enforcer examples

| File | Shows |
|---|---|
| `spark/spark_delta_constraints.py` | Delta Lake `NOT NULL` + `CHECK` constraint, valid append, rejected all-or-nothing batch, optional pre-filter dispatch. |
| `kafka/kafka_avro_constraints_producer.py` | Avro schema as type/nullability constraint at produce time, plus value rules in the serializer hook (protovalidate equivalent shown in docstring). |
| `airflow/constraints_enforcer_dag.py` | PostgreSQL table with NOT NULL, CHECK, FK, PK; transactional daily load that the DB rejects on any violation. |

Prerequisites: Spark 3.5 + delta-spark; `confluent-kafka[avro]` with Kafka + Schema Registry; Airflow 2.x with common-sql/postgres providers and a `dp_postgres` connection.

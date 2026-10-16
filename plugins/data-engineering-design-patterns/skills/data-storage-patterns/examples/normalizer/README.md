# Normalizer examples

| File | Shows |
|------|-------|
| `spark/spark_normalizer.py` | Flat visits -> snowflake schema (fact + dimensions + sub-dimensions) on Delta, and the join chain needed to read it (broadcasting small dims) |
| `kafka/README.md` | Kafka equivalent: one compacted topic per entity, foreign keys in events, schema references |
| `airflow/normalizer_dag.py` | PostgreSQL 3NF model (fixing 1NF/2NF/3NF violations in a games dataset) with FK-ordered loads |

Prerequisites: `pip install pyspark delta-spark`; Airflow 2.x + Postgres provider, connection `dp_postgres`
from env, and a `dp.games_staging` table (`name, studio, studio_country, platform, platform_language,
release_year, comments TEXT[]`).

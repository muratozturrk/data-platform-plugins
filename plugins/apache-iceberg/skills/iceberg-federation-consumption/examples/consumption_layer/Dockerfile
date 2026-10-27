# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus the consumer-side clients.
# The base image already ships pyarrow 17.0.0 (incl. pyarrow.flight), duckdb 1.2.1, pandas 2.2.3, SQLAlchemy 2.0.38.
# No WORKDIR here: it breaks the base image entrypoint. Use `docker compose exec -w /home/iceberg/labs`.
FROM tabulario/spark-iceberg:latest
RUN pip install --no-cache-dir pytest==8.3.4 "pyiceberg[pyarrow]==0.9.1" polars==1.31.0 \
        "trino[sqlalchemy]==0.335.0" mcp==1.9.4

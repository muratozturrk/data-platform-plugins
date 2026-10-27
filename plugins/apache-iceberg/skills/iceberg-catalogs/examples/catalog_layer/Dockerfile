# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus test tooling.
# The image ships pyiceberg 0.7.1; the labs need a newer PyIceberg (SqlCatalog + REST register_table).
# No WORKDIR here: it breaks the image's entrypoint. Use `docker compose exec -w`.
FROM tabulario/spark-iceberg:latest
RUN pip install --no-cache-dir pytest==8.3.4 boto3==1.35.99 "pyiceberg[sql-sqlite,pyarrow]==0.9.1"

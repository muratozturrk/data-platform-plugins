# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus test tooling.
# The image already ships pyiceberg 0.7.1, pyarrow 17.0.0 (with Arrow Flight), pandas and matplotlib.
# Baking the PostgreSQL JDBC driver into the image (instead of spark.jars.packages)
# avoids an Ivy download on every session start.
# No WORKDIR here: it breaks the image's entrypoint. Use `docker compose exec -w`.
FROM tabulario/spark-iceberg:latest
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/postgresql-42.7.3.jar \
 && pip install --no-cache-dir pytest==8.3.4 boto3==1.35.99 requests==2.32.3

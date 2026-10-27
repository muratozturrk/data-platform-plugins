# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus audit and test tooling.
# The image already ships pyiceberg 0.7.1, pyarrow 17.0.0, PyYAML 6.0.2 and requests.
# No WORKDIR here: it breaks the image's entrypoint. Use `docker compose exec -w`.
FROM tabulario/spark-iceberg:latest

# PostgreSQL JDBC driver: Spark reads legacy warehouse tables for CTAS migration (architecture_plan).
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/spark/jars/
# Hadoop S3A connector: add_files and spark.read address the legacy lake through s3a:// paths
# (Iceberg's own S3FileIO is not used for files outside an Iceberg table). Matches the image's Hadoop 3.3.4.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/postgresql-42.7.3.jar /opt/spark/jars/hadoop-aws-3.3.4.jar \
              /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
 && pip install --no-cache-dir pytest==8.3.4 boto3==1.35.99 psycopg2-binary==2.9.10

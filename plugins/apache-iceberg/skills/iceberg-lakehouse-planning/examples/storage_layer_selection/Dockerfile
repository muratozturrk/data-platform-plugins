# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus test tooling.
# No WORKDIR here: it breaks the image's entrypoint. Use `docker compose exec -w`.
FROM tabulario/spark-iceberg:latest
RUN pip install --no-cache-dir pytest==8.3.4 boto3==1.35.99
# Hadoop S3A connector (versions match the image's Hadoop 3.3.4). storage_standards compares Iceberg's
# S3FileIO with HadoopFileIO over s3a://, and the Hadoop catalog's rename-based commit on S3.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

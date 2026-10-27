# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus Apache Airflow 2.10.5,
# installed with the official constraints file so the dependency set is the one Airflow tests.
# Airflow runs in-process (DagBag + dag.test()), the way the Airflow docs recommend testing DAGs:
# https://airflow.apache.org/docs/apache-airflow/2.10.5/core-concepts/debug.html
FROM tabulario/spark-iceberg:latest

ARG AIRFLOW_VERSION=2.10.5
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" pytest boto3 prometheus_client openlineage-python \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"

ENV AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/home/iceberg/labs/dags \
    AIRFLOW__CORE__UNIT_TEST_MODE=False \
    AIRFLOW__LOGGING__LOGGING_LEVEL=WARNING
RUN airflow db migrate

# rewrite_table_path (disaster_recovery) writes its copy-plan CSV through the Hadoop FileSystem API, not through
# Iceberg's S3FileIO, so the Hadoop S3A connector is needed (versions match the image's Hadoop 3.3.4).
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus the Spark Kafka
# connector and test tooling. The image ships without spark-sql-kafka, so the connector
# and its runtime dependencies are downloaded from Maven Central at build time
# (same artifacts that `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5` resolves).
# fix: pinned; `latest` moves to newer Spark/Iceberg builds that no longer match the 3.5.5 Kafka jars below
FROM tabulario/spark-iceberg:3.5.5_1.8.1
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
 && curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
RUN pip install --no-cache-dir pytest==8.3.4 confluent-kafka==2.6.1

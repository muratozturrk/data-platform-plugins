# Official Apache Spark 3.5.5 image (Scala 2.12, Python 3.8), user spark (uid 185).
# The Spark configuration downloads iceberg-spark-runtime 1.5.2 and hadoop-aws 3.4.0 from
# Maven Central at session start (spark.jars.packages); nothing Iceberg-related is preinstalled.
FROM apache/spark:3.5.5-python3
USER root
RUN pip3 install --no-cache-dir pytest==8.3.4 requests==2.32.3 && mkdir -p /home/spark/.ivy2 && chown -R spark /home/spark
USER spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PATH=/opt/spark/bin:$PATH
WORKDIR /labs
CMD ["sleep", "infinity"]

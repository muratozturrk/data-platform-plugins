"""Lag Detector - Spark Structured Streaming listener for a Kafka source + Delta version lag.

1. `KafkaLagListener`: after each micro-batch, compares the
   processed end offsets with the latest available offsets per partition and pushes
   per-partition lag plus MAX (worst case) and P90 (overall) to Prometheus.
   The Kafka source reports `latestOffset` in the progress, so no extra admin call.
2. `delta_version_lag()`: for a Delta table read with the
   availableNow trigger, lag = last written version - last read version.

Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming_lag_listener.py
Env:  KAFKA_BOOTSTRAP, PUSHGATEWAY_URL
Deps: pyspark>=3.4 (Python listener API), prometheus-client
"""
import json
import os
import statistics

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "localhost:9091")
TOPIC = "visits"


def p90(values: list[int]) -> float:
    # Percentiles, not averages: [10,5,30,2,3,5,3] -> avg 8 but P90 18.
    return statistics.quantiles(values, n=10, method="inclusive")[-1] if len(values) > 1 else float(values[0])


class KafkaLagListener(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        source = event.progress.sources[0]
        if not source.endOffset or not source.latestOffset:
            return
        processed = json.loads(source.endOffset)[TOPIC]
        latest = json.loads(source.latestOffset)[TOPIC]
        lags = {p: latest[p] - processed.get(p, 0) for p in latest}

        registry = CollectorRegistry()
        per_partition = Gauge("visits_reader_lag", "Offset lag per partition", ["partition"], registry=registry)
        for partition, lag in lags.items():
            per_partition.labels(partition=partition).set(lag)
        Gauge("visits_reader_lag_max", "Worst partition lag", registry=registry).set(max(lags.values()))
        Gauge("visits_reader_lag_p90", "P90 partition lag", registry=registry).set(p90(list(lags.values())))
        push_to_gateway(PUSHGATEWAY_URL, job="visits_reader", registry=registry)

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        pass


def delta_version_lag(spark: SparkSession, table: str, query) -> int:
    """Producer side: MAX(version) of the table. Consumer side: reservoirVersion of the last progress."""
    last_written = (spark.sql(f"DESCRIBE HISTORY {table}")
                    .selectExpr("MAX(version) AS v").collect()[0].v)
    last_read = query.lastProgress["sources"][0]["endOffset"]["reservoirVersion"]
    return last_written - last_read


def main() -> None:
    spark = SparkSession.builder.appName("visits_reader_with_lag").getOrCreate()
    spark.streams.addListener(KafkaLagListener())
    visits = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
              .option("subscribe", TOPIC).option("maxOffsetsPerTrigger", 1000).load())
    (visits.selectExpr("CAST(value AS STRING)").writeStream.format("console")
     .option("checkpointLocation", "/tmp/dp/checkpoints/lag_reader").start().awaitTermination())


if __name__ == "__main__":
    assert p90([10, 5, 30, 2, 3, 5, 3]) == 18.0
    main()

"""SLA Misses Detector - Spark Structured Streaming (micro-batch mode).

* Processing-time SLA per micro-batch: the listener compares `progress.batchDuration`
  (end - start of the iteration) with the SLA and pushes a miss counter.
* Event-time SLA (end-to-end, generation -> write): computed inside foreachBatch as
  P95 / MAX of (write time - event_time). Kept SEPARATE from the processing-time SLA
  because late data (e.g. a producer offline for minutes) breaks it without any
  fault on the processing side.

Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 microbatch_sla_listener.py
Env:  KAFKA_BOOTSTRAP, PUSHGATEWAY_URL
Deps: pyspark>=3.4, prometheus-client
"""
import os

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "localhost:9091")
PROCESSING_SLA_MS = 30_000
EVENT_TIME_SLA_SECONDS = 120


def push(job: str, **gauges: float) -> None:
    registry = CollectorRegistry()
    for name, value in gauges.items():
        Gauge(name, name, registry=registry).set(value)
    push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry)


class MicroBatchSlaListener(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        duration_ms = event.progress.batchDuration
        missed = duration_ms > PROCESSING_SLA_MS
        if missed:
            print(f"[SLA MISS] batch {event.progress.batchId} took {duration_ms}ms > {PROCESSING_SLA_MS}ms")
        push("visits_sla", visits_batch_duration_ms=duration_ms, visits_batch_sla_missed=int(missed))

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        pass


def write_with_event_time_sla(batch: DataFrame, batch_id: int) -> None:
    batch.persist()
    batch.write.mode("append").parquet("/tmp/dp/visits_sla_output")
    delays = (batch.select((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("event_time"))
                           .alias("delay"))
              .agg(F.percentile_approx("delay", 0.95).alias("p95"), F.max("delay").alias("max")).collect()[0])
    if delays.p95 is not None:
        push("visits_event_time_sla", visits_e2e_delay_p95_s=delays.p95, visits_e2e_delay_max_s=delays.max)
        if delays.p95 > EVENT_TIME_SLA_SECONDS:
            print(f"[EVENT-TIME SLA MISS] p95 {delays.p95}s - check producers/late data first")
    batch.unpersist()


def main() -> None:
    spark = SparkSession.builder.appName("visits_with_sla").getOrCreate()
    spark.streams.addListener(MicroBatchSlaListener())
    visits = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
              .option("subscribe", "visits").load()
              .select(F.from_json(F.col("value").cast("string"), "visit_id STRING, event_time TIMESTAMP")
                      .alias("v")).select("v.*"))
    (visits.writeStream.foreachBatch(write_with_event_time_sla)
     .option("checkpointLocation", "/tmp/dp/checkpoints/visits_sla").start().awaitTermination())


if __name__ == "__main__":
    main()

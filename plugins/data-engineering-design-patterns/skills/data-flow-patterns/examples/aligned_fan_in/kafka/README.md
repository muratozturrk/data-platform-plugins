# Aligned Fan-In -- Kafka equivalent

No standalone code: Kafka has no "wait until all parents succeeded" primitive. The pattern is
expressed in the consuming processor instead:

- **UNION of streams** = subscribe one consumer/stream to several topics
  (`consumer.subscribe(["visits_web", "visits_app"])` or Spark `subscribe=topicA,topicB`).
  Align the schemas first (same Avro/JSON schema per topic), the analog of `unionByName`.
- **JOIN of streams** = stream-stream or stream-table join (Kafka Streams `join`, Spark
  `withWatermark` + `join`). The "all parents" barrier becomes a *watermark*: the output for a
  window is emitted only once every input has progressed past it.
- **Batch barrier over streams**: emit a readiness event per hourly branch (see
  `isolated_sequencer/kafka/readiness_events.py`) and let an Airflow sensor/consumer start the
  daily merge when all 24 events for the day arrived.

Watch out for the scheduling-skew analog: one lagging topic/partition holds back the watermark,
and therefore the merged output, for everyone.

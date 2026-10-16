# Local Sequencer -- Kafka equivalent

No dedicated code: Kafka is a transport, not an orchestrator, so "chain tasks in one pipeline"
maps to one of two shapes:

- **Single streaming app** (Kafka Streams / Spark Structured Streaming / a consumer loop): the
  steps are chained in-process (`consume -> validate -> enrich -> produce`). Same rules as the
  Spark example -- keep steps that must be atomic (processing + offset commit) together; use
  Kafka transactions (`transactional.id` + `send_offsets_to_transaction`) when they span topics.
- **Chained topics** (`raw -> validated -> enriched`): each hop is a separate consumer group and
  thus a separate restart boundary. A failing enricher replays from its own committed offset
  without re-running validation. This is the streaming analog of splitting an Airflow task.

Choose topic hops only at expensive or independently-replayable boundaries; every extra topic
adds latency, storage and schema contracts.

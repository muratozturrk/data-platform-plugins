# API Gateway - examples

| File | Shows |
|------|-------|
| `kafka/gateway_app.py` | Flask endpoint: topic allow-list, schema and semantic validation, Kafka delivery with headers, fallback storage for undelivered records |
| `spark/fallback_replay_job.py` | availableNow replay of fallback files to Kafka (`cleanSource=delete`, sorted per key, `source=replay` header) |
| `airflow/fallback_replay_dag.py` | Runs the replay only when fallback files exist, and alerts on growing backlog |

Run: `pip install flask confluent-kafka`, Kafka on `localhost:9094`, then `flask --app kafka/gateway_app.py run --port 8080`.
In production, run the gateway behind a load balancer/DNS name with autoscaling and DDoS protection
(for example a managed API gateway), and use durable fallback storage (object store) instead of local disk.

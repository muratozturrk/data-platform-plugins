# Partial State Writer - Airflow

Partial state comes from a **stateful stream processor** (Flink triggers, Spark stateful
operators), so there is no Airflow DAG for it.

The batch equivalent: a frequent incremental DAG (for example every 10 minutes) recomputes the
open sessions of the current day and MERGEs them into the target with `is_final = false`. The
last run after the day closes (plus a lateness margin) writes `is_final = true`. This is the
same contract as the streaming version (consumers must check `is_final`) with the latency of
the schedule.

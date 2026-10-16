# Fine-Grained Accessor for Tables - Kafka equivalent

Kafka has no columns or row predicates: its ACLs stop at the resource level
(topic, consumer group, transactional id), so a principal can read either all
of a topic or none of it. You get the table pattern's effect by changing how
the data is laid out:

- **Columns become topics.** Split sensitive attributes into their own topic
  (see `vertical_partitioner`), then grant `READ` on each topic separately.
- **Rows become topics.** Route records into per-tenant or per-region topics
  (`visits.eu`, `visits.na`) and give each consumer group access only to its
  own topics.
- **Push the filtering downstream.** Sink the topic into a store that supports
  GRANT/RLS/masks (PostgreSQL, Unity Catalog), as in `../airflow` and `../spark`,
  and let consumers read from there.

For topic-level least-privilege ACLs, see
`../../fine_grained_accessor_for_resources/kafka/least_privilege_acls.py`.

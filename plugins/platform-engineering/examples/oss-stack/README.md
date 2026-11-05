# oss-stack examples

- `stack-map.yaml` – the component map (capability → OSS → operator → wave → tenancy default)
- `kafka-tenant-user.yaml` – Strimzi `KafkaUser` with prefix ACLs and quotas = one Kafka tenant boundary
- `ranger-trino-policy.json`, `ranger-trino-mask-policy.json` – Ranger access and masking policies generated per tenant security zone
- `retention_policy.rego` – OPA retention/lifecycle policy (most-restrictive-wins, legal hold) — OPA is the retention engine, Ranger is access control
- `clickhouse-tenant.sql` – database + role + quota + row policy for one tenant

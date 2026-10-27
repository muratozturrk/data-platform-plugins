# oss-stack examples

- `stack-map.yaml` – the component map (capability → OSS → operator → wave → tenancy default)
- `kafka-tenant-user.yaml` – Strimzi `KafkaUser` with prefix ACLs and quotas = one Kafka tenant boundary
- `tenant_isolation.rego` – OPA policy skeleton: tenant from token claim, deny cross-tenant, catalog/schema mapping
- `clickhouse-tenant.sql` – database + role + quota + row policy for one tenant

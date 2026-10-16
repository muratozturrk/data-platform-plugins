---
name: data-security-patterns
description: Data security design patterns for data pipelines - GDPR/CCPA personal data removal (Vertical Partitioner, In-Place Overwriter), column/row and cloud-resource access control (Fine-Grained Accessor for Tables, Fine-Grained Accessor for Resources), data protection (Encryptor, Anonymizer, Pseudo-Anonymizer) and credential handling (Secrets Pointer, Secretless Connector), with Spark, Kafka and Airflow examples. Use when implementing right-to-be-forgotten deletes, tombstones, VACUUM, column masking, row-level security, least-privilege IAM or Kafka ACLs, KMS/TLS encryption, PII anonymization, masking, hashing, tokenization, secrets managers, IAM roles, IRSA, or certificate/mTLS authentication.
---

# Data Security Design Patterns

Valuable datasets are also targets. Data engineering doesn't stop at processing: pipelines must **comply** with privacy laws (GDPR, CCPA: delete a user's data on request), **control access** so that only authorized users and jobs can read or overwrite data, **protect** the data itself so that leaked bytes are useless (encryption, anonymization), and **connect** to stores without leaking credentials. This skill covers nine data security patterns, grouped into data removal, access control, data protection and connectivity, with runnable-ish Spark, Kafka and Airflow samples.

## When to Use This Skill

- Designing or fixing a **personal data removal** (right-to-be-forgotten) pipeline on lakes, lakehouses or Kafka
- Some users may read a table but **not all of its columns or rows**
- A security audit flags **overly broad IAM, bucket or Kafka permissions**
- Data must be **encrypted at rest and in transit** (KMS, TLS)
- **Sharing data with partners**, or other teams, that must not see PII
- Pipelines contain **hardcoded passwords or API keys**, or teams want **no credentials at all**
- Reviewing a pipeline for GDPR or security compliance

## Pattern Selection Guide

| Problem / symptom | Pattern | Reference |
|---|---|---|
| New pipeline. Immutable PII repeated in every event makes deletes expensive | Vertical Partitioner | [references/vertical_partitioner.md](references/vertical_partitioner.md) |
| Legacy data with no PII layout, and you must still delete users | In-Place Overwriter | [references/in_place_overwriter.md](references/in_place_overwriter.md) |
| Table readers must not see some columns or rows | Fine-Grained Accessor for Tables | [references/fine_grained_accessor_for_tables.md](references/fine_grained_accessor_for_tables.md) |
| A job can overwrite every bucket or topic, so least privilege is needed | Fine-Grained Accessor for Resources | [references/fine_grained_accessor_for_resources.md](references/fine_grained_accessor_for_resources.md) |
| Fear of stolen disks or sniffed traffic | Encryptor | [references/encryptor.md](references/encryptor.md) |
| Share data with a third party, and no one may be identifiable | Anonymizer | [references/anonymizer.md](references/anonymizer.md) |
| Share data but keep joins, bands and approximate values | Pseudo-Anonymizer | [references/pseudo_anonymizer.md](references/pseudo_anonymizer.md) |
| A target needs a login, password or API key that must not live in code | Secrets Pointer | [references/secrets_pointer.md](references/secrets_pointer.md) |
| You want no credentials to manage at all | Secretless Connector | [references/secretless_connector.md](references/secretless_connector.md) |

Quick picks:
- **Removal.** Use the Vertical Partitioner if you can reorganize the data. Otherwise use the In-Place Overwriter.
- **Access.** Use the tables pattern for SQL objects, the resources pattern for cloud and broker resources. Often you need both.
- **Sharing.** The Anonymizer is the safest. The Pseudo-Anonymizer is more useful but still counts as personal data.
- **Credentials.** Prefer the Secretless Connector. Use the Secrets Pointer when the target only supports passwords.

## Patterns

### Vertical Partitioner
- **What:** split each row into a mutable or behavioral part (events with only a `user_id` join key) and an immutable PII part, deduplicated to one row per user, written to separate stores.
- **When:** you can change ingestion and PII repeats in every record.
- **Mechanic:** projections in `foreachBatch` write to two sinks. The PII sink is a Delta table updated with MERGE, or a compacted Kafka topic. A removal is one `DELETE` + `VACUUM`, or one tombstone.
- **Gotchas:**
  - Readers must join the parts back together. Expose a view.
  - Without `VACUUM`, time travel still serves the deleted PII.
  - Compaction is only safe on key-unique topics, never on the events topic.
  - Raw unsplit data needs a short retention.
- Reference: [vertical_partitioner.md](references/vertical_partitioner.md) · Examples: [examples/vertical_partitioner/](examples/vertical_partitioner/)

### In-Place Overwriter
- **What:** rewrite existing storage without the removed entities.
- **When:** legacy or horizontally partitioned data with no budget to reorganize.
- **Mechanic:**
  - Native `DELETE` + cleanup (`VACUUM` or snapshot expiry).
  - For flat files: read as text, filter, write to **staging**, validate, then **promote**.
  - For compacted topics: send tombstones for every key of a removed user.
- **Gotchas:**
  - Heavy I/O and cost. **Batch** the requests.
  - Never overwrite the input in place, because a retry loses data.
  - Promotion on object stores is not atomic.
  - No rollback without bucket versioning or a Proxy.
- Reference: [in_place_overwriter.md](references/in_place_overwriter.md) · Examples: [examples/in_place_overwriter/](examples/in_place_overwriter/)

### Fine-Grained Accessor for Tables
- **What:** column- and row-level access on top of table grants.
- **When:** authorized table readers must still not see some columns or rows.
- **Mechanic:**
  - Columns: `GRANT SELECT(cols)` (PostgreSQL, Redshift), policy tags (BigQuery), or mask functions (Unity Catalog, Snowflake).
  - Rows: `CREATE POLICY ... USING (login = current_user)`, `ROW FILTER`, row access policies, or a guard view as a fallback.
- **Gotchas:**
  - Policies can only use session attributes.
  - Nested fields must be unnested before they can be protected.
  - Per-query overhead. Materialize per group if it's too slow.
  - PostgreSQL RLS skips owners and superusers unless you use `FORCE`.
- Reference: [fine_grained_accessor_for_tables.md](references/fine_grained_accessor_for_tables.md) · Examples: [examples/fine_grained_accessor_for_tables/](examples/fine_grained_accessor_for_tables/)

### Fine-Grained Accessor for Resources
- **What:** least-privilege IAM for cloud and broker resources.
- **When:** jobs or users have broader access than they need.
- **Mechanic:**
  - Resource-based policies (bucket policy, GCS binding) or identity-based policies (a role per job).
  - Scope by exact ARN, prefix, or runtime conditions (tags, user id).
  - Kafka ACLs per principal, LITERAL or PREFIXED.
- **Gotchas:**
  - Many tiny policies are hard to maintain, but wildcards over-grant to future resources.
  - Mixing both models adds complexity.
  - Policy quotas (AWS: 1,500 custom policies, GCP: 300 custom roles).
  - Test the negative path (expect AccessDenied).
- Reference: [fine_grained_accessor_for_resources.md](references/fine_grained_accessor_for_resources.md) · Examples: [examples/fine_grained_accessor_for_resources/](examples/fine_grained_accessor_for_resources/)

### Encryptor
- **What:** encrypt data at rest (server-side with KMS or Key Vault, or client-side) and in transit (TLS).
- **When:** stolen storage or intercepted traffic must not expose data.
- **Mechanic:**
  - Server-side: the store calls the key store on every read and write. Authorize clients on both the store and the key.
  - Client-side: the producer encrypts and owns the keys.
  - In transit: TLS of at least version 1.2 enforced on the service.
- **Gotchas:**
  - CPU and KMS overhead (use bucket keys).
  - **A lost key means lost data.** Use deletion windows and alert on `PendingDeletion`.
  - TLS versions get deprecated.
  - Default encryption doesn't re-encrypt existing objects.
  - Enforce TLS by **merging** a deny-non-TLS statement into the bucket policy, never replacing it.
- Reference: [encryptor.md](references/encryptor.md) · Examples: [examples/encryptor/](examples/encryptor/)

### Anonymizer
- **What:** irreversibly remove or alter PII so no one can be identified, even after combining datasets.
- **When:** sharing with third parties or users who didn't consent.
- **Mechanic:** data removal (`drop`), perturbation (noise, random digits, date shifts), or synthetic replacement (Faker, or ML generators).
- **Gotchas:**
  - Strong **information loss**: anonymized columns are useless for joins or models.
  - Quasi-identifiers (zip + birth date) re-identify people unless you generalize them.
  - Allowlist the output columns.
  - Validate before publishing.
- Reference: [anonymizer.md](references/anonymizer.md) · Examples: [examples/anonymizer/](examples/anonymizer/)

### Pseudo-Anonymizer
- **What:** replace PII with substitutes that keep some meaning: masking, tokenization (vault), hashing (HMAC), encryption, and generalization (country to area, salary to range).
- **When:** consumers need joins, bands or reversibility for authorized processes.
- **Mechanic:** column functions or `mapInPandas`. Type-changing transformations use column expressions.
- **Gotchas:**
  - **A false sense of security.** Joined with other data it re-identifies people, so it is still personal data.
  - Masks collide.
  - Plain hashes of emails are brute-forceable. Use keyed hashes (HMAC in a UDF, so the key never shows in the Spark plan).
  - The vault or key is the crown jewel.
- Reference: [pseudo_anonymizer.md](references/pseudo_anonymizer.md) · Examples: [examples/pseudo_anonymizer/](examples/pseudo_anonymizer/)

### Secrets Pointer
- **What:** code references a secret's **name**. The value comes from a secrets manager at runtime.
- **When:** the target only supports a login/password or an API key.
- **Mechanic:**
  - Fetch from Secrets Manager or Vault and keep it in memory, optionally cached with a TTL.
  - In Airflow, configure a secrets backend and keep using `conn_id` and Variables.
  - IaC generates values under the same names in every environment.
- **Gotchas:**
  - Cached secrets go stale after a rotation. Refresh and retry once on auth errors (SQLSTATE 28P01), otherwise fail and restart with idempotent writes.
  - Secrets still leak through **logs**, the Spark UI, or URLs. Redact every resolved value.
  - Someone still produces the secret, so protect that path too.
- Reference: [secrets_pointer.md](references/secrets_pointer.md) · Examples: [examples/secrets_pointer/](examples/secrets_pointer/)

### Secretless Connector
- **What:** authenticate with an identity instead of a secret.
- **When:** a cloud service or store supports identities, and you want nothing to rotate or leak.
- **Mechanic:**
  - IAM-based: roles, service accounts, IRSA, managed identities. The default SDK credential chain picks up short-lived STS tokens.
  - Certificate-based: a CA-signed client certificate, mTLS, `sslmode=verify-full`.
- **Gotchas:**
  - It isn't workless: you still set up roles, trust policies and bindings.
  - Certificate rotation needs an overlap period.
  - Static keys in the environment silently win over the workload identity.
  - The certificate's private key still needs protection.
- Reference: [secretless_connector.md](references/secretless_connector.md) · Examples: [examples/secretless_connector/](examples/secretless_connector/)

## Cross-cutting Best Practices

- **Defense in depth.** Combine access control, encryption, anonymization and secretless connectivity. No single pattern is enough.
- **Policy as code.** Grants, IAM, ACLs, KMS keys and secrets are created by IaC or a pipeline, reviewed in Git, and re-applied idempotently. Drift-detection DAGs catch manual changes.
- **Least privilege by default.** One identity per job, deny by default, and a test that expects AccessDenied.
- **Deletion means physical deletion.** Account for time travel (`VACUUM`, snapshot expiry), Kafka tombstones and compaction timing, backups, staging leftovers and downstream copies. Lineage tells you where PII flows.
- **Batch privacy work.** Group removal requests and run them with a Single Runner (`max_active_runs=1`) so concurrent rewrites don't corrupt data.
- **Validate before publishing.** Assert that no forbidden columns or original values are present before data leaves a trust boundary.
- **Never log secrets or PII.** Mask them in logs, avoid credentials in URLs, and prefer allowlists over denylists for outgoing fields.
- **Plan key and credential lifecycles.** Cover rotation with overlap, deletion windows, and monitoring of key states.
- Check retention-based shortcuts with legal and compliance teams first.

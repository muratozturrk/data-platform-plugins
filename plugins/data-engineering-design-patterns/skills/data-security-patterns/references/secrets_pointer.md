# Secrets Pointer

Category: Connectivity · Layer: job configuration / runtime

## Problem
The real-time visits pipeline enriches each event with geolocation from an external API whose
only authentication is a login/password pair. In the past, your team accidentally shared the
credentials of another, request-billed API and the bill spiked. You want to avoid storing the
login/password in the code that calls the new API.

## Solution / Mechanics
Don't store credentials anywhere in code or config: use a **reference** (pointer) to a secret in
a secrets manager such as Google Cloud Secret Manager or AWS Secrets Manager.

Steps:
1. A secrets **producer** (an administrator, or IaC generating random values when it creates
   the database) writes the value under a name.
2. Consumers reference only the **name** and query the service at runtime.
3. Consumers may **cache** the value locally for some time to save calls.
4. Access is protected twice: the consumer must be allowed to read the secret (fine-grained
   access), and the credentials must be valid for the target.

Advantages: one central place to manage and **monitor** access; credentials can be replaced
without updating consumers; the same secret names in every environment (IaC generates the
values), so no per-environment config files with passwords.

```
  Git / config:  DB_SECRET_ID = "dp/postgres/devices-reader"     (name only)
        |
        v
  job driver --GetSecretValue(name)--> [ Secrets Manager ] <-- IaC / admin writes value
        |      (IAM: this role, this ARN)        |
        |  <------- {"username","password"} -----+
        |  TTL cache, value registered for log redaction
        v
  spark.read.jdbc(url, properties={user, password}) --> PostgreSQL
        |  auth error 28P01? -> invalidate cache, fetch again, retry once
```

## Consequences & trade-offs
- **Cache invalidation and streaming jobs**: a cached secret may be outdated after a
  rotation. Simplest: let the job fail; the restart reloads the secret (use idempotency patterns
  so retries keep data correct). Frequent rotations mean frequent failures. An asynchronous
  refresh can hit a rotation between sending data and refreshing the connection.
- **Logs**: the store protects the secret, but you can still leak it in logs.
- **A secret remains secret**: someone produces the value. Protect that path (admin
  access, IaC state files holding generated passwords).
- Bootstrapping: reading the secrets manager needs an identity; use a Secretless Connector for
  it, or you're back to storing a key.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Target supports only login/password or API key | Yes | - |
| Target supports IAM/workload identity or certificates | No | Secretless Connector |
| Airflow connections/variables | Yes, via secrets backend | - |
| Per-environment configs with passwords today | Yes, same names per env | - |
| Very frequent rotation + long-running streams | Yes, with refresh-on-auth-error | Short-lived tokens (Secretless) |

## Key design decisions
- Naming convention `<domain>/<system>/<consumer>` mapping to IAM resource patterns.
- One secret per consumer, so a single consumer can be revoked.
- Secret format: JSON with `username`/`password` (RDS-managed rotation format) or plain string.
- Cache TTL vs rotation frequency; refresh-on-auth-failure vs fail-and-restart.
- Rotation: managed rotation (RDS) or dual credentials during switch-over.

## Gotchas & best practices
- Pass passwords through JDBC `properties`, never inside the URL: Spark redacts option keys
  matching `spark.redaction.regex` (`password`) but not a password embedded in the URL.
- Register every resolved value (raw JSON **and** parsed password) with a log redaction filter;
  give credential objects a `__repr__` that hides the password.
- Truncate and redact exception messages before logging: drivers can echo connection details.
- Don't put long-lived secrets in environment variables on shared hosts (other processes,
  crash dumps); fetch at runtime.
- Secret-scan the repository (gitleaks, trufflehog) and rotate anything that reached Git.
- Alert on unusual `GetSecretValue` access patterns (CloudTrail).

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `password authentication failed` (28P01) after rotation | Stale cached secret | Invalidate + refetch + retry once; else restart |
| `AccessDeniedException` on GetSecretValue | IAM missing on secret ARN or KMS key | Grant per-secret ARN and `kms:Decrypt` |
| Password visible in Spark UI | Credentials in JDBC URL | Move to `properties` |
| Password in logs | Printed dict / exception | Redaction filter, `__repr__`, truncated errors |
| Duplicates after restart | Non-idempotent write | Overwrite the run's partition |
| Different values per environment break code | Different secret names | Same names, IaC-generated values |

## Implementation notes per stack
**Spark** — `examples/secrets_pointer/spark/secrets_pointer_job.py` (extended):
```python
def with_credentials_refresh(action, cache, secret_id, default_user):
    credentials = parse_secret(cache.get(secret_id), default_user)
    try:
        return action(credentials)
    except Exception as error:
        if not is_auth_failure(error):
            raise
        cache.invalidate(secret_id)
        return action(parse_secret(cache.get(secret_id), default_user))
```
Exports each table to `<output>/<table>/export_date=<date>` with `mode("overwrite")`
(replay-safe). `--self-test` checks the TTL cache, refresh-once and redaction.

**Kafka** — `examples/secrets_pointer/kafka/sasl_from_secrets_manager.py`: SASL/SCRAM
credentials from Secrets Manager with a TTL cache; on `_AUTHENTICATION` errors the cache is
invalidated and the client rebuilt.

**Airflow** — `examples/secrets_pointer/airflow/secrets_backend_dag.py`: `SecretsManagerBackend`
configured in `airflow.cfg` (see docstring); the DAG only references `conn_id` and Variable keys.
```ini
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```

## Related patterns
- Secretless Connector: removes credentials when the target supports identities.
- Fine-Grained Accessor for Resources: who may read each secret.
- Idempotency patterns: make fail-and-restart after rotation safe.
- Encryptor: secrets managers encrypt values with KMS keys.

# Encryptor

Category: Data Protection · Layer: storage and network

## Problem
Fine-grained access policies for tables and cloud resources are in place. Stakeholders now worry
that someone could intercept data between streaming brokers and jobs (**in transit**) or
physically steal it from servers (**at rest**). Even with the bytes, data must be unusable.

## Solution / Mechanics
Two protection levels, two implementations.

**At rest**, the difference between options is key management:
- **Client-side**: the producer encrypts before sending and manages the key.
- **Server-side**: the store encrypts/decrypts and the key lives in a key store (AWS/GCP KMS,
  Azure Key Vault). Widely supported by cloud services.

Server-side read workflow:
1. The request reaches the encrypted store, which doesn't return data directly.
2. The store identifies the key and asks the key store for it. Unauthorized client -> failure.
3. The store decrypts the data with the key.
4. The store returns decrypted records.

```
 client --(1) GET object--> [ S3 / store ] --(2) Decrypt data key--> [ KMS ]
                                  |          <-- key (if caller may use it)
                            (3) decrypt
 client <--(4) plaintext----------+
 Two permissions needed: s3:GetObject on the object AND kms:Decrypt on the key.
```

**In transit**: enable TLS in the client SDK and require a minimum protocol version on the
service (Azure Event Hubs `minimum_tls_version = "1.2"`). On S3, a bucket policy
statement denying `aws:SecureTransport = false` rejects plain HTTP.

## Consequences & trade-offs
- **Encryption/decryption overhead**: every read and write costs CPU; with KMS also
  API latency and request cost. S3 Bucket Keys cut KMS calls.
- **Data loss risk**: losing the key, or access to it, locks out authorized users too.
  Key stores soft-delete keys with a grace period: `deletion_window_in_days = 14`.
- **Protocol updates**: TLS 1.0/1.1 are deprecated (RFC 8996); services and clients must
  keep up. Cloud services usually reduce this to one setting.
- Client-side encryption adds key distribution and rotation work; old data needs old keys.

## When to use / when not
| Situation | Use? | Alternative / complement |
|---|---|---|
| Any cloud storage with sensitive data | Yes, server-side KMS by default | - |
| Provider or platform admins must not read data | Client-side | Parquet modular encryption, app-level envelope |
| Broker <-> job traffic | TLS (+ mTLS for auth) | Secretless Connector for the identity part |
| Hide columns from authorized table readers | No | Fine-Grained Accessor for Tables |
| Share data with partners without PII | No | Anonymizer / Pseudo-Anonymizer |

## Key design decisions
- Server-side vs client-side (simplicity vs provider-blind data).
- Customer-managed keys (key policy, CloudTrail audit, revocation) vs provider-managed.
- Key granularity per domain/dataset: revoking one key shouldn't lock out everything.
- Who may schedule key deletion; alerting on `PendingDeletion` and `Disabled`.
- Enforcement: default encryption + deny statements, verified by a drift detector.

## Gotchas & best practices
- Encryption isn't access control: whoever can read the store **and** use the key sees plaintext.
- Default bucket encryption doesn't re-encrypt existing objects; copy in place or batch job.
- Enforcing TLS by **replacing** the bucket policy wipes other statements (cross-account grants).
  Read, merge by `Sid`, put back; treat `NoSuchBucketPolicy` as empty, any other error as fatal.
- Rotation: KMS automatic rotation keeps old versions for decryption; client-side keyrings
  (MultiFernet) must keep old keys until data is re-encrypted.
- Never hardcode keys; Parquet `InMemoryKMS` is a demo, plug a real `KmsClient` in production.
- Check hostname verification on TLS clients, not just the protocol.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `AccessDenied` reading an S3 object you can list | Missing `kms:Decrypt` on the key | Add key grant / key policy entry |
| High KMS bill, throttling | One KMS call per object | Enable Bucket Keys, cache data keys |
| Partner access broke after TLS remediation | Bucket policy overwritten | Merge statements by Sid |
| Old objects unencrypted | Default applies to new writes only | Re-copy with SSE-KMS |
| Clients fail after TLS upgrade | Old client library / TLS 1.0 | Upgrade client, set min protocol 1.2 |
| `InvalidToken` on consume | Key rotated, old key dropped | Keep old keys in MultiFernet |
| Data unreadable after key deletion | Deletion window expired | Restrict `ScheduleKeyDeletion`, alert, cancel in window |

## Implementation notes per stack
**Airflow** — `examples/encryptor/airflow/enforce_bucket_encryption_dag.py`: daily drift
detector (audit-only unless `ENFORCE=true`) for SSE-KMS, KMS key state, and a
TLS-only statement merged into the existing policy (`--self-test` checks the merge):
```python
def merge_statement(existing_policy, statement: dict) -> str:
    policy = json.loads(existing_policy) if existing_policy else {"Version": "2012-10-17", "Statement": []}
    statements = policy.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    policy["Statement"] = [s for s in statements if s.get("Sid") != statement["Sid"]] + [statement]
    return json.dumps(policy, sort_keys=True)
```

**Spark** — `examples/encryptor/spark/encrypted_storage_job.py`: `sse` mode (server-side):
```python
.config("spark.hadoop.fs.s3a.encryption.algorithm", "SSE-KMS")
.config("spark.hadoop.fs.s3a.encryption.key", os.environ["KMS_KEY_ARN"])
```
`parquet` mode (client-side): `parquet.encryption.column.keys = piiKey:user_id,ip` and a footer key.

**Kafka** — `examples/encryptor/kafka/encrypted_producer_consumer.py`: `security.protocol=SSL`
with `ssl.endpoint.identification.algorithm=https`, plus client-side `MultiFernet` payload
encryption supporting key rotation (`PAYLOAD_KEY`, `PAYLOAD_OLD_KEYS`).

## Verification
- `aws s3api get-bucket-encryption` returns `aws:kms` with the expected key ARN on every bucket.
- `head-object` on a fresh write shows `ServerSideEncryption: aws:kms` and the key ID.
- A plain-HTTP request (`aws s3 ls s3://<bucket> --endpoint-url http://s3.<region>.amazonaws.com`)
  is denied by the TLS-only statement.
- The bucket policy still contains every pre-existing statement after remediation.
- `describe-key` shows `Enabled`; CloudTrail alarms on `ScheduleKeyDeletion` / `DisableKey`.
- A principal without `kms:Decrypt` gets `AccessDenied` on read, even with `s3:GetObject`.
- Kafka: `openssl s_client -tls1_1` against the listener fails; `-tls1_2` succeeds.

## Related patterns
- Fine-Grained Accessor for Resources: key policies and grants decide who can decrypt.
- Secrets Pointer: storage for client-side keys.
- Secretless Connector: TLS/mTLS also authenticates the client.
- Pseudo-Anonymizer: column-level encryption as a reversible pseudonymization method.

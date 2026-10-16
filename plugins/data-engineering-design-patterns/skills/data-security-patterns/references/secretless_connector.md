# Secretless Connector

Category: Connectivity · Layer: identity / authentication

## Problem
A team is integrating a new data processing service. Every code example it found uses API keys
to reach cloud-managed resources. The team is small, doesn't want to manage keys, and asks for a
solution that grants access **without any credentials referenced in code**.

## Solution / Mechanics
Authenticate with an **identity** instead of a secret. Two implementations.

**1. IAM-based access.** An administrator attaches read/write actions to users, groups or roles
through access policy documents. Application users (scheduled jobs) don't log in; their runtime
identity is authorized. Workflow:
1. The application issues a request to a cloud service (e.g. read one object).
2. The service asks the IAM service whether the caller has the required permissions.
3. IAM answers with the permission scope.
4. The service returns the response, or an error if permissions are missing.

**2. Certificate-based authentication.** Same flow, but a certificate authority (CA) validates
the client certificate before the connection proceeds. PostgreSQL `cert` auth,
Kafka mTLS.

```
 IAM-based                                         certificate-based
 job (pod SA / instance profile)                  job with client.crt + key
   | (1) GET s3://visits/obj  [STS short-lived]     | TLS handshake, presents cert
   v                                                v
 [ S3 ] --(2) authorize?--> [ IAM ]               [ broker / DB ] --verify--> [ CA ]
   | <--(3) scope----------                         | CN=visits-processor -> principal -> ACLs
   v (4) object or AccessDenied                     v connection or handshake failure
 nothing stored in code; AWS rotates STS tokens   no password; certificate expiry = rotation
```

## Consequences & trade-offs
- **Workless impression**: no credentials doesn't mean no work. Entities must be
  configured, e.g. on AWS an assume-role permission so the entity gets temporary STS credentials.
  Plus trust policies, service-account bindings, role annotations.
- **Rotation**, mainly certificate-based: rotating credentials reduces leak risk but adds
  overhead. Issue new credentials, support old **and** new in parallel until every consumer
  confirms the switch, then drop the old ones.
- The certificate's private key is still sensitive material; protect the mount.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Cloud services with IAM (S3, GCS, Kinesis, Dataflow) | Yes, roles/service accounts | - |
| Jobs on EKS/GKE/AKS | Yes, workload identity (IRSA, GKE WI) | - |
| PostgreSQL / Kafka with a PKI | Yes, cert / mTLS | - |
| Third-party API with login/password only | No | Secrets Pointer |
| Needing access to secrets manager itself | Yes, bootstraps the Secrets Pointer | - |

## Key design decisions
- One identity per job (combine with Fine-Grained Accessor for Resources).
- Identity source: instance profile, IRSA / workload identity, ECS task role, managed identity.
- Certificate lifetime and automated issuance (cert-manager, Vault PKI, ACM PCA).
- Principal mapping for certificates (`ssl.principal.mapping.rules`, pg `clientcert`/ident maps).
- Fail-fast checks: refuse static keys found in the environment.

## Gotchas & best practices
- The default credential chain prefers environment variables: a stray `AWS_ACCESS_KEY_ID` silently
  wins over the workload identity. Detect and fail.
- `sslmode=verify-full` checks the server hostname against its certificate; `require` doesn't
  verify the server at all.
- pgJDBC expects the client key in PKCS-8 DER (`.pk8`) format.
- A password-protected private key brings a secret back; prefer platform-mounted unencrypted keys
  with strict file permissions.
- GCP: bind roles on the bucket/resource to the service account; attach the SA to the job.
- Log `sts:GetCallerIdentity` at job start to see who the job actually runs as.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Job uses a human's keys | Env vars / shared credentials file present | Remove; fail on static keys |
| `WebIdentityErr` / no credentials in pod | SA not annotated, trust policy mismatch | Annotate SA; trust `sts:AssumeRoleWithWebIdentity` for that SA |
| `SSL handshake failed` | Expired cert or untrusted CA | Rotate cert; check CA bundle |
| `hostname does not match` | `verify-full` with wrong host | Use the cert's DNS name |
| Consumers broke after rotation | Old cert dropped too early | Overlap period: CA trusts both |
| `AccessDenied` despite identity | Role lacks permission | Fix policy (Fine-Grained Accessor) |

## Implementation notes per stack
**Spark** — `examples/secretless_connector/spark/secretless_connector_job.py`:
```python
devices = spark.read.jdbc(url=JDBC_URL, table="dp.devices", properties={
    "driver": "org.postgresql.Driver",
    "user": os.environ.get("DB_USER", "dp_job"),  # identity, not a secret
    "ssl": "true", "sslmode": "verify-full",
    "sslrootcert": os.environ.get("PG_SSL_ROOT_CERT", "/certs/root.crt"),
    "sslcert": os.environ.get("PG_SSL_CERT", "/certs/client.crt"),
    "sslkey": os.environ.get("PG_SSL_KEY", "/certs/client.pk8"),
})
```
S3A uses `WebIdentityTokenCredentialsProvider` (IRSA) then `InstanceProfileCredentialsProvider`.

**Kafka** — `examples/secretless_connector/kafka/mtls_client.py`: `security.protocol=SSL` with
`ssl.certificate.location` / `ssl.key.location`; the broker maps the cert subject to an ACL principal.

**Airflow** — `examples/secretless_connector/airflow/identity_based_access_dag.py`:
`refuse_static_keys >> whoami >> count_objects`; `whoami` fails if boto3's credential method is
`env`, `shared-credentials-file` or `config-file`.

GCP equivalent: a `google_service_account`, a
`google_storage_bucket_iam_binding` with `roles/storage.objectViewer`, and
`service_account_email` on the `google_dataflow_job`.

## Verification
- No `AWS_ACCESS_KEY_ID`, passwords or key files committed or set in job environments.
- `get-caller-identity` from the job shows the expected role, not a user.
- Credentials method is `assume-role-with-web-identity` / `iam-role`, never `env`.
- Removing the role binding makes the job fail with `AccessDenied` (proves the identity is used).
- Client certificate expiry monitored, alert >= 14 days before.
- `psql "sslmode=verify-full sslcert=... sslkey=..."` connects without a password prompt; the
  same command without the certificate is rejected by `pg_hba.conf` (`hostssl ... cert`).
- Kafka: a client without a certificate fails the handshake; one with a cert but no ACLs gets
  `TopicAuthorizationException` (authentication works, authorization is separate).

## Related patterns
- Secrets Pointer: for targets that only accept passwords; uses this pattern to reach the store.
- Fine-Grained Accessor for Resources: decides what the identity may do.
- Encryptor: TLS in transit is the channel mTLS authenticates over.

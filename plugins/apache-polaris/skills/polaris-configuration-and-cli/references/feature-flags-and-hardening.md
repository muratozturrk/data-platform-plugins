# Feature Flags, Catalog Properties and Hardening Settings

## Concept

Polaris behavior is tuned with Quarkus configuration properties (environment variables in
containers, or `application.properties`). Three layers matter:

1. **Feature flags**, server-wide: `polaris.features."<FLAG>"`, and per realm:
   `polaris.features.realm-overrides."<realm>"."<FLAG>"`.
2. **Catalog-level properties** set on a catalog entity: `polaris.config.<name>` in the catalog's
   `properties` map.
3. **Service settings**: token broker, rate limiter, task executor, realm context, readiness.

Flag values are JSON for lists (`'["S3","GCS"]'`) and strings for booleans (`"true"`).

## Feature Flag Reference (names used in the examples, Polaris 1.8.0)

| Flag | Value used | Effect |
|---|---|---|
| `SUPPORTED_CATALOG_STORAGE_TYPES` | default `S3, GCS, AZURE` | Storage types allowed in catalogs |
| `ALLOW_INSECURE_STORAGE_TYPES` | `"true"` | Required (with the list above) for FILE |
| `DROP_WITH_PURGE_ENABLED` | `"true"` | Drop-with-purge (set in quickstarts; not measured separately) |
| `ENABLE_CATALOG_FEDERATION` | `"true"` | `EXTERNAL` catalogs with connections (`polaris-catalog-federation` skill) |
| `SUPPORTED_CATALOG_CONNECTION_TYPES` | `'["ICEBERG_REST"]'` | Federation connection types |
| `SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES` | `'["OAUTH","BEARER","IMPLICIT"]'` | Auth types toward remotes |

Catalog properties used in the examples: `polaris.config.drop-with-purge.enabled=true`,
`polaris.config.allow.unstructured.table.location=true` (registered tables outside Polaris'
namespace layout). The flag `INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST` named in some older
hardening guides does not occur in the 1.8.0 jars.

## Global vs Realm Override (measured)

```yaml
polaris.realm-context.realms: POLARIS,analytics
polaris.features.defaults."SUPPORTED_CATALOG_STORAGE_TYPES": '["S3","GCS","AZURE","FILE"]'   # older form
polaris.features."ALLOW_INSECURE_STORAGE_TYPES": "true"
polaris.features.realm-overrides."analytics"."SUPPORTED_CATALOG_STORAGE_TYPES": '["S3"]'
```

| Request | Status | Meaning |
|---|---|---|
| FILE catalog in `POLARIS` | 400 | `.defaults.` form ignored (FILE not added) |
| GCS catalog in `POLARIS` | 201 | Built-in default applies |
| GCS catalog in `analytics` | 400 | Realm override applies |
| S3 catalog in `analytics` | 201 | Allowed by the override |

Use `polaris.features."FLAG"` for global values; `polaris.features.defaults."FLAG"` is silently
ignored on 1.8.0.

## Rate Limiting

| Property | Value | Note |
|---|---|---|
| `polaris.rate-limiter.filter.type` | `default` or `no-op` | **Not** `token-bucket` |
| `polaris.rate-limiter.token-bucket.requests-per-second` | `"5"` | Bucket refill |
| `polaris.rate-limiter.token-bucket.window` | `PT1S` | ISO-8601 duration |

Measured: with `filter.type=token-bucket` every API request, including token requests, returned
500 (`No bean found for required type [interface org.apache.polaris.service.ratelimiter.RateLimiter] ... Identifier(value="token-bucket")`)
while the health check stayed green. With `default`, 5/s and `PT1S`, 30 rapid calls gave
7 x 200 and 23 x 429; a token request in the other realm right after returned 200 (one bucket per
realm). Legitimate bursts also get 429, so size the bucket for your engines.

## Token Broker

| Property | Value |
|---|---|
| `polaris.authentication.token-broker.type` | `rsa-key-pair` (default) or `symmetric-key` |
| `polaris.authentication.token-broker.rsa-key-pair.public-key-file` | `/secrets/public.key` |
| `polaris.authentication.token-broker.rsa-key-pair.private-key-file` | `/secrets/private.key` |

Measured: with mounted PEM keys (2048-bit RSA, PKCS8 private, SubjectPublicKeyInfo public) tokens
were RS256 and verified with the mounted public key. Shared key files are what several replicas need
to accept each other's tokens. `symmetric-key`
suits a single replica, at the cost that every verifier holds the signing secret.

```python
head, payload, sig = token.split(".")
public = serialization.load_pem_public_key(open("/secrets/public.key", "rb").read())
public.verify(b64(sig), f"{head}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
```

## Task Limits

`polaris.tasks.max-concurrent-tasks` (default 100) and `polaris.tasks.max-queued-tasks` (default
1000) bound background work such as purges. The examples set 200 / 1000; their effect was not
measured separately.

## Hardening Checklist

| Area | Setting | Trade-off |
|---|---|---|
| Storage | Keep `SUPPORTED_CATALOG_STORAGE_TYPES` to what you use; no insecure types | FILE gone for demos |
| Per tenant | `realm-overrides` for stricter realms | More configuration to review |
| Tenancy | `polaris.realm-context.require-header=true` | Every client must send the header |
| Tokens | RSA key files shared by replicas, rotated through the secret store | Key distribution |
| Overload | `filter.type=default` + bucket per realm | Bursts get 429 |
| Startup | Fix what the readiness checks list; drop `polaris.readiness.ignore-severe-issues` | `polaris-deployment` skill |

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Flag has no effect | `polaris.features.defaults."X"` form | `polaris.features."X"` |
| Every request 500, health green | `rate-limiter.filter.type=token-bucket` | `filter.type=default` |
| Unexpected 429s | Bucket too small for engine bursts | Raise `requests-per-second` / window |
| Tokens rejected by another replica | Replicas sign with different keys | Same mounted key pair everywhere |
| FILE catalog 400 although listed | `ALLOW_INSECURE_STORAGE_TYPES` missing | Set both flags |
| Storage type allowed in one realm, refused in another | A realm override | Check `realm-overrides` for that realm |

## Related

- [realms.md](realms.md), [polaris-cli.md](polaris-cli.md).
- `polaris-deployment` skill: persistence, storage/STS, observability, readiness checks.
- `polaris-catalog-federation` skill: the federation flags in use.

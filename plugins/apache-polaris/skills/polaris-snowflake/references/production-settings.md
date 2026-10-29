# Production Settings for a Polaris That Snowflake Reaches

## Concept

Snowflake needs a Polaris endpoint it can reach over HTTPS, which in practice means an internet-facing
service. Defaults meant for laptops are not acceptable there: in-memory state, root credentials
printed to the log, long-lived tokens. The checklist:

1. Durable persistence instead of `in-memory`
2. Restricted storage types and allowed locations per catalog
3. Real authentication and a deliberate token broker choice
4. Bounded token lifetime
5. No dev-mode root credentials. Bootstrap root explicitly and keep it in a vault
6. Credential rotation for service principals
7. Firewall allowlist for Snowflake's ranges, generated from `SYSTEM$ALLOWLIST()`
8. Secrets (client secrets, signing keys) in a secret manager, not in compose files

Persistence and deployment in depth are in the `polaris-deployment` skill of this plugin. This page covers
what was measured.

## Property reference (Polaris 1.x)

Polaris 1.x reads Quarkus properties (environment variables or `application.properties`). Older
YAML layouts with `metaStoreManager`, `defaultRealms`, `authenticator`, `featureConfiguration` and
`tokenBroker` belong to the pre-1.0 server and do nothing on 1.x.

| Intent | Polaris 1.x property | Default in 1.8.0 |
|---|---|---|
| Persistence backend | `polaris.persistence.type` | `in-memory` |
| Token broker | `polaris.authentication.token-broker.type` (`rsa-key-pair` / `symmetric-key`) | `rsa-key-pair` |
| Symmetric signing key | `polaris.authentication.token-broker.symmetric-key.secret` or `.symmetric-key.file` | none |
| Token lifetime | `polaris.authentication.token-broker.max-token-generation` (ISO duration) | `PT1H` |
| Rotation check | `polaris.features."ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING"` | `false` |
| Root bootstrap | `POLARIS_BOOTSTRAP_CREDENTIALS=<realm>,<client-id>,<client-secret>` | unset (dev mode) |

## Worked example (docker-compose excerpt, tested)

```yaml
  polaris-prod:
    image: apache/polaris:latest        # 1.8.0 when tested
    environment:
      POLARIS_BOOTSTRAP_CREDENTIALS: POLARIS,root,s3cr3t          # from a secret store in real life
      polaris.realm-context.realms: POLARIS
      polaris.features."ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING": "true"
      polaris.authentication.token-broker.type: symmetric-key
      polaris.authentication.token-broker.symmetric-key.secret: lab-only-symmetric-secret-change-me
      polaris.authentication.token-broker.max-token-generation: PT30M
```

Reading the token broker from a token (header of the JWT):

```python
import base64, json, requests
r = requests.post(f"{server}/api/catalog/v1/oauth/tokens", data={
    "grant_type": "client_credentials", "client_id": cid, "client_secret": secret,
    "scope": "PRINCIPAL_ROLE:ALL"}).json()
head = r["access_token"].split(".")[0]
json.loads(base64.urlsafe_b64decode(head + "=" * (-len(head) % 4)))["alg"], r["expires_in"]
```

## Measured results

| Setting | Result |
|---|---|
| Server without `POLARIS_BOOTSTRAP_CREDENTIALS` | Log line `realm: POLARIS root principal credentials: <client-id>:<client-secret>`, and those credentials got a token (200) |
| Default token broker | `alg` RS256, `expires_in` 3600 |
| `symmetric-key` + `PT30M` | `alg` HS256, `expires_in` 1800 |
| Rotation flag on, new principal with `NAMESPACE_LIST` | List namespaces 200 (not blocked) |
| Same, after admin `POST /principals/{name}/reset` | List namespaces 200 with the reset secret (not blocked) |
| Self-rotation `POST /principals/{name}/rotate` | 200, then list namespaces 200 with the new secret |

The rotation flag was accepted, but it blocked no call on 1.8.0 with in-memory persistence.
Don't rely on it alone. Rotate service-principal credentials on a schedule and update the
Snowflake integration after each rotation. A later release may enforce the flag, so re-test after
upgrades.

## Token broker choice

| Broker | Signing | Trade-off |
|---|---|---|
| `rsa-key-pair` (default) | RS256 | Key pair to manage. Verifiers need only the public key |
| `symmetric-key` | HS256, one shared secret | Simple, but every verifier holds the signing secret |

With several Polaris replicas, every replica must use the same key material. Otherwise a token
issued by one replica is rejected by another.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Root secret visible to anyone with log access | Dev mode (no bootstrap credentials) | Always set `POLARIS_BOOTSTRAP_CREDENTIALS` from a secret store |
| Catalogs and principals gone after restart | `in-memory` persistence | Configure a durable persistence backend |
| YAML settings ignored | Pre-1.0 configuration layout | Use Quarkus properties |
| Long-lived stolen tokens | Default 1 h lifetime or longer | Shorter `max-token-generation`. The OAuth client refreshes |
| Rotation never forced | Flag not enforced on 1.8.0 | Scheduled rotation outside Polaris |
| Snowflake blocked after IP changes | Hand-maintained allowlist | Generate it from `SYSTEM$ALLOWLIST()` |

## Related

- [service-principal.md](service-principal.md), [catalog-integration.md](catalog-integration.md)
- The `polaris-deployment` and `polaris-configuration-and-cli` skills of this plugin cover persistence, realms and the full property set

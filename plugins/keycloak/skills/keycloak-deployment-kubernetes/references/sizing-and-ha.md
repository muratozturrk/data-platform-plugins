# Sizing, deployment and high availability

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications and Cloud Native Data Security with OAuth (2026-11-17); API Security in Action, Türkiye mevzuatı (BG Rehberi), Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Size CPU from measured rates (1 vCPU per 15 password logins/s, per 120 client-credentials grants/s and per 120 refreshes/s), add 150 % headroom, set CPU and memory `resources` explicitly in the CR, and confirm with load tests. Why: password hashing and TLS dominate CPU, throttled pods degrade sharply, and caches must refill after restarts. (research: Keycloak HA guide "Concepts for sizing CPU and memory resources"; Keycloak 26.4 benchmark, 2025-10; Keycloak docs 26.8, p. 16)
2. Keep all Keycloak nodes of one cluster in one region, and run multi-cluster v2 only inside one region. Why: p99 latency rises steeply with inter-node delay, and multi-cluster v2 needs database commit latency below 10 ms. (research: Keycloak 26.4 benchmark; Keycloak docs 26.8, p. 16; HKM D21, D25)
3. Back up the database with point-in-time recovery and test restores. Why: realms, users, keys and persisted sessions live there. (plugin v1)
4. Build one optimized image from the vendor image plus providers and themes (never from source, never by copying providers into a running pod), keep secrets out of it, pin its version and promote the same digest through every stage. Why: Keycloak discovers providers only at start-up, start-up is faster, every pod and stage runs identical code, and registries hold no secrets. (research: Keycloak containers guide; Keycloak IAM ch. 13; Cloud Native OAuth ch. 11; Zero Trust Networks ch. 7)
5. Serve the admin console, Admin REST API, health and metrics only on an internal hostname (`hostname-admin`) or the management port, block `/admin/` and `/realms/master/` at the public ingress, and do not let the Operator create the ingress. Why: the admin surface is the highest-value target, a separate admin hostname hides the console but does not block admin calls through the public URL, and the Operator's default ingress does not block admin endpoints. (research: Keycloak hostname guide; Keycloak docs 26.8, p. 13, p. 15, p. 16; Keycloak IAM ch. 9, 14; Cloud Native OAuth ch. 1, 3, 11; BG Rehberi 3.1.12.20; HKM D21)

## Rules by topic

### Production configuration checklist

Source: Keycloak 26.8.2 documentation, Server guides: production configuration, hostname, management interface; Operator guides: basic deployment, advanced configuration; Keycloak – IAM for Modern Applications, ch. 9, 13; Cloud Native Data Security with OAuth, ch. 3, 11 (merged 2026-11-17); API Security in Action, ch. 10; Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 7 (merged 2026-11-18).

1. Set Require SSL to all requests in every realm. Why: the weaker setting enforces TLS only for external clients and allows plain HTTP from private addresses. (Keycloak IAM ch. 9; Keycloak docs 26.8, p. 1, p. 13)
2. Run Keycloak with configuration from files and environment variables under infrastructure as code, readiness and liveness probes on the management port, metrics with security alarms, and JSON logs on standard output. Why: deployments become auditable, reversible and observable like any other workload. (Cloud Native OAuth ch. 3, 11)
3. Give the Keycloak CR a real hostname and a CA-issued TLS Secret, and never disable hostname strictness or enable plain HTTP outside development. Why: the CR can switch off these required options, which the guide reserves for development. (Keycloak docs 26.8, p. 16)
4. Restrict who may create or edit `Keycloak` and related CRs to namespace administrators, treat the image field as privileged, and use no `spec.unsupported.podTemplate`. Why: whatever image runs can read the Secrets used for environment variables, and `podTemplate` is Tech Preview. (Keycloak docs 26.8, p. 16)
5. Keep preview features (such as `http-optimized-serializers`) off in production, and keep theme caching on. Why: preview features are not for production, and Keycloak caches templates and theme configuration for performance. (Keycloak docs 26.8, p. 15; Keycloak IAM ch. 13)
6. Extend the Operator NetworkPolicy so that only the ingress namespace reaches HTTPS and only the monitoring namespace reaches management port 9000, give every Keycloak pod policies of both types (Ingress and Egress) with egress limited to its known targets, and review all policies of the namespace together. Why: by default only the clustering port is closed and HTTP(S) is open to every namespace and the outside world, and allowed traffic is the union of all policies, so one loose policy opens lateral movement. (Keycloak docs 26.8, p. 16; API Security in Action ch. 10)
7. Route traffic only on `/health/ready` on the management port, and set `http-max-queued-requests`. Why: ports open while Keycloak is still initializing, readiness hides migrating nodes, and the queue limit sheds overload with 503. (Keycloak docs 26.8, p. 15, p. 17)
8. Run Keycloak pods as non-root with least privilege, keep the running production image under continuous vulnerability scanning, and give every automated response (restart, scale-down, disabling a client) a fail-safe such as never dropping below three running Keycloak pods. Why: isolation limits lateral movement, and unguarded automation can cause the outage it reacts to. (Zero Trust Networks ch. 7; HKM D21)

Example (resource numbers from the documentation's worked example; confirm by load test):

```yaml
apiVersion: k8s.keycloak.org/v2beta1
kind: Keycloak
metadata: { name: hkm-sso, namespace: keycloak }
spec:
  instances: 3
  image: registry.hkm.example/keycloak-hkm@sha256:<digest>   # pinned optimized image
  hostname:
    hostname: https://sso.hkm.example
    admin: https://sso-admin.hkm.internal
  http:
    tlsSecret: sso-hkm-tls
  proxy:
    headers: forwarded
  ingress:
    enabled: false
  db:
    vendor: postgres
    host: hkm-keycloak-db-rw.keycloak.svc
    usernameSecret: { name: kc-db, key: username }
    passwordSecret: { name: kc-db, key: password }
  update:
    strategy: Auto
  resources:
    requests: { cpu: "1", memory: 1250Mi }
    limits:   { cpu: "2500m", memory: 1360Mi }
  additionalOptions:
    - { name: db-tls-mode, value: verify-server }
    - { name: proxy-trusted-addresses, value: 10.42.0.0/16 }   # ingress pod CIDR
    - { name: https-protocols, value: "TLSv1.3,TLSv1.2" }
    - { name: http-max-queued-requests, value: "1000" }        # illustrative
    - { name: log-console-output, value: json }
    - { name: metrics-enabled, value: "true" }
    - { name: health-enabled, value: "true" }
```

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: keycloak-ingress-egress, namespace: keycloak }
spec:
  podSelector: { matchLabels: { app: keycloak } }
  policyTypes: [Ingress, Egress]
  ingress:
    - from: [{ namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: ingress } } }]
      ports: [{ port: 8443 }]
  egress:
    - to: [{ podSelector: { matchLabels: { cnpg.io/cluster: hkm-keycloak-db } } }]
      ports: [{ port: 5432 }]
    - to: [{ podSelector: { matchLabels: { app: keycloak } } }]
      ports: [{ port: 7800 }, { port: 57800 }]
    # plus DNS, OpenBao, SMTP, institution IdPs and back-channel logout targets, each listed explicitly
```

### Database, caching and sessions

Source: Keycloak 26.8.2 documentation, Server guides: database, distributed caches; High Availability guides; Server Admin: Managing user sessions; Keycloak – IAM for Modern Applications, ch. 9, 10, 14 (merged 2026-11-17).

1. Run one external PostgreSQL 14 to 18 shared by all nodes and outside the Operator (for example CloudNativePG with a synchronous replica in another zone), and never use `dev-file`. Why: the database decides performance, availability and integrity of the whole cluster, the Operator does not manage it, and `dev-file` is for development only. (Keycloak docs 26.8, p. 15, p. 16; Keycloak IAM ch. 9)
2. Authenticate Keycloak to the database with strong (preferably certificate) credentials, connect with `db-tls-mode=verify-server`, and encrypt data and backups at rest. Why: the database holds users, credentials and by default private keys, and backups leak the same data. (Keycloak docs 26.8, p. 15; Keycloak IAM ch. 10, 14)
3. Allow database connections only from Keycloak pods, block outbound connections from the database, and give no person standing access. Why: write access to the database lets an attacker change configuration or credentials and impersonate anyone. (Keycloak IAM ch. 14)
4. Size the connection pool from load tests and raise it when logs show pool exhaustion. Why: an undersized pool fails requests under peak load. (Keycloak IAM ch. 9)
5. Keep persistent user sessions, do not override the cache XML, and configure no sticky sessions. Why: volatile sessions, sticky-session routing options and XML overrides are deprecated or unsupported in 26.8, and any pod must be able to serve any step of a flow. (Keycloak docs 26.8, p. 15; Cloud Native OAuth ch. 11; HKM D21)
6. Size caches deliberately: raise the offline session cache when many offline tokens are expected, and above 2500 concurrent clients grow the `users` cache 2x and the `realms` cache 4x the client count. Why: by default only 10 000 entries stay in memory and the rest load from the database on demand. (Keycloak docs 26.8, p. 5, p. 16)

Example:

```yaml
spec:
  db:
    vendor: postgres
    host: hkm-keycloak-db-rw.keycloak.svc
    poolMaxSize: 50          # from load test
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: keycloak-db-ingress, namespace: keycloak }
spec:
  podSelector: { matchLabels: { cnpg.io/cluster: hkm-keycloak-db } }
  policyTypes: [Ingress, Egress]
  ingress:
    - from: [{ podSelector: { matchLabels: { app: keycloak } } }]
      ports: [{ port: 5432 }]
  egress: []
```

### Clustering, high availability and disaster recovery

Source: Keycloak 26.8.2 documentation, Server guides: distributed caches; Operator and High Availability guides; Keycloak – IAM for Modern Applications, ch. 9, 14; Cloud Native Data Security with OAuth, ch. 11 (merged 2026-11-17).

1. Run at least three instances spread over nodes and zones, with `whenUnsatisfiable: DoNotSchedule` or required anti-affinity. Why: one pod is a single point of failure, the default spread is only preferred, and cache entries with two owners are lost if both sit on the failed node or zone. (Keycloak docs 26.8, p. 15, p. 16; Keycloak IAM ch. 9; Cloud Native OAuth ch. 11; HKM D21)
2. Use the `jdbc-ping` cache stack, open cluster ports 7800 and 57800 only between Keycloak nodes, and encrypt cluster traffic instead of relying on cluster membership authentication alone. Why: the `kubernetes`, `tcp`, `udp` and `jdbc-ping-udp` stacks are deprecated, and membership authentication stops rogue members joining but not outsiders talking to members. (Keycloak docs 26.8, p. 15; Keycloak IAM ch. 14)
3. Run multi-cluster v2 with synchronous database replication, commit latency below 10 ms (target below 5 ms), NTP-synchronised clocks with skew within 10 s, and an external load balancer probing `/lb-check`. Why: these are the stated infrastructure requirements, and without them failover loses data or routes to dead sites. (Keycloak docs 26.8, p. 16; HKM D21)
4. Monitor for degraded states and restore failed nodes promptly. Why: "no data loss" holds only when the setup is not already degraded by an earlier failure. (Keycloak docs 26.8, p. 16)
5. Before go-live, stop the pod serving an admin session and confirm the session survives, then fetch the discovery document through the ingress from every pod and confirm it lists only public URLs. Why: this proves session replication, failover and the hostname setup. (Keycloak IAM ch. 9)

Example:

```yaml
spec:
  scheduling:
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector: { matchLabels: { app: keycloak } }
  additionalOptions:
    - { name: cache-stack, value: jdbc-ping }
```

### Reverse proxy, hostname and TLS

Source: Keycloak 26.8.2 documentation, Server guides: hostname (v2), reverse proxy; Keycloak – IAM for Modern Applications, ch. 9, 14; Cloud Native Data Security with OAuth, ch. 8, 10, 11 (merged 2026-11-17); API Security in Action, ch. 10; Learning Digital Identity (Windley, 2023), ch. 8, 10; Zero Trust Networks, 2nd ed., ch. 2, 8, 9, 10 (merged 2026-11-18).

1. Set `hostname` on every node to one full, neutral public URL (`https://sso.hkm.example`) with no product, vendor or other sensitive context in the host name, serve both realms from it with the realm only in the path (`/realms/hkm`, `/realms/hkm-public`), never let Keycloak build URLs from the incoming Host header, and never change the hostname after go-live. Why: the hostname sets the issuer, links and cookie scope, a forged Host header can put an attacker's URL into password-reset emails, TLS hides the path but not the host name so a revealing name leaks what it protects, and issuers and redirect URIs stay valid only while the public name stays fixed. (Keycloak docs 26.8, p. 13, p. 15; Keycloak IAM ch. 9, 14; Cloud Native OAuth ch. 11; API Security in Action ch. 10; Learning Digital Identity ch. 8, 10)
2. Advertise backend endpoints (token, introspection, UserInfo, JWKS) under the public URL, let workloads reach Keycloak through its cluster-internal URL, and define a public URL for every stage. Why: the issuer must match for every caller, internal calls should not leave the cluster, and early stages need real URLs to rehearse. (Keycloak IAM ch. 9; Cloud Native OAuth ch. 11)
3. Make the central ingress the only public entry, terminate there with TLS re-encrypt to the pods, and keep Keycloak, APIs and databases on `ClusterIP`, with the edge blocked from business databases. Why: exposing pods bypasses the proxy's controls, and a breached edge then yields no connection strings. (Keycloak IAM ch. 9, 14; Cloud Native OAuth ch. 8; API Security in Action ch. 10; HKM D21)
4. Set `proxy-headers` to the standard `forwarded` header and `proxy-trusted-addresses` to the ingress IPs or CIDRs only, and make the ingress overwrite client forwarding headers and strip tracing headers. Why: client-supplied forwarding headers are spoofable, and hostname entries in the trusted list are deprecated. (Keycloak docs 26.8, p. 15; Keycloak IAM ch. 9)
5. Encrypt and authenticate every hop that carries tokens or credentials, including gateway to API, workload to Keycloak, engines to Ranger and remote administrative access, with CA-signed certificates, keys of at least 2048 bits, TLS 1.2 minimum and 1.3 preferred (TLS 1.3 only once every client, including engines, institution IdPs, LDAP and back-channel logout targets, is confirmed to support it), forward-secret key exchange and authenticated-encryption cipher suites only (for example AES-GCM; no MD5, SHA-1 or non-AEAD suites); use a public certificate only for `sso.<domain>` and, for internal traffic, a private CA that internal links trust exclusively, whose root stays offline behind per-cluster intermediates issuing short-lived, automatically renewed certificates, with issuance automated but certificates the platform classes as highly sensitive approved by a person. Why: in-cluster traffic is plain by default, an unencrypted internal hop exposes codes and tokens, encryption without authentication allows forged or replayed messages, any public CA could mint a certificate for an internal service, and short-lived certificates limit the damage of a stolen key where revocation is weak. (Keycloak IAM ch. 9, 14; Cloud Native OAuth ch. 10, 11; API Security in Action ch. 10; Zero Trust Networks ch. 2, 8, 9, 10; HKM D21; decision 2026-11-18)
6. Expose only `/realms/`, `/resources/`, `/.well-known/` and `/lb-check` publicly, keep `/admin/` and `/realms/master/` internal, and never proxy port 9000. Why: the other paths are needless attack surface. (Keycloak docs 26.8, p. 15; Cloud Native OAuth ch. 1, 11)

Example:

```yaml
# HTTPRoute for the public hostname: allow-list of paths only
spec:
  hostnames: [sso.hkm.example]
  rules:
    - matches:
        - path: { type: PathPrefix, value: /realms/hkm/ }
        - path: { type: PathPrefix, value: /realms/hkm-public/ }
        - path: { type: PathPrefix, value: /resources/ }
        - path: { type: PathPrefix, value: /.well-known/ }
        - path: { type: Exact, value: /lb-check }
      backendRefs: [{ name: hkm-sso-service, port: 8443 }]
```

### Performance and load testing

Source: Keycloak 26.8.2 documentation, High Availability guides: sizing; Keycloak – IAM for Modern Applications, ch. 9; Cloud Native Data Security with OAuth, ch. 11 (merged 2026-11-17).

1. Start memory at a 1250 MB request per pod plus expected session growth, derive the limit as (expected memory − 300 MB) / 0.7, and confirm both with a load test. Why: heap takes 70 % of the limit and non-heap about 300 MB, and the base figure covers realm caches plus 10 000 sessions. (Keycloak docs 26.8, p. 16)
2. Load-test Keycloak to find its limits, pick the ingress balancing algorithm and pool size from the results, and scale on OAuth metrics such as p95 token request time rather than on CPU alone. Why: generic container metrics are weak scaling signals. (Cloud Native OAuth ch. 11; Keycloak IAM ch. 9)

---
name: ranger-deployment-kubernetes
description: Deploying Ranger on Kubernetes for the platform when no official Helm chart exists: building and pinning images (Admin, usersync, plugin layers), PostgreSQL backing store, install.properties and ranger-admin-site settings, TLS, HA Admin replicas, single usersync, secrets via OpenBao + External Secrets, audit store choice (OpenSearch/Solr), sizing, probes, backups and environment layout. Use when installing, sizing or restructuring a Ranger deployment.
---

# Ranger Deployment on Kubernetes

## Packaging reality
- Apache Ranger publishes source/binary tarballs and Docker build tooling (`dev-support/ranger-docker`: ranger, ranger-usersync, ranger-tagsync, ranger-kms, db, solr, zookeeper compose files); there is **no official Helm chart**. Options: build your own images + a small in-house chart (platform default), the Canonical `ranger-k8s` charmed operator (Juju), or vendor charts (pin older Ranger versions). Check Docker Hub for `apache/ranger*` images for the pinned version; otherwise build from the release tag.
- Plugins are not sidecars: plugin jars and configs must be inside the engine's own image/classpath (Trino ships its authorizer; Kafka and Spark images need a plugin layer).

## Components and layout
| Workload | Replicas | Notes |
|---|---|---|
| ranger-admin (Deployment) | 2+ | stateless; port 6080 HTTP / 6182 HTTPS; `setup.sh` + DB schema upgrade run as an init Job, not on every pod start |
| PostgreSQL | HA via platform operator | databases `ranger` (and audit DB is no longer used — audits go to Solr/ES); pgBackRest backups |
| ranger-usersync (Deployment) | **1** | LDAP/file source; leader-less — never scale > 1 |
| ranger-tagsync | 0 | not deployed; tags pushed via REST from the control plane/OpenMetadata |
| Audit store | OpenSearch (platform) or small Solr | Admin's audit UI reads Solr or Elasticsearch-compatible stores; verify the Ranger ES client against the platform's OpenSearch version in staging (compatibility mode may be required) |
| Namespace | `platform-ranger` | NetworkPolicy: engines → Admin 6182 only; Admin → PostgreSQL, audit store, LDAP |

## Key configuration (`install.properties` → `ranger-admin-site.xml`)
`DB_FLAVOR=POSTGRES`, `db_host`, `db_name`, `db_user`/`db_password` (from Secret), `rangerAdmin_password`, `rangerUsersync_password`, `rangerTagsync_password`, `keyadmin_password` (all changed, from OpenBao), `policymgr_external_url=https://ranger.platform.svc:6182`, `policymgr_https_keystore_file`, `audit_store=elasticsearch|solr` with `audit_elasticsearch_urls/port/protocol/user/password/index` (or `audit_solr_urls`), `authentication_method=LDAP|ACTIVE_DIRECTORY|UNIX|NONE` (see identity skill for UI login), `ranger.jpa.jdbc.maxpoolsize`, `ranger.service.https.attrib.ssl.enabled=true`, `ranger.spnego.*` only if Kerberos is used.
2.8: enable JWT auth for plugin ↔ Admin where engines support it; otherwise 2-way TLS (`ranger-policymgr-ssl.xml`) and restrict `policy.download.auth.users` per service to the plugin principal.

## Sizing (starting points)
Admin 2 vCPU / 4 GiB (JVM `-Xmx2g`) per replica serves hundreds of plugins and tens of thousands of policies; policy download is cached with ETags/versions so idle polling is cheap. PostgreSQL small (GBs). Audit store sized by audit volume: events/day ≈ queries × tables touched (+ Kafka requests if audited) — sample before sizing; use ISM/ILM rollover and retention per KVKK policy.

## Probes and readiness
Liveness/readiness: `GET /` login page or `/service/public/v2/api/service` with a probe user; startup probe generous (schema checks). Usersync: process + last-sync metric. PodDisruptionBudget for Admin.

## Secrets
ExternalSecret → Kubernetes Secret → env/volume for DB, admin passwords, keystores, LDAP bind, audit store credentials; rotate by rolling Admin; plugin credentials rotated per engine.

## Environments
dev / staging / prod Ranger instances (never share one Ranger across environments); policies promoted through Git; staging holds the rehearsal DB copy for upgrades.

## Anti-patterns
- Running `setup.sh` in every pod start with multiple replicas (schema races)
- Default passwords; HTTP only
- Multiple usersync replicas
- Embedded Solr without persistence as the only audit store
- Baking secrets into images

Sources: `references/sources.md` (Ranger installation wiki, ranger-docker tooling, Canonical operator README, community K8s walkthroughs, release notes 2.7/2.8).

# The `polaris` CLI

## Concept

The Polaris CLI (PyPI package `apache-polaris`, command `polaris`) wraps the management API and
parts of the Iceberg REST API. It turns governance changes (catalogs, principals, roles, grants,
namespaces) into short, scriptable commands for operations scripts and CI pipelines. Every command is an
HTTP call underneath (the endpoints are in the `polaris-catalog-fundamentals` skill).

```
polaris [global-options] <command> <subcommand> [options] [arguments]
```

Commands: `catalogs`, `principals`, `principal-roles`, `catalog-roles`, `namespaces`,
`privileges`, `profiles`. Tested with `apache-polaris==1.8.0` against Polaris 1.8.0.

## Global Options and Authentication

| Option | Meaning | Measured |
|---|---|---|
| `--host` / `--port` | Server; default `localhost:8181` | Without `--host` in a container: exit 1 (nothing on localhost) |
| `--base-url` | Alternative to host/port: the **server root** | `http://polaris:8181` exit 0; `http://polaris:8181/api/catalog/v1` exit 1 |
| `--client-id` / `--client-secret` | OAuth client credentials | exit 0 |
| `CLIENT_ID` / `CLIENT_SECRET` env | Same, from the environment | exit 0 |
| `--access-token` | Use an existing token | Together with client ID: exit 2, `... either --client-id & --client-secret or --access-token, but not both` |
| `--realm` | Target realm | see [realms.md](realms.md) |
| `--profile` / `CLIENT_PROFILE` env | Saved connection | both exit 0 |

| Method | Use when | Trade-off |
|---|---|---|
| Flags | One-off admin work | Secrets in shell history |
| `CLIENT_ID` / `CLIENT_SECRET` | Scripts, CI | Visible to the process tree |
| Profiles | Several environments or realms | Secrets stored in `~/.polaris` |
| REST API | Applications | More code, full control |

`polaris profiles create dev` prompts, in order, for client ID, client secret, host, port,
scheme, realm and realm header name; `polaris --profile dev catalogs list` or
`CLIENT_PROFILE=dev polaris catalogs list` then need no other connection options.

## Command Reference (1.8.0 forms)

| Task | Command |
|---|---|
| FILE catalog (server must allow FILE) | `polaris catalogs create --storage-type file --default-base-location file:///tmp/polaris my_catalog` |
| S3 catalog | `polaris catalogs create --storage-type s3 --default-base-location s3://my-bucket/polaris-data --role-arn arn:aws:iam::1234567890:role/my-polaris-role my_cloud_catalog` |
| List / get | `polaris catalogs list`, `polaris catalogs get my_catalog` |
| Set a catalog property | `polaris catalogs update my_catalog --set-property environment=dev` |
| Principal | `polaris principals create data_engineer_1` |
| Principal with property | `polaris principals create --property team=analytics data_scientist_1` |
| New secret (admin) | `polaris principals reset data_engineer_1` |
| Rotate own secret | `polaris principals rotate-credentials <name>` (as that principal) |
| List / get | `polaris principals list`, `polaris principals get data_engineer_1` |
| Principal role | `polaris principal-roles create analyst_role` |
| Assign to principal | `polaris principal-roles grant --principal data_scientist_1 analyst_role` |
| Catalog role | `polaris catalog-roles create --catalog my_catalog limited_reader` |
| Attach to principal role | `polaris catalog-roles grant --catalog my_catalog --principal-role analyst_role limited_reader` |
| Namespace (parent first) | `polaris namespaces create --catalog my_catalog analytics`, then `... analytics.q1` |
| List namespaces | `polaris namespaces list --catalog my_catalog` |
| Catalog privilege | `polaris privileges catalog grant --catalog my_catalog --catalog-role limited_reader CATALOG_MANAGE_CONTENT` |
| Table privilege | `polaris privileges table grant --catalog my_catalog --catalog-role limited_reader --namespace analytics.q1 --table sales_data TABLE_READ_DATA` |
| Revoke | `polaris privileges table revoke ... --table sales_data TABLE_READ_DATA` (no `--cascade`) |
| List grants | `polaris privileges list --catalog my_catalog --catalog-role limited_reader` |

## Syntax from Older Docs That Fails on 1.8.0

Of 18 commonly documented entity commands, 14 ran unchanged; these 4 did not:

| Command as often written | Exit / error | 1.8.0 form |
|---|---|---|
| `catalogs update my_catalog --property environment=dev` | 2, unrecognized argument | `--set-property environment=dev` |
| `principals rotate-credentials data_engineer_1` as root | 1, 403 `not authorized for op ROTATE_CREDENTIALS` | Admin: `principals reset data_engineer_1`; rotate only as the principal itself |
| `namespaces create --catalog my_catalog analytics.q1` (no parent) | 1, parent does not exist | Create `analytics` first |
| `privileges table revoke ... --cascade TABLE_READ_DATA` | 1, 501 Not Implemented | Drop `--cascade` |

Further measured behavior: a table grant needs an existing table; on a missing namespace the
server answered with a misleading `CatalogRole not found`, on a missing table with
`NoSuchTableException`. The FILE `catalogs create` line fails on a default server
(`Unsupported storage type: FILE`).

## Worked Example: Onboarding a Read-Only Analyst

```bash
export CLIENT_ID=root CLIENT_SECRET=s3cr3t
P="polaris --host polaris --port 8181"
$P principals create --property team=analytics data_scientist_1     # prints clientId/clientSecret
$P principal-roles create analyst_role
$P principal-roles grant --principal data_scientist_1 analyst_role
$P catalog-roles create --catalog my_catalog limited_reader
$P catalog-roles grant --catalog my_catalog --principal-role analyst_role limited_reader
$P privileges table grant --catalog my_catalog --catalog-role limited_reader \
   --namespace analytics.q1 --table sales_data TABLE_READ_DATA
$P privileges list --catalog my_catalog --catalog-role limited_reader
```

Measured end state of the full session: `my_catalog` properties include `environment=dev`,
`data_scientist_1` has `{"team": "analytics"}`, and after the revoke `limited_reader` holds only
`{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}`.

Output format: `get`/`list` print JSON objects (one per line for lists), so `jq` or
`json.loads` per line works in scripts.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Connection refused from a container/CI | Default `localhost:8181` | `--host`/`--port` or `--base-url http://host:8181` |
| `--base-url` fails | URL includes `/api/catalog/v1` | Server root only |
| Exit 2 `... but not both` | Client ID and access token together | Pick one auth method |
| 403 on `rotate-credentials` | Root rotating another principal | `principals reset` |
| 501 on revoke | `--cascade` | Drop it |
| `CatalogRole not found` on a table grant | Namespace missing (misleading message) | Create namespace and table first |

## Related

- [realms.md](realms.md): `--realm`, the realm header, per-realm root principals.
- [feature-flags-and-hardening.md](feature-flags-and-hardening.md): why FILE catalogs fail by default.
- `polaris-deployment` skill: the same first-user setup as raw HTTP calls.

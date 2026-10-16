# Fine-Grained Accessor for Resources

Category: Access Control · Layer: cloud IAM / broker ACLs

## Problem
A security audit found overly broad permissions: one data processing job could overwrite every
dataset in the object store. The auditor asks for **least privilege**: each component gets only
the permissions it needs, so a job can manipulate only the dataset it works on.

## Solution / Mechanics
All major clouds implement least privilege with two strategies:

1. **Resource-based**: the access scope is attached to the resource itself. A GCS bucket IAM
   policy (`google_storage_bucket_iam_policy`), an S3 bucket policy naming the
   principal (`aws s3api put-bucket-policy`).
2. **Identity-based**: permissions are attached to the identity (human or application user). On
   AWS, an IAM role assumed by the service (EMR role with Kinesis `Get*/Describe*/List*/Put*` on
   one stream ARN; S3 reader role scoped to one bucket).

Scope can be one exact resource, a **prefix** (`visits*`), or a **runtime condition** (tags,
`${www.amazon.com:user_id}`), the resource-level analog of row-level security.

```
 resource-based                         identity-based
 +--------------+                       +-------------------+
 | bucket visits|  policy:              | role visits-job   |  policy:
 |  Principal = |  Allow Get/List       |  Allow Get/List   |  on arn:...:visits/*
 |  job-role    |                       +---------+---------+
 +------+-------+                                 | sts:AssumeRole
        ^ request                                 v
   job (any identity) -------------------->  job (assumes role) ---> bucket visits
        X write to bucket raw  -> AccessDenied (nothing grants it)
```

## Consequences & trade-offs
- **Security vs usability trade-off**: strict least privilege creates many small
  policies that are hard to maintain. Wildcards (`visits*`) reduce the count but grant access to
  future resources with that prefix. Agree on wildcards with the security team.
- **Complexity**: mixing resource- and identity-based policies in one project makes
  effective permissions hard to reason about. Prefer one model, the one covering most cases.
- **Quotas**: AWS IAM allows 1,500 custom policies by default, GCP 300 custom roles per
  project. Some limits can be raised.
- Defining policies is not hard technically: use IaC or scripts.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Jobs share one broad cluster role | Yes, role per job or per bucket | - |
| Cross-account consumer of one bucket | Resource-based policy | - |
| Many jobs, each touching its own datasets | Identity-based roles | - |
| Thousands of per-user resources | Condition/tag-based policies | Per-resource policies hit quotas |
| Column/row restrictions inside a table | No | Fine-Grained Accessor for Tables |
| Kafka topics/groups | Yes, ACLs per principal | - |

## Key design decisions
- One model per project (resource vs identity), the exception documented.
- Granularity: job, team or domain role. Finer = safer, more policies.
- Exact ARNs vs prefixes: prefixes only within a namespace you own.
- Separate read and write identities so a bug in a writer can't touch inputs.
- Policy-as-code: Terraform/CDK, peer-reviewed, plus a negative test expecting AccessDenied.

## Gotchas & best practices
- `s3:ListBucket` applies to the bucket ARN, object actions to `bucket/*`: list both resources.
- Explicit `Deny` wins over any `Allow`; use it for guardrails (e.g. deny writes to raw zone).
- Kafka: once an authorizer runs with `allow.everyone.if.no.acl.found=false`, everything not
  granted is denied. `PREFIXED` ACLs are the Kafka version of wildcards.
- Consumer groups need their own `READ` ACL besides the topic.
- Prefer short-lived assumed-role sessions over long-lived users with keys.
- Use IAM Access Analyzer / policy simulator before deploying.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `AccessDenied ... ListObjectsV2` | Policy only on `bucket/*` | Add bucket ARN for `s3:ListBucket` |
| Job still writes everywhere | Old broad policy still attached | Detach; verify with negative test |
| `GroupAuthorizationException` | Group ACL missing | `READ` on group resource |
| Policy creation fails | Quota reached | Consolidate with tags/conditions, request increase |
| Access works for new unrelated dataset | Prefix wildcard too wide | Narrow prefix, use exact ARNs |
| AssumeRole denied | Trust policy doesn't list the caller | Fix `assume_role_policy` principal |

## Implementation notes per stack
**Spark** — `examples/fine_grained_accessor_for_resources/spark/per_bucket_assumed_role_job.py`:
S3A per-bucket assumed roles, read-only input role and write-only output role.
```python
def bucket_role_conf(bucket: str, role_arn: str) -> dict:
    prefix = f"spark.hadoop.fs.s3a.bucket.{bucket}"
    return {
        f"{prefix}.aws.credentials.provider": ASSUMED,
        f"{prefix}.assumed.role.arn": role_arn,
        f"{prefix}.assumed.role.session.duration": "1h",
        f"{prefix}.assumed.role.credentials.provider":
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    }
```

**Kafka** — `examples/fine_grained_accessor_for_resources/kafka/least_privilege_acls.py`:
```python
POLICY = [
    ("User:visits-processor", TOPIC, "visits_raw", LITERAL, AclOperation.READ),
    ("User:visits-processor", GROUP, "visits-processor", LITERAL, AclOperation.READ),
    ("User:visits-processor", TOPIC, "visits_enriched", LITERAL, AclOperation.WRITE),
    ("User:visits-reporting", TOPIC, "visits_enriched", PREFIXED, AclOperation.READ),
]
```

**Airflow** — `examples/fine_grained_accessor_for_resources/airflow/scoped_roles_dag.py`:
one AWS connection (`role_arn` extra) per task, policies scoped to one bucket per role, and a
`prove_isolation` task that **fails** unless the export role gets `AccessDenied` on the raw bucket.

## Verification
- Positive test: each job identity performs exactly its documented actions.
- Negative test: the same identity gets `AccessDenied` on a neighboring bucket/topic (automate it,
  as the `prove_isolation` task does).
- IAM Access Analyzer reports no public or cross-account access you didn't intend.
- Unused permissions (last-accessed data) are reviewed and removed periodically.
- Kafka: `kafka-acls --list --principal User:<app>` matches the declared policy list.

## Related patterns
- Fine-Grained Accessor for Tables: column/row level inside tables.
- Secretless Connector: the role is also the credential; no keys to manage.
- Encryptor: KMS key policies and grants are resource-based policies too.
- Secrets Pointer: restrict `GetSecretValue` to each consumer's secret ARN.

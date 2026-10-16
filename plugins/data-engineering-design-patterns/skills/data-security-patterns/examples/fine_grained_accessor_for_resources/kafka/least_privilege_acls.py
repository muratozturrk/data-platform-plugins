"""Fine-Grained Accessor for Resources - least-privilege Kafka ACLs with confluent-kafka AdminClient.

Each application principal gets only the operations it needs on the exact
resources it uses:
  * visits-processor : READ `visits_raw` + its consumer group, WRITE `visits_enriched`
  * visits-reporting : READ topics with PREFIX `visits_enriched` (wildcard trade-off:
                       future topics with that prefix are readable too - agree with security)
Everything else is denied by default once the broker runs with an authorizer and
`allow.everyone.if.no.acl.found=false`.

Run:
  pip install confluent-kafka
  export KAFKA_BOOTSTRAP=localhost:9093 KAFKA_SASL_USER=admin KAFKA_SASL_PASSWORD=...
  python least_privilege_acls.py apply|list
"""
import os
import sys

from confluent_kafka.admin import (AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
                                   AdminClient, ResourcePatternType, ResourceType)

ADMIN_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9093"),
    "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": os.environ.get("KAFKA_SASL_USER", ""),
    "sasl.password": os.environ.get("KAFKA_SASL_PASSWORD", ""),
}

LITERAL, PREFIXED = ResourcePatternType.LITERAL, ResourcePatternType.PREFIXED
TOPIC, GROUP = ResourceType.TOPIC, ResourceType.GROUP

# (principal, resource type, name, pattern, operation)
POLICY = [
    ("User:visits-processor", TOPIC, "visits_raw", LITERAL, AclOperation.READ),
    ("User:visits-processor", GROUP, "visits-processor", LITERAL, AclOperation.READ),
    ("User:visits-processor", TOPIC, "visits_enriched", LITERAL, AclOperation.WRITE),
    ("User:visits-reporting", TOPIC, "visits_enriched", PREFIXED, AclOperation.READ),
    ("User:visits-reporting", GROUP, "visits-reporting", LITERAL, AclOperation.READ),
]


def to_binding(principal, resource_type, name, pattern, operation) -> AclBinding:
    return AclBinding(resource_type, name, pattern, principal, "*", operation, AclPermissionType.ALLOW)


def apply_policy(admin: AdminClient) -> None:
    bindings = [to_binding(*rule) for rule in POLICY]
    for binding, future in admin.create_acls(bindings).items():
        future.result()  # raises on failure; creating an existing ACL is a no-op
        print(f"granted {binding.operation.name} on {binding.restype.name}:{binding.name} to {binding.principal}")


def list_acls(admin: AdminClient) -> None:
    everything = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY, None, None,
                                  AclOperation.ANY, AclPermissionType.ANY)
    for binding in admin.describe_acls(everything).result():
        print(binding)


if __name__ == "__main__":
    client = AdminClient(ADMIN_CONFIG)
    action = sys.argv[1] if len(sys.argv) > 1 else "list"
    if action == "apply":
        apply_policy(client)
    elif action == "list":
        list_acls(client)
    else:
        raise SystemExit(f"unknown action {action}")

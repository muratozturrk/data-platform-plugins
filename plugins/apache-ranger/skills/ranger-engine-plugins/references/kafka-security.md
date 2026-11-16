# Kafka authorization notes

Status: starter rules; Azure mapping from the Azure docs and Authorization in Action; authorizer model and auditing partly filled from the Apache Ranger wiki and the verification lab (merged 2026-11-15); threat model, custom authorization, Kafka Connect pipelines, cross-cluster mirroring and administrative operations from Kafka: The Definitive Guide, 2nd ed. (merged 2026-11-15); principals and principal mapping partly filled from the same book, workload principals, client mechanism and quotas from the decision on conflicts #23 (2026-11-16); engine notes from Hadoop Security (merged 2026-11-15); event governance (topic owner and stream class, derived streams, self-service through Git) from Data Management at Scale, 2nd ed. (merged 2026-11-16); stream classes compile to topic prefixes (decision 2026-11-16, conflicts #58).

## Starter rules

1. Isolate each institution by a topic prefix and a consumer-group prefix (for example `t.acme.*` for both) with policies in the institution zone, granted on the prefixes to groups (service clients directly, conflicts #12), never per topic to individual people. Why: it maps scope to names, and per-resource grants to individual users do not scale. (plugin v1; Kafka DG ch. 11; conflicts #12.)
2. Put only broker, inter-broker, KRaft controller and operator service principals in the super-user list, never a human or a group, and keep that list in `ranger.plugin.kafka.super.users`, managed as code and checked by CI against an allow-list; do not use Kafka's `super.users`, never set `ranger.plugin.super.users` in a Ranger Admin service config, and let CI and the drift check compare both the file and the service configs. Why: the cluster must keep working when policies change, a Ranger super user passes even an explicit Ranger deny, RangerKafkaAuthorizer ignores `super.users`, and a service-config entry added a super user within 10 s without a broker restart, while a change to the file list took effect only after a restart. (plugin v1; Apache Ranger wiki p. 8; Kafka DG ch. 11, 12; trial: verification lab V3, 2026-11-14/15; decisions 2026-11-14/15, conflicts #4, #16, #31; decision 2026-11-15, conflicts #42: the same allow-list governs 2.9 configured super users and groups (RANGER-5627).)
3. Keep quotas, including Kafka's user and client configuration overrides, in Kafka (Strimzi) and out of institution role definitions and Ranger policies. Why: Ranger does not manage quotas, and these overrides limit resources rather than grant access. (plugin v1; Kafka DG ch. 12.)

### Azure model mapping on the Kafka path

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 02, 03, 04, 05, 07, 10, 11 (merged 2026-11-14); Authorization in Action, ch. 6, 11 (merged 2026-11-15); Attribute-Based Access Control, ch. 3 engine notes (merged 2026-11-15); Hadoop Security, ch. 6 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15); Data Management at Scale, 2nd ed., ch. 8 (merged 2026-11-16).

1. Put `publish`, `consume` and `describe` in the data lists of platform roles, and `create`, `delete`, `alter`, `configure` and `alter_configs` (the cluster and topic management operations, which form the service-level layer) in the management lists. Why: Azure keeps data actions and management actions apart, cluster operations are a gate separate from topic data access, and Kafka exposes management operations through the same authorizer as data access. (Azure docs 02; Hadoop Security ch. 6; Kafka DG ch. 11.)
2. Map domain scope to a topic-prefix wildcard together with the matching consumer-group prefix, and dataset scope to one topic; Kafka has no column level. Why: Ranger inherits only through wildcards, and a consumer needs its group as well as its topic. (Azure docs 03; Kafka DG ch. 11.)
3. Assign producers and consumers that run as applications as service principals, so the compiler emits user entries for them; people who read or write topics (for example members of `platform_t-acme_analyst`) get group entries only. Why: service clients have no meaningful group, and per-user grants for people do not scale. (Azure docs 04; Kafka DG ch. 11; conflicts #12.)
4. Compile topic-prefix conditions into wildcard topic resources (`t.acme.*`), never into Kafka's prefixed ACL pattern type, and a host restriction into an IP-range condition. Why: a location-prefix condition is a resource pattern in Ranger, and the Kafka ACL host field corresponds to Ranger's IP condition. (Azure docs 10, 11; Kafka DG ch. 11.)
5. Validate custom roles against the Kafka service definition's access types, and check that it enables deny and exceptions before relying on a guardrail. Why: access types and deny support differ per service definition. (Azure docs 05, 07.)
6. Remember that Kafka authorization ends at topic and consumer group: Ranger has no row or column level there, so an authorized consumer receives every message of the topic. Narrow Kafka access only through topic-prefix scope or tags; per-message sensitivity checks belong to the consumer, and a filtered copy is a governed dataset under conflicts #10. Why: consumers authorized for a topic still receive its sensitive messages. (Authorization in Action ch. 6 engine notes, 11.1.5; Attribute-Based Access Control ch. 3 engine notes; DMaS ch. 8; conflicts #10.)
7. Give every topic the highest tag of the fields it carries (the field tags in OpenMetadata roll up to the topic), and never carry `PII` or `RESTRICTED` values in clear on a `.pub.` topic: the producing pipeline publishes a de-identified stream (masked or tokenized per attribute-and-tag-design.md › Differentiated access, masking and tokenization principles, rule 8), and clear values stay on `.int.` topics that only pipeline identities may consume. Why: Kafka cannot mask or filter fields (rule 6), and a Data Reader assignment at domain scope compiles to consume on every `.pub.` topic of the domain. (trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #64.)

### Kafka Connect pipelines

Source: Kafka: The Definitive Guide, 2nd ed., ch. 9 (merged 2026-11-15). Worker identities: Principals and principal mapping, rule 4 (decision 2026-11-16, conflicts #23).

1. Before onboarding a connector, record four answers in its manifest entry: who may read the data it moves, who may change the pipeline, how it authenticates to the external system, and whether the data includes personal data under KVKK. Why: each answer maps to a separate control. (Kafka DG ch. 9.)
2. Limit a Connect worker principal's grants to its own institution's topic prefix and to its internal offset, configuration and status topics, and give it nothing outside them. Why: Connect keeps offsets, connector configuration and status in Kafka topics, and authorization stops a pipeline from moving sensitive topics into a less protected system. (Kafka DG ch. 9.)
3. Treat creating or changing a connector as a control-plane action separate from moving data: connector definitions come from Git through the control plane, and no institution principal calls the Connect REST API directly. Why: the REST API sits outside Ranger, and who may modify a pipeline is a security question of its own. (Kafka DG ch. 9.)

Example (rewritten, platform manifest):
```yaml
kind: Pipeline
metadata: { id: pl-t-acme-crm-sink }
spec:
  institution: t-acme
  readers: [platform_t-acme_analyst]               # who may read the data it moves
  changedBy: git:institutions/t-acme/connectors/   # control plane only, no direct REST calls
  externalAuth: openbao:kv/t-acme/crm-sink         # resolved at runtime by the config provider
  personalData: true                               # KVKK review before onboarding
  grants:
    consume: ["t.acme.crm.*"]
    internalTopics: ["t.acme.connect.*"]           # offsets, configs, status (naming: Unclear U6)
  principal: wi-t-acme-connect                     # Connect cluster identity (Principals rule 4)
```

### Cross-cluster mirroring (MirrorMaker)

Source: Kafka: The Definitive Guide, 2nd ed., ch. 10 (merged 2026-11-15). MirrorMaker's identity: a platform workload identity (identity-and-principals.md › Service accounts and ingest identities; decision 2026-11-16, conflicts #23).

1. Run MirrorMaker under its own service principal, declared in the platform folder as a cross-zone platform principal (conflicts #9) and connecting only over authenticated, encrypted listeners, and give it on the source cluster only `consume` and `describe_configs` on the mirrored topics, `describe` on consumer groups and `describe` on the cluster. Why: it reads records, topic configurations, group offsets and topic ACLs, and nothing more. (Kafka DG ch. 10; conflicts #9.)
2. On the target cluster give MirrorMaker `create`, `publish`, `alter_configs` and `alter` on the mirrored topics and `consume` on consumer groups, and on both clusters `create` and `publish` on its internal topics. Why: it creates mirrored topics, copies their configuration, adds partitions and translates committed offsets. (Kafka DG ch. 10.)
3. Never rely on MirrorMaker's ACL synchronisation: deploy the same compiled Ranger policies from Git to the Ranger service of every cluster. Why: MirrorMaker copies only literal topic ACLs, so prefix, wildcard and Ranger rules are never migrated. (Kafka DG ch. 10.)
4. Keep `publish` on mirrored topics with MirrorMaker alone until failover, and grant applications `publish` on the target only through a failover change prepared and approved in Git beforehand and merged by a named person at failover. Why: write access is deliberately not migrated, and a pre-approved change makes failover fast without standing write access. (Kafka DG ch. 10; conflicts #44.)
5. Add the source-alias form of each institution's prefixes (for example `dc1.t.acme.*`) to the institution's zone resources on the target cluster's service and compile its policies for them as well. Why: mirrored topics carry the source cluster alias as a prefix by default, so policies on `t.acme.*` do not match them. (Kafka DG ch. 10.)

Example (rewritten, zone resources generated for the target cluster):
```yaml
zone: t-acme
services:
  kafka_dr:                                   # target cluster's Ranger service
    resources:
      - { topic: ["t.acme.*", "dc1.t.acme.*"] }   # local and mirrored topics
      - { consumergroup: ["t.acme.*"] }
# publish on dc1.t.acme.* : MirrorMaker only until the pre-approved failover change is merged
```

### Administrative operations: dynamic configuration and unsafe operations

Source: Kafka: The Definitive Guide, 2nd ed., ch. 11, 12 (merged 2026-11-15); Data Management at Scale, 2nd ed., ch. 6 (merged 2026-11-16).

1. Run Kafka admin tooling only as authenticated, named principals over the authenticated listeners, so that every administrative request is authorized and audited by Ranger, and give it only to administrators. Why: by default these tools change topic, client and broker configuration with no security check and no audit. (Kafka DG ch. 11, 12.)
2. Grant topic `alter_configs` only on the institution's own topic prefix, never on `*` or another prefix, and apply topic configuration overrides (for example `min.insync.replicas`) only from versioned files in the institution folder; `alter`, `alter_configs`, `configure` and `delete` on topics belong to the Institution Owner only, through an eligible assignment with no data actions, while Data Contributor holds only `create`; retention values come from the OPA retention policy, and CI rejects a topic configuration file that sets retention outside the approved value. Why: these overrides change the durability and data lifetime of the institution's topics, and retention is the one policy the platform gives to OPA. (Kafka DG ch. 12; DMaS ch. 6; decision 2026-11-15, conflicts #49, #50; conflicts #20.)
3. Treat operations that bypass the Kafka authorizer (direct changes to cluster metadata, manual deletion of topic data, forced controller changes) as break-glass under conflicts #8: an eligible platform operator group, approval by a different person, a second operator present during the work, a fixed time window and full audit, reviewed afterwards. Why: these operations skip every normal safeguard, and manual topic deletion requires every broker to be shut down. (Kafka DG ch. 12; conflicts #8.)
4. Run any operation that bypasses the authorizer only inside a change window approved in Flowable and lasting at most 2 hours. Why: an unaudited path must be short and announced in advance. (decision 2026-11-16)

Example (rewritten, activation record):
```yaml
kind: OperationActivation
metadata: { id: brk-kafka-0007 }
spec:
  operation: manual-topic-deletion
  cluster: kafka-prod
  eligibleGroup: platform_kafka-operators-eligible
  requestedBy: alex
  approvedBy: dana            # a different person (conflicts #8)
  secondOperator: sam         # present during the work
  window: PT2H                # within the #8 maximum
  incident: INC-311
```

### Event governance: topic ownership and stream classes

Source: Data Management at Scale, 2nd ed., ch. 6 (merged 2026-11-16). Compiling stream classes to grants: rule 4 (decision 2026-11-16, conflicts #58).

1. Give every topic an owner and a stream class before it is created: the owning institution and domain, the data owner, and whether the stream is internal to the producing domain or a business stream for other domains, recorded in the topic's OpenMetadata entry and in its versioned topic configuration file in the institution folder; CI rejects a topic file without owner or class. Why: event streams are owned like data products, and the class decides who may consume. (DMaS ch. 6.) How the class compiles to grants: rule 4.
2. Make a consumer that builds a new stream from another domain's events ingest them into its own application first and publish the derived stream under its own institution and domain prefix as a new owned topic whose classification follows its sources through lineage; grant no consumer `publish` on another domain's prefix. Why: streams built directly on streams become an ungoverned web, and a derived stream is new data with its own owner (access-control-models.md › Central vs domain policy ownership, rule 4; conflicts #10). (DMaS ch. 6; conflicts #10.)
3. Offer self-service on topics only through the institution folder, one operation pattern at a time: a reviewed topic configuration file creates the topic and its compiled Ranger policies in one change (`create` for Data Contributor, other topic management for the Institution Owner, conflicts #49; retention from the OPA retention policy, conflicts #50), and a further operation such as an offset reset becomes self-service only once it has its own reviewed pattern; nothing is provisioned outside Git. Why: registry metadata makes automation safe when security policies are applied with every provisioned topic, and Git review stays the only change path (conflicts #6, #7). (DMaS ch. 6; conflicts #6, #7, #49, #50.)
4. Put the stream class into the topic name (for example `t.acme.sales.pub.orders` for business streams and `t.acme.sales.int.orders` for internal ones), compile a domain scope only to the business prefix (`t.acme.sales.pub.*`), and grant internal topics only to the producing domain's own principals; confirm in a lab that Ranger's topic wildcards keep the two prefixes apart. Why: a domain pattern over all topics would let consumers read internal streams, the same gap that conflicts #48 closed for raw tables. (DMaS ch. 6; decision 2026-11-16, conflicts #58; conflicts #48.)

Example (rewritten, topic configuration file in the institution folder):
```yaml
kind: Topic
metadata: { id: t.acme.sales.orders.v1 }
spec:
  institution: t-acme
  domain: sales
  dataOwner: t-acme sales lead
  streamClass: business              # internal | business (compiles to a `.pub.` or `.int.` topic segment, rule 4)
  derivedFrom: []                    # lineage for derived streams (rule 2)
  partitions: 6
  retention: "opa:class-orders"      # value from the OPA retention policy (conflicts #50)
```

### Threat model and security requirements

Source: Kafka: The Definitive Guide, 2nd ed., ch. 9, 11 (merged 2026-11-15).

1. Trace every Kafka threat to its control before go-live: unauthenticated clients (Authorizer model, rule 2), grants to all or anonymous principals (Authorizer model, rule 1), stolen bearer tokens or credential files (rule 2; token lifetime per conflicts #3), sessions that outlive a revoked identity (Principals and principal mapping, rule 3), reused principal names (identity-and-principals.md › Identity life cycle, rule 4), super-user bypass (starter rule 2) and admin tooling (Administrative operations, rule 1). Why: Kafka fixes the principal at authentication and authorizes every request against it, so each of these opens a path that policies alone cannot close. (Kafka DG ch. 11.)
2. Keep every Kafka-side credential out of clear text: broker keystores and truststores and every external-system credential a connector uses are held in OpenBao and resolved at runtime (for connectors through a configuration provider backed by OpenBao), and access to keystore files on brokers and workers is limited to the service that uses them. Why: whoever holds a credential file can impersonate its principal, and plaintext credentials in configuration need separate handling and access control. (Kafka DG ch. 9, 11.)
3. Keep default deny on Kafka: a request that matches no Ranger allow item is denied, and no allow-if-no-policy setting is enabled in production. Why: with allow-if-no-policy every new topic starts open, and the first prefix policy added later silently removes access others relied on. (Kafka DG ch. 11.)

### Principals and principal mapping

Source: Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15). Workload principals, the client mechanism and delegation tokens: decision 2026-11-16, conflicts #23.

1. Configure the principal mapping of every Kafka listener so that the resulting principal string equals the canonical Keycloak username that usersync loads into Ranger (identity-and-principals.md › Starter rule 1): on OAUTHBEARER listeners take the configured token subject claim, on certificate listeners reshape the distinguished name with ordered mapping rules, default rule last, and check every mapping against the Ranger user list before rollout. Why: Kafka authorizes and audits the exact principal string, and a string that differs from the Ranger user matches no policy. (Kafka DG ch. 11; decision 2026-11-15, conflicts #33.)
2. Let Ranger resolve a Kafka principal's groups from the usersync user store, never from group claims in the OAuth token. Why: one group source keeps Kafka decisions identical to the other engines', and tokens stay limited to stable identity claims (identity-and-principals.md › Custom security attributes, rule 9). (Kafka DG ch. 11.)
3. Set `connections.max.reauth.ms` on every SASL listener equal to the Keycloak access-token lifetime. Why: a connection keeps its authenticated principal until it reauthenticates, so without this setting a removed user's open connections keep working until they drop; a session lasts at most the lower of the credential lifetime and this interval. (Kafka DG ch. 11; conflicts #3.)
4. Authenticate every Kafka client, Kafka Connect, MirrorMaker and long-lived applications included, with SASL OAUTHBEARER over SASL_SSL using Keycloak tokens of its own workload identity or external service principal; create no mTLS client identities, Strimzi `KafkaUser` client users or X.509 service identities, and run one Kafka Connect cluster per institution under its own documented workload identity, with a separate Connect cluster for flows that carry `PII` or `RESTRICTED` data. Why: OAUTHBEARER yields the Keycloak name as the principal without certificate mapping rules, and Kafka sees one principal per Connect worker cluster, so the cluster is the identity boundary. (Kafka DG ch. 9, 10, 11; Data Mesh ch. 14; decision 2026-11-16, conflicts #23.)
5. Set client quotas as broker dynamic quotas keyed by the OAuth principal name, applied by the control plane from versioned files in the institution's folder, not through Strimzi `KafkaUser` resources. Why: client principals no longer exist as `KafkaUser` objects, and quotas belong in Git with the rest of the institution's configuration. (Kafka DG ch. 12; decision 2026-11-16, conflicts #23.)

### Authorizer model: resources, operations, super users

Source: Apache Ranger wiki, p. 8 Kafka plugin (2015; corrected by lab V3 on Ranger 2.8.0), p. 2, p. 6, p. 9 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11, 12 (merged 2026-11-15). Super users: starter rule 2.

1. Remove group `public` from every default policy of a new Ranger Kafka service before the service carries traffic, and let CI reject any Kafka policy item for group `public`, user `ANONYMOUS` or the `{USER}` macro. Why: a new Kafka service starts with default policies granting `public` almost every access type, `kafka_admin` included, the 2015 wiki page relied on `public` plus IP ranges instead, and a grant to all or anonymous principals also reaches unauthenticated, removed or reused identities. (Apache Ranger wiki p. 8; Kafka DG ch. 11; trial: verification lab V3, 2026-11-14; decision 2026-11-14, conflicts #16; conflicts #40.)
2. Authorize Kafka only on authenticated, encrypted listeners (SASL_SSL with OAUTHBEARER for clients, Principals and principal mapping, rule 4), the KRaft controller listener included, which uses a named principal; no PLAINTEXT or SASL_PLAINTEXT listener carries traffic. Why: on an unauthenticated listener every client is `ANONYMOUS`, so policies cannot tell clients apart, bearer credentials sent without TLS can be reused by whoever sees them, and the anonymous controller flooded the audit store. (Apache Ranger wiki p. 8, p. 9; Kafka DG ch. 11; trial: verification lab V5, 2026-11-14; conflicts #18.)
3. Grant topic creation as `create` on the institution's topic prefix (for example `t.acme.*`), never on `*`. Why: Ranger 2.8 checks `create` per topic (lab V3), while the 2015 page needed `create` on all topics. (Apache Ranger wiki p. 8.)
4. Map Kafka operations to Ranger access types as Write → `publish`, Read → `consume`, Describe → `describe`, Create → `create`, Delete → `delete`, Alter → `alter`, AlterConfigs → `alter_configs`, DescribeConfigs → `describe_configs`, ClusterAction → `cluster_action`, IdempotentWrite → `idempotent_write`, on the resources `topic`, `consumergroup`, `transactionalid`, `cluster` and `delegationtoken`, and use the 12 access types of the Ranger 2.8 service definition, `configure` and `alter_configs` included, as the vocabulary of platform role definitions. Why: role definitions must name what the Kafka authorizer checks, and both names exist in Ranger 2.8 and are listed by the lab. (Kafka DG ch. 11; trial: verification lab V3, 2026-11-14.) — main session: delete the trailing note "main session: drop this rule if Kafka DG 11 …"; the rule now rests on the book and the lab.
5. Do not rely on script conditions in the Kafka plugin, including tag-attribute expiry such as the default `EXPIRES_ON` policy; use `validitySchedules` for time-bound Kafka access. Why: script conditions failed to initialise in the Kafka plugin on JDK 17. (Apache Ranger wiki p. 2, p. 6.)
6. Compile each Kafka data role into every resource a client needs, with `describe` listed explicitly: a consumer gets `consume` and `describe` on the topic prefix and on the consumer-group prefix; a producer gets `publish` and `describe` on the topic prefix; a transactional producer also gets `publish` on the `transactionalid` prefix and `consume` on the consumer-group prefix. Why: Kafka checks the group and transactional-ID resources besides the topic, and listing implied access types keeps compiled items readable without relying on Kafka's implications. (Kafka DG ch. 11.) Note: `idempotent_write` on the `cluster` resource for idempotent producers is Unclear U1.
7. Grant `cluster_action` only to broker and KRaft controller principals, and cluster-level `alter` and `alter_configs` (broker configuration, ACL and reassignment changes) only to platform administrators (azure-style-rbac-abac.md › Best practices and limits, rule 7); no institution role holds `cluster_action`, `alter` or `alter_configs` on the `cluster` resource, and CI rejects such items. Why: `cluster_action` authorizes replication and controller traffic, and a broker setting such as unclean leader election can lose data of every institution. (Kafka DG ch. 11, 12.)

Example (rewritten, Ranger policy JSON compiled from ra-0101, data-reader for `platform_t-acme_analyst` on `/institutions/t-acme/domains/sales`):
```json
[
  { "service": "kafka-main", "zoneName": "t-acme", "name": "ra-0101 data-reader topics",
    "resources": { "topic": { "values": ["t.acme.sales.*"] } },
    "policyItems": [ { "groups": ["platform_t-acme_analyst"],
      "accesses": [ { "type": "consume", "isAllowed": true }, { "type": "describe", "isAllowed": true } ] } ] },
  { "service": "kafka-main", "zoneName": "t-acme", "name": "ra-0101 data-reader groups",
    "resources": { "consumergroup": { "values": ["t.acme.sales.*"] } },
    "policyItems": [ { "groups": ["platform_t-acme_analyst"],
      "accesses": [ { "type": "consume", "isAllowed": true }, { "type": "describe", "isAllowed": true } ] } ] }
]
```

### Custom authorization considerations

Source: Kafka: The Definitive Guide, 2nd ed., ch. 10, 11 (merged 2026-11-15).

1. Use `RangerKafkaAuthorizer` as the only Kafka authorizer, with group decisions resolved from usersync groups; write no custom authorizer and keep no Kafka ACLs beside Ranger. Why: the book's custom directory-group authorizer is what Ranger provides natively, and rules kept outside Ranger are neither generated from Git nor audited by Ranger, and are not carried over consistently between clusters. (Kafka DG ch. 10, 11; conflicts #38.)
2. Restrict Kafka management requests by context as well as by principal: compile platform administrator items for cluster-level management with an IP-range condition for the platform's internal administration network. Why: the authorizer sees the request context and can refuse administrative requests that arrive from the client side; whether the Ranger Kafka plugin can also match the listener, as the book's internal-listener pattern does, is open (Unclear U2). (Kafka DG ch. 11.)

### Auditing Kafka access

Source: Apache Ranger wiki, p. 9 Audit filters (2025) (merged 2026-11-15); lab V5; Hadoop Security, ch. 8 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11, 12 (merged 2026-11-15).

1. Rely on the RangerKafkaAuthorizer audit, not on Kafka's own authorizer log, for allowed and denied Kafka decisions, per topic operation, and exclude only the KRaft controller principal's `cluster_action` events through the Kafka service's audit filter (audit-design.md › Audit filters and exclusions, rule 3); volume filtering never drops denied events. Why: both decisions reach the Ranger audit store by default, while Kafka's own authorizer logs allowed decisions only at DEBUG, and the controller produced one `cluster_action` event every 2–4 s. (Apache Ranger wiki p. 9; Hadoop Security ch. 8; Kafka DG ch. 11; trial: verification lab V5, 2026-11-14; conflicts #18.)
2. Give the clients of each consumer group a client ID unique to that group. Why: a distinct client ID makes requests traceable to their group in the logs. (Kafka DG ch. 12.)


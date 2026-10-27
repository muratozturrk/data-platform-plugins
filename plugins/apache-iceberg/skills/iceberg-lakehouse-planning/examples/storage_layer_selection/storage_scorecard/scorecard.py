"""Requirement-driven storage selection as a transparent, testable scorecard.

Inputs and where they come from:
  VENDORS    documented traits of ten storage options: deployment, S3 API, cost model,
             capabilities and operational weaknesses (vendor matrix).
  SCENARIOS  16 hypothetical selection scenarios (4 per requirement category) with the
             vendors a reference analysis names. The weights that turn scenario text into
             numbers are assumptions (1 = nice to have, 3 = central to the scenario).
  measured_profile()  measured: capabilities proven by storage_requirements_probe and storage_governance.

Method (weighted-sum decision matrix): hard constraints filter, weighted capabilities score,
listed weaknesses subtract. Ties keep VENDORS order.
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Vendor:
    name: str
    deployment: frozenset          # deployment model (+ "edge" where the vendor supports it)
    s3_api: str                    # "none" | "native" | "full"
    cost_model: str
    caps: frozenset                # documented capabilities
    weaknesses: frozenset = frozenset()


def V(name, deployment, s3_api, cost_model, caps, weaknesses=()):
    return Vendor(name, frozenset(deployment.split()), s3_api, cost_model,
                  frozenset(caps.split()), frozenset(weaknesses.split() if weaknesses else ()))


VENDORS = [
    V("HDFS", "onprem", "none", "CapEx-heavy",
      "high_throughput data_locality open_source capex",
      "ops_heavy weak_multitenancy no_http_api"),
    V("Amazon S3", "cloud", "native", "Pay-per-use (including egress)",
      "elastic_scale request_optimization cloud_compute_integration worm compliance_lock "
      "georeplication iam encryption_at_rest cmek audit_logging pay_as_you_go managed tiering "
      "lifecycle_policies cost_monitoring gov_cloud versioning",
      "per_request_cost"),
    V("Google Cloud Storage", "cloud", "native", "Tiered, usage-based",
      "elastic_scale strong_consistency low_latency high_throughput cloud_compute_integration "
      "iam encryption_at_rest cmek audit_logging pay_as_you_go managed tiering worm "
      "lifecycle_policies cost_monitoring",
      "per_request_cost"),
    V("Azure Blob/ADLS", "cloud", "native", "Tiered, usage-based",
      "elastic_scale strong_consistency cloud_compute_integration fine_grained_acl idp worm "
      "encryption_at_rest cmek audit_logging pay_as_you_go managed tiering hierarchical_namespace",
      "per_request_cost"),
    V("MinIO", "onprem hybrid edge", "full", "Tiered, usage-based",
      "low_latency high_throughput erasure_coding self_healing worm encryption_at_rest idp "
      "lightweight_ops open_source capex multitenancy"),
    V("Ceph", "onprem", "full", "Open source, infra-dependent",
      "erasure_coding self_healing multitenancy idp rbac open_source capex unified_block_file_object",
      "needs_tuning ops_heavy"),
    V("NetApp StorageGRID", "onprem hybrid", "full", "Enterprise licensing",
      "worm compliance_lock lifecycle_policies policy_placement georeplication erasure_coding "
      "audit_logging idp encryption_at_rest high_throughput centralized_mgmt enterprise_support "
      "multitenancy capex versioning",
      "premium_cost"),
    V("Everpure", "onprem", "full", "Premium appliance",
      "low_latency deterministic_latency high_throughput encryption_at_rest multitenancy idp "
      "audit_logging centralized_mgmt enterprise_support low_ops_appliance capex",
      "premium_cost"),
    V("Dell ECS", "onprem hybrid", "full", "Enterprise licensing",
      "georeplication policy_placement erasure_coding multitenancy worm audit_logging idp "
      "encryption_at_rest high_throughput centralized_mgmt enterprise_support capex versioning",
      "premium_cost"),
    V("Wasabi", "cloud", "full", "Flat-rate, no egress fees",
      "flat_rate_no_egress managed worm immutable_by_default encryption_at_rest simple_ops",
      "coarse_access_control no_ecosystem"),
]


@dataclass(frozen=True)
class Scenario:
    category: str                                   # performance | security | integrity | cost_ops
    name: str
    prefer: dict                                    # capability -> weight (lab assumption)
    avoid: dict = field(default_factory=dict)       # weakness -> penalty (lab assumption)
    must: frozenset = frozenset()                   # hard capability constraints
    deployment: frozenset = frozenset()             # allowed deployment models (empty = any)
    picks: tuple = ()                               # vendors the reference analysis names
    weaker: tuple = ()                              # vendors it names as weaker fits


def S(category, name, prefer, picks, avoid=None, must="", deployment="", worse=()):
    return Scenario(category, name, prefer, avoid or {}, frozenset(must.split()),
                    frozenset(deployment.split()), tuple(picks), tuple(worse))


SCENARIOS = [
    # performance
    S("performance", "Interactive dashboards, high concurrency",
      {"low_latency": 3, "deterministic_latency": 3, "elastic_scale": 2, "request_optimization": 2},
      ["Everpure", "Amazon S3"], avoid={"needs_tuning": 3}, worse=["Ceph"]),
    S("performance", "Large-scale overnight batch ETL",
      {"high_throughput": 3, "data_locality": 1, "strong_consistency": 1, "capex": 1},
      ["HDFS", "Google Cloud Storage", "MinIO"]),
    S("performance", "Time-sensitive ML feature pipelines",
      {"cloud_compute_integration": 3, "elastic_scale": 2, "low_latency": 1},
      ["Azure Blob/ADLS", "Amazon S3"], avoid={"no_ecosystem": 3}, worse=["Wasabi"]),
    S("performance", "Regulatory queries, non-urgent timing",
      {"lifecycle_policies": 2, "policy_placement": 2, "worm": 2, "high_throughput": 1, "enterprise_support": 1},
      ["NetApp StorageGRID", "Dell ECS"]),
    # security
    S("security", "Healthcare data under HIPAA",
      {"worm": 2, "fine_grained_acl": 3, "idp": 2, "compliance_lock": 1},
      ["NetApp StorageGRID", "Azure Blob/ADLS"], must="encryption_at_rest audit_logging"),
    S("security", "Internal R&D data, restricted roles",
      {"multitenancy": 2, "idp": 2, "rbac": 2, "open_source": 2},
      ["Ceph", "MinIO"], deployment="onprem hybrid"),
    S("security", "Public cloud with strong auditability",
      {"cmek": 3, "audit_logging": 3, "iam": 1, "gov_cloud": 1},
      ["Amazon S3", "Google Cloud Storage"], deployment="cloud"),
    S("security", "Data sharing across jurisdictions",
      {"policy_placement": 3, "georeplication": 2, "audit_logging": 1, "encryption_at_rest": 1},
      ["Dell ECS", "NetApp StorageGRID"], deployment="onprem hybrid"),
    # integrity
    S("integrity", "Disaster recovery, geo-distributed enterprise",
      {"georeplication": 3, "versioning": 2, "worm": 1, "policy_placement": 1, "audit_logging": 1},
      ["NetApp StorageGRID", "Dell ECS"], deployment="onprem hybrid"),
    S("integrity", "Long-term retention, verifiable immutability",
      {"compliance_lock": 3, "immutable_by_default": 3, "flat_rate_no_egress": 1},
      ["Amazon S3", "Wasabi", "NetApp StorageGRID"], must="worm"),
    S("integrity", "Frequent schema evolution and metadata updates",
      {"strong_consistency": 3, "managed": 1},
      ["Google Cloud Storage", "Azure Blob/ADLS"]),
    S("integrity", "Edge deployment, unreliable network",
      {"erasure_coding": 2, "self_healing": 3, "open_source": 1},
      ["MinIO", "Ceph"], deployment="onprem edge"),
    # cost and operations
    S("cost_ops", "Startup with limited DevOps capacity",
      {"flat_rate_no_egress": 3, "managed": 1, "lightweight_ops": 3, "simple_ops": 1},
      ["Wasabi", "MinIO"], avoid={"ops_heavy": 3, "premium_cost": 2}),
    S("cost_ops", "Enterprise consolidating infrastructure",
      {"multitenancy": 2, "centralized_mgmt": 3, "enterprise_support": 3},
      ["Dell ECS", "NetApp StorageGRID", "Everpure"]),
    S("cost_ops", "Research institution, grant-funded spikes",
      {"pay_as_you_go": 3, "cost_monitoring": 2, "tiering": 1, "elastic_scale": 1},
      ["Google Cloud Storage", "Amazon S3"], avoid={"premium_cost": 2}),
    S("cost_ops", "Government, sovereignty and cost transparency",
      {"gov_cloud": 3, "open_source": 2, "capex": 2, "encryption_at_rest": 1},
      ["Amazon S3", "Ceph", "MinIO"], avoid={"premium_cost": 2}),
]


@dataclass(frozen=True)
class Score:
    vendor: str
    score: int
    eligible: bool
    missing: tuple


def score(v: Vendor, s: Scenario) -> Score:
    missing = tuple(sorted(s.must - v.caps))
    if s.deployment and not (v.deployment & s.deployment):
        missing += ("deployment:" + "/".join(sorted(s.deployment)),)
    pts = sum(w for c, w in s.prefer.items() if c in v.caps) - sum(p for w_, p in s.avoid.items() if w_ in v.weaknesses)
    return Score(v.name, pts, not missing, missing)


def rank(s: Scenario, vendors=VENDORS) -> list[Score]:
    """Eligible vendors first, by score; stable sort keeps VENDORS order on ties."""
    scores = [score(v, s) for v in vendors]
    return sorted(scores, key=lambda x: (not x.eligible, -x.score))


def shortlist(s: Scenario, slack: int = 2, vendors=VENDORS) -> list[str]:
    """Top (number of reference picks + slack) eligible vendors."""
    return [x.vendor for x in rank(s, vendors) if x.eligible][: len(s.picks) + slack]


# ---------- measured profiles (storage_requirements_probe + storage_governance) ----------

def measured_profile(name: str, feature_matrix: dict, probe=None, required_s3_checks_pass=True) -> Vendor:
    """A vendor row built only from what the lab proved. Unprobed features count as absent."""
    caps = {"open_source", "capex"}
    if feature_matrix.get("object_lock") == "enforced":
        caps |= {"worm", "compliance_lock"}
    if feature_matrix.get("versioning") == "enforced":
        caps.add("versioning")
    if feature_matrix.get("sse_s3") == "enforced":
        caps.add("encryption_at_rest")
    if feature_matrix.get("lifecycle") in ("enforced", "accepted"):
        caps.add("lifecycle_policies")
    if probe is not None and probe.read_after_write and probe.list_after_write:
        caps.add("strong_consistency")
    return Vendor(name, frozenset({"onprem", "edge"}), "full" if required_s3_checks_pass else "partial",
                  "self-hosted (lab)", frozenset(caps))


# ---------- capacity overhead of the durability scheme ----------

def raw_capacity_tb(usable_tb: float, replicas: int | None = None, ec: tuple[int, int] | None = None) -> float:
    """Raw disk needed for `usable_tb`: n-way replication (HDFS default 3) or k+m erasure coding."""
    if replicas:
        return usable_tb * replicas
    k, m = ec
    return usable_tb * (k + m) / k

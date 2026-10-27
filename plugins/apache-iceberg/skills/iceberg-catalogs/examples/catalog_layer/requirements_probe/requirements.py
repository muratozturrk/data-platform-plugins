"""Requirements scorecard for catalog selection (reused by catalog_scenarios).

A requirement is either a hard constraint (must) or a weighted preference. Candidates that miss a
must are eliminated with the reason, not just ranked lower: a catalog without credential vending
does not become acceptable because it is cheap.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Requirement:
    key: str                      # capability name, e.g. "rbac" or "load_p95_ms"
    must: bool = True
    weight: int = 1               # used for preferences (and as tie-breaker for musts)
    max_value: float | None = None   # numeric requirement: fact must be <= max_value
    min_value: float | None = None   # numeric requirement: fact must be >= min_value
    why: str = ""                 # traceability: which stakeholder / audit finding asked for it

    def satisfied_by(self, facts: dict) -> bool:
        value = facts.get(self.key)
        if self.max_value is not None or self.min_value is not None:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return False
            if self.max_value is not None and value > self.max_value:
                return False
            return self.min_value is None or value >= self.min_value
        return bool(value)


@dataclass
class Verdict:
    candidate: str
    eligible: bool
    score: int
    missing_musts: list[str] = field(default_factory=list)
    met_preferences: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def evaluate(candidates: dict[str, dict], requirements: list[Requirement]) -> list[Verdict]:
    """Rank candidates: eligible first, then by weighted score; input order breaks ties."""
    verdicts = []
    for order, (name, facts) in enumerate(candidates.items()):
        missing = [r.key for r in requirements if r.must and not r.satisfied_by(facts)]
        met = [r.key for r in requirements if not r.must and r.satisfied_by(facts)]
        score = sum(r.weight for r in requirements if r.satisfied_by(facts))
        v = Verdict(name, not missing, score, missing, met)
        if facts.get("status") in ("deprecated", "legacy"):
            v.warnings.append(f"{name} is {facts['status']}: plan the exit before adopting")
        verdicts.append((not missing, score, -order, v))
    verdicts.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    return [v for *_, v in verdicts]


def choose(candidates: dict[str, dict], requirements: list[Requirement]) -> Verdict | None:
    ranked = evaluate(candidates, requirements)
    return ranked[0] if ranked and ranked[0].eligible else None


def facts_from_probe(caps, latency, contention) -> dict:
    """Turn lab measurements (probe.py) into the same fact vocabulary the scorecard uses."""
    return {
        "rest": caps.rest_spec,
        "auth_required": caps.auth_required,
        "credential_vending": caps.credential_vending,
        "multi_table_transactions": caps.multi_table_transactions,
        "views": caps.views,
        "register_table": caps.register_table,
        "load_p95_ms": latency.p95_ms,
        "commits_per_second": contention.commits_per_second,
        "lost_updates": contention.writers * contention.commits_per_writer - contention.columns_added,
    }

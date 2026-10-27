#!/usr/bin/env python3
"""Fitness function: no domain/customer nouns and no hard-coded environment values in core.
Usage: python forbidden_tokens.py src/platform/core
Exit 1 on violation, printing file:line, token and the fix hint."""
import re, sys, pathlib

DOMAIN_TOKENS = ["customer_id", "order", "invoice", "policy_number", "acme", "contoso"]   # configuration, not code
ENV_PATTERNS = {
    "bucket uri":  re.compile(r"\b(s3|gs|abfss|wasbs)://[\w./-]+"),
    "hostname":    re.compile(r"\b[\w-]+\.(internal|corp|local|amazonaws\.com|windows\.net)\b"),
    "region":      re.compile(r"\b(eu|us|ap)-(central|west|east|south|north)-\d\b"),
    "abs path":    re.compile(r"[\"'](/mnt|/data|/opt|C:\\\\)[^\"']*[\"']"),
    "ip address":  re.compile(r"\b\d{1,3}(\.\d{1,3}){3}\b"),
}
ALLOW = re.compile(r"#\s*ff:allow")   # explicit, reviewable escape hatch

def scan(root: pathlib.Path) -> int:
    violations = 0
    for f in root.rglob("*.py"):
        for n, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if ALLOW.search(line):
                continue
            low = line.lower()
            for t in DOMAIN_TOKENS:
                if re.search(rf"\b{re.escape(t)}\b", low):
                    print(f"{f}:{n}: domain noun '{t}' in core -> move to tenant config or a plugin"); violations += 1
            for kind, pat in ENV_PATTERNS.items():
                if pat.search(line):
                    print(f"{f}:{n}: hard-coded {kind} -> move to environment config layer / provider plugin"); violations += 1
    return violations

if __name__ == "__main__":
    sys.exit(1 if scan(pathlib.Path(sys.argv[1])) else 0)

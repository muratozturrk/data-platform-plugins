#!/usr/bin/env python3
"""Legacy ratchet: fail CI if lint violations increase vs a committed baseline.
Usage: python ratchet.py --baseline .standards-baseline.json [--update]"""
import argparse, json, subprocess, sys

CHECKS = {
    "ruff":     ["ruff", "check", "src", "--output-format", "json"],
    "sqlfluff": ["sqlfluff", "lint", "models", "--format", "json"],
}

def count(cmd):
    out = subprocess.run(cmd, capture_output=True, text=True).stdout or "[]"
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return 0
    if isinstance(data, list) and data and "violations" in data[0]:      # sqlfluff shape
        return sum(len(f["violations"]) for f in data)
    return len(data)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--update", action="store_true")
    a = ap.parse_args()
    current = {k: count(v) for k, v in CHECKS.items()}
    if a.update:
        json.dump(current, open(a.baseline, "w"), indent=2); print("baseline updated", current); sys.exit(0)
    try:
        base = json.load(open(a.baseline))
    except FileNotFoundError:
        base = {}
    worse = {k: (base.get(k, 0), n) for k, n in current.items() if n > base.get(k, 0)}
    for k, (b, n) in worse.items():
        print(f"{k}: {n} violations > baseline {b}  (fix new ones or lower the baseline deliberately)")
    print("current:", current)
    sys.exit(1 if worse else 0)

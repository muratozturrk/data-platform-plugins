#!/usr/bin/env python3
"""Fitness function: validate names against naming-scheme.yaml.
Input: JSON inventory [{"asset": "kafka_topic", "name": "...", "location": "..."}, ...]
Usage: python check_names.py naming-scheme.yaml inventory.json  (exit 1 on violations)"""
import json, re, sys, yaml

def load_scheme(path):
    spec = yaml.safe_load(open(path))["spec"]
    return {k: v for k, v in spec["assets"].items()}, spec

def check(scheme, inventory):
    violations = []
    for item in inventory:
        rule = scheme.get(item["asset"])
        if not rule:
            violations.append((item, "unknown asset type")); continue
        name = item["name"]
        if "max_len" in rule and len(name) > rule["max_len"]:
            violations.append((item, f"longer than {rule['max_len']}"))
        if not re.match(rule["regex"], name):
            violations.append((item, f"does not match {rule['regex']}"))
        low = name.lower()
        for w in rule.get("banned_words", []):
            if re.search(rf"(^|_){w}(_|$)", low):
                violations.append((item, f"banned word '{w}'"))
        for s in rule.get("banned_suffixes", []):
            if name.endswith(s):
                violations.append((item, f"banned suffix '{s}'"))
    return violations

if __name__ == "__main__":
    scheme, _ = load_scheme(sys.argv[1])
    inv = json.load(open(sys.argv[2]))
    v = check(scheme, inv)
    for item, why in v:
        print(f"{item.get('location','?')}: {item['asset']} '{item['name']}' -> {why}")
    print(f"{len(inv)-len(v)}/{len(inv)} names compliant")
    sys.exit(1 if v else 0)

#!/usr/bin/env python3
"""Static validation of a Superset assets bundle. Usage: assets_validate.py <bundle_dir> [--baseline <git_ref>] [--superset-version 6.1.0]"""
import argparse, pathlib, re, subprocess, sys, yaml

CRED = re.compile(r"://[^/\s:]+:[^@\s]+@")                  # user:password@ in URI
PII = re.compile(r"iban|email|tckn|phone|address", re.I)
NAME_SNAKE = re.compile(r"^[a-z][a-z0-9_]{1,62}$")
REQUIRED = {
    "databases": ["database_name", "uuid", "sqlalchemy_uri"],
    "datasets": ["table_name", "uuid", "database_uuid", "schema", "metrics", "cache_timeout", "description"],
    "charts": ["uuid", "dataset_uuid", "viz_type", "params", "slice_name"],
    "dashboards": ["uuid", "slug", "position", "metadata", "dashboard_title"],
}

def load_yaml_files(root: pathlib.Path):
    for kind in REQUIRED:
        for f in (root / kind).glob("**/*.yaml"):
            yield kind, f, yaml.safe_load(f.read_text()) or {}

def baseline_uuids(ref: str, root: pathlib.Path):
    """Map (kind, natural key) -> uuid from the Git baseline."""
    out = {}
    try:
        files = subprocess.run(["git", "ls-tree", "-r", "--name-only", ref, str(root)], capture_output=True, text=True, check=True).stdout.split()
    except subprocess.CalledProcessError:
        return out
    for path in files:
        if not path.endswith(".yaml") or "/metadata.yaml" in path: continue
        kind = pathlib.Path(path).relative_to(root).parts[0]
        if kind not in REQUIRED: continue
        doc = yaml.safe_load(subprocess.run(["git", "show", f"{ref}:{path}"], capture_output=True, text=True).stdout) or {}
        out[(kind, natural_key(kind, doc))] = doc.get("uuid")
    return out

def natural_key(kind, doc):
    return {"databases": doc.get("database_name"), "datasets": f"{doc.get('schema')}.{doc.get('table_name')}",
            "charts": doc.get("slice_name"), "dashboards": doc.get("slug")}[kind]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("bundle"); ap.add_argument("--baseline"); ap.add_argument("--superset-version")
    a = ap.parse_args(); root = pathlib.Path(a.bundle); findings = []
    meta = yaml.safe_load((root / "metadata.yaml").read_text()) if (root / "metadata.yaml").exists() else {}
    if not meta: findings.append(("metadata.yaml", "missing", "BLOCK"))
    rls = yaml.safe_load((root / "policies" / "rls.yaml").read_text()) if (root / "policies" / "rls.yaml").exists() else []
    rls_tables = {t for r in rls for t in r.get("tables", [])}
    seen_uuid, base = {}, baseline_uuids(a.baseline, root) if a.baseline else {}
    for kind, f, doc in load_yaml_files(root):
        rel = str(f.relative_to(root))
        for k in REQUIRED[kind]:
            if k not in doc: findings.append((rel, f"missing key {k}", "FIX"))
        u = doc.get("uuid")
        if u in seen_uuid: findings.append((rel, f"duplicate uuid also in {seen_uuid[u]}", "BLOCK"))
        seen_uuid[u] = rel
        bu = base.get((kind, natural_key(kind, doc)))
        if bu and bu != u: findings.append((rel, f"uuid changed vs baseline ({bu} -> {u}); sync would duplicate", "BLOCK"))
        if kind == "databases":
            if CRED.search(str(doc.get("sqlalchemy_uri", ""))): findings.append((rel, "credentials inline in sqlalchemy_uri", "BLOCK"))
            if doc.get("allow_dml"): findings.append((rel, "allow_dml true", "FIX"))
        if kind == "datasets":
            if not NAME_SNAKE.match(str(doc.get("table_name", ""))): findings.append((rel, "table_name not snake_case", "NIT"))
            cols = [c.get("column_name", "") for c in doc.get("columns", [])]
            tenanted = any(c in ("tenant_id", "institution_id") for c in cols) or any(PII.search(c) for c in cols)
            if tenanted and f"{doc.get('schema')}.{doc.get('table_name')}" not in rls_tables:
                findings.append((rel, "tenant/PII columns but no RLS rule in policies/rls.yaml", "BLOCK"))
            for m in doc.get("metrics", []):
                if not m.get("description"): findings.append((rel, f"metric {m.get('metric_name')} has no description", "NIT"))
            if not doc.get("owners") and not doc.get("is_managed_externally"): findings.append((rel, "no owner and not managed externally", "FIX"))
        if kind == "charts" and isinstance(doc.get("params"), dict) and doc["params"].get("adhoc_filters") is None and doc["params"].get("time_range") in (None, "No filter"):
            findings.append((rel, "chart time_range 'No filter' on time-series dataset", "FIX"))
    for rel, msg, sev in sorted(findings, key=lambda x: {"BLOCK": 0, "FIX": 1, "NIT": 2}[x[2]]):
        print(f"{sev:5} {rel}: {msg}")
    blocks = sum(1 for _, _, s in findings if s == "BLOCK")
    print(f"{len(findings)} findings, {blocks} blocking"); sys.exit(1 if blocks else 0)

if __name__ == "__main__":
    main()

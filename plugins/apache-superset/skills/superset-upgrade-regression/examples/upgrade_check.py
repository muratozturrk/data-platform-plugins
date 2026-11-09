#!/usr/bin/env python3
"""Pre-upgrade triage: diff feature flags/config keys in superset_config.py against the target version's config.py defaults;
grep UPDATING.md for the version range. Usage: upgrade_check.py --config superset_config.py --target 6.1.0 --current 6.0.0 [--superset-src ./superset]"""
import argparse, ast, pathlib, re, subprocess, sys

def names_in(pyfile: pathlib.Path):
    tree = ast.parse(pyfile.read_text()); out = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name): out.add(t.id)
    return out

def flags_in(pyfile: pathlib.Path):
    m = re.search(r"FEATURE_FLAGS\s*(?::\s*dict\[[^\]]*\])?\s*=\s*\{(.*?)\n\}", pyfile.read_text(), re.S)
    return set(re.findall(r'"([A-Z0-9_]+)"\s*:', m.group(1))) if m else set()

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--config", required=True); ap.add_argument("--target", required=True); ap.add_argument("--current", required=True); ap.add_argument("--superset-src", default="superset-src")
    a = ap.parse_args(); src = pathlib.Path(a.superset_src)
    if not src.exists():
        subprocess.run(["git", "clone", "--depth", "1", "--branch", a.target, "https://github.com/apache/superset.git", str(src)], check=True)
    target_cfg = src / "superset" / "config.py"
    mine, theirs = names_in(pathlib.Path(a.config)), names_in(target_cfg)
    my_flags, their_flags = flags_in(pathlib.Path(a.config)), flags_in(target_cfg)
    unknown = sorted(k for k in mine - theirs if k.isupper() and not k.startswith("_"))
    dead_flags = sorted(my_flags - their_flags)
    print("== config keys not present in target config.py (renamed/removed?):"); print("\n".join(unknown) or "none")
    print("== feature flags not known to target:"); print("\n".join(dead_flags) or "none")
    upd = (src / "UPDATING.md").read_text()
    start = upd.find(f"## {a.target}"); end = upd.find(f"## {a.current}")
    print(f"== UPDATING.md entries between {a.current} and {a.target}:"); print(upd[start:end] if start != -1 and end != -1 and start < end else "(sections not found; read UPDATING.md manually)")
    sys.exit(1 if unknown or dead_flags else 0)

if __name__ == "__main__":
    main()

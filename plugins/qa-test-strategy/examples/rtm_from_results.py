#!/usr/bin/env python3
"""Generate RTM + coverage from test-case YAML files and JUnit XML results.
Usage: python rtm_from_results.py tests/cases junit-results/ requirements.yaml > rtm.csv"""
import sys, glob, csv, yaml, xml.etree.ElementTree as ET

def load_cases(d):
    cases = {}
    for f in glob.glob(f"{d}/**/*.yaml", recursive=True):
        docs = yaml.safe_load(open(f))
        if not isinstance(docs, list):
            continue                      # only case files are lists of cases
        for c in docs:
            if not isinstance(c, dict):
                continue
            cid = c.get("test_case_id") or c.get("id")
            reqs = set(c.get("requirement_ids", []))
            reqs.update(st.get("requirement_id") for st in c.get("test_execution_steps", []) if st.get("requirement_id"))
            c["requirement_ids"] = sorted(reqs)
            cases[cid] = c
    return cases

def load_results(d):
    res = {}
    for f in glob.glob(f"{d}/**/*.xml", recursive=True):
        for tc in ET.parse(f).getroot().iter("testcase"):
            name = tc.get("name", "")
            tid = next((p for p in name.replace("[", " ").replace("]", " ").split() if p.startswith("T-")), None)
            if tid:
                res[tid] = "FAIL" if tc.find("failure") is not None or tc.find("error") is not None else ("SKIP" if tc.find("skipped") is not None else "PASS")
    return res

if __name__ == "__main__":
    cases = load_cases(sys.argv[1]); results = load_results(sys.argv[2])
    reqs = yaml.safe_load(open(sys.argv[3]))
    w = csv.writer(sys.stdout); w.writerow(["req_id", "priority", "test_case_ids", "executed", "result", "covered"])
    covered = 0
    for r in reqs:
        ids = [cid for cid, c in cases.items() if r["id"] in c.get("requirement_ids", []) and c.get("status") == "active"]
        rs = [results.get(i, "NOT RUN") for i in ids]
        overall = "FAIL" if "FAIL" in rs else ("PASS" if rs and all(x == "PASS" for x in rs) else ("PARTIAL" if rs else "NONE"))
        covered += bool(ids)
        w.writerow([r["id"], r.get("priority"), ";".join(ids), sum(x != "NOT RUN" for x in rs), overall, "yes" if ids else "NO"])
    print(f"# requirement coverage: {covered}/{len(reqs)} = {100*covered/max(len(reqs),1):.0f}%", file=sys.stderr)

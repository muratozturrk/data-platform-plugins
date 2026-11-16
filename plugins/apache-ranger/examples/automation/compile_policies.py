#!/usr/bin/env python3
"""Compile the platform manifest (Institution, RoleDefinition, RoleAssignment) into Ranger policy JSON
(REST API v2 shape) with stable GUIDs. Per institution it also emits the zone tag mask policy and the zone
break-glass allows. Row filters come only from assignments with an `agreement:` (F26).
Platform decisions D1-D11: trial: ranger-policy-compile, 2026-11-16.
Usage: GIT_SHA=$(git rev-parse --short HEAD) compile_policies.py manifests/*.yaml > desired-policies.json"""
import calendar, json, os, pathlib, sys, uuid, yaml
from datetime import datetime, timedelta

NS = uuid.UUID("6f1c2f0e-7a1b-4c59-9d2e-000000000001")
SHA = os.environ.get("GIT_SHA", "local")
TAG_SERVICE = "platform-tags"
WRITE = {"trino:insert", "trino:update", "trino:delete", "spark:update", "kafka:publish"}
TRINO_WRITE = {"insert", "update", "delete"}
MAX_ACTIVATION, MAX_ELIGIBILITY_MONTHS = timedelta(hours=8), 3          # D7
MASK_ENGINES = ("trino", "hive")                                        # spark-sql is a hive-type service
# Default mask per infotype, first match wins, after the activated unmask items (D10, D1). TCKN and other PII types -> MASK_HASH.
MASKS = [("TAG._type == 'PII' && TAG.type == 'iban'", "MASK_SHOW_LAST_4"),
         ("TAG._type == 'PII'", "MASK_HASH"),
         ("TAG._type == 'RESTRICTED'", "MASK_NULL")]                    # column level; table level = deny guardrail
# institution `services` key -> (engine of the role's data actions, which of them this service gets)
SERVICES = {"trino": ("trino", lambda a: a - TRINO_WRITE),              # interactive: never writes (D8)
            "trinoEtl": ("trino", lambda a: a if a & TRINO_WRITE else set()),  # ETL: only assignments that write
            "spark": ("spark", lambda a: a),
            "kafka": ("kafka", lambda a: a)}

def guid(*parts: str) -> str: return str(uuid.uuid5(NS, "/".join(parts)))
def res(values: dict) -> dict:
    return {k: {"values": v if isinstance(v, list) else [v], "isExcludes": False, "isRecursive": False} for k, v in values.items()}
def accesses(lst): return [{"type": a, "isAllowed": True} for a in lst]
def ts(s): return str(s).replace("-", "/").replace("T", " ")[:19]
def dt(s): return datetime.fromisoformat(str(s))
def validity(v): return [{**{f"{k}Time": ts(v[k]) for k in ("start", "end") if k in v}, "timeZone": "Europe/Istanbul"}]
def who(pr): return {"groups": [pr["name"]], "users": []} if pr["type"] == "group" else {"groups": [], "users": [pr["name"]]}
def plus_months(d, n):
    y, m = divmod(d.month - 1 + n, 12); y += d.year; m += 1
    return d.replace(year=y, month=m, day=min(d.day, calendar.monthrange(y, m)[1]))
def active(a): return a["spec"].get("assignmentType") != "eligible" or "start" in a["spec"].get("activation", {})

def load(paths) -> list[dict]:
    return [dict(d, _source=pathlib.Path(p).as_posix()) for p in paths for d in yaml.safe_load_all(open(p, encoding="utf-8")) if d]

def labels(inst_id, inst, inst_doc, docs, kind=None) -> list[str]:
    """source, institution, assignment, role, owner, review-by, manifest, request (F28)."""
    out = ["source:policy-manifest", f"institution:{inst_id}", f"owner:{inst['owner']}"]
    if not docs:                                                           # platform-generated per-zone policy
        out += ["assignment:none", f"role:{kind}", f"review-by:{inst['reviewBy']}", f"manifest:{inst_doc.get('_source', 'inline')}@{SHA}"]
    for d in docs:
        s = d["spec"]
        out += [f"assignment:{d['metadata']['id']}", f"role:{s['roleDefinition']}", f"owner:{s.get('owner', inst['owner'])}",
                f"review-by:{s.get('reviewBy', inst['reviewBy'])}", f"manifest:{d.get('_source', 'inline')}@{SHA}"]
        if "request" in s: out.append(f"request:{s['request']}")
    return list(dict.fromkeys(out))

def scope_resources(inst: dict, s: dict) -> dict:
    """/institutions/<i>[/domains/<d>[/staging | /datasets/<t>]] -> resources per engine (#48, #58, #61)."""
    p = s["scope"].strip("/").split("/")
    cat, pre = inst["catalog"], inst["topicPrefix"]
    tr = lambda schemas, table="*", cols="*": {"catalog": cat, "schema": schemas, "table": table, "column": cols}
    # D5 (provisional, lab check of the Kyuubi/Iceberg catalog mapping): Spark database = <catalog>_<schema>
    sp = lambda schemas, table="*", cols="*": {"database": [f"{cat}_{x}" for x in schemas], "table": table, "column": cols}
    if len(p) == 2:
        return {"trino": tr("*"), "spark": {"database": f"{cat}_*", "table": "*", "column": "*"}, "kafka": {"topic": f"{pre}.*"}}
    d = inst["domains"][p[3]]
    if len(p) == 4:
        return {"trino": tr(d["published"]), "spark": sp(d["published"]), "kafka": {"topic": f"{pre}.{p[3]}.pub.*"}}
    if p[4:] == ["staging"]:
        out = {"kafka": {"topic": f"{pre}.{p[3]}.int.*"}}
        if d["staging"]: out.update(trino=tr(d["staging"]), spark=sp(d["staging"]))
        return out
    assert len(p) == 6 and p[4] == "datasets", f"unknown scope {s['scope']}"
    schema, cols = d["datasets"][p[5]], s.get("columns", "*")
    out = {"trino": tr(schema, p[5], cols), "spark": sp([schema], p[5], cols)}
    if "topics" in s:                                                      # Kafka at dataset scope only when listed
        assert "columns" not in s, f"{s['scope']}: Kafka has no column level, a column-scoped assignment lists no topics"
        out["kafka"] = {"topic": s["topics"]}
    return out

def compile_assignment(a: dict, roles: dict, insts: dict) -> tuple[list[dict], list[dict]]:
    """Returns (resource policies, unmask items for the zone tag mask policy)."""
    rid, s = a["metadata"]["id"], a["spec"]
    pr, rd = s["principal"], roles[s["roleDefinition"]]
    assert pr["type"] in ("group", "workloadIdentity", "servicePrincipal"), f"{rid}: humans are assigned only through groups"
    inst_id = s["scope"].split("/")[2]; inst, inst_doc = insts[inst_id]
    data = set(rd["dataActions"]) - set(rd.get("notDataActions", []))
    unmask = sorted(x.split(":", 1)[1] for x in data if x.startswith("unmask:"))
    if pr["type"] == "group" and data & WRITE and not s["scope"].endswith("/staging"):
        raise AssertionError(f"{rid}: human write access only on raw/staging/sandbox scopes (#61)")
    if pr["type"] == "group" and not pr["name"].startswith(f"platform_{inst_id}_"):
        assert "agreement" in s, f"{rid}: a group of another institution needs an agreement (cross-institution share)"
    if "agreement" in s:
        assert not data & WRITE and "/datasets/" in s["scope"] and "columns" in s and "validity" in s, \
            f"{rid}: a share is a read-only, column-scoped, time-bound dataset assignment"
    item_v = None
    if s.get("assignmentType") == "eligible":
        el, act = s["eligibility"], s.get("activation", {})
        if unmask:
            assert dt(el["end"]) <= plus_months(dt(el["start"]), MAX_ELIGIBILITY_MONTHS), f"{rid}: eligibility longer than 3 months (D7)"
        if "start" not in act: return [], []                              # eligible: grants nothing until activated
        assert dt(act["end"]) - dt(act["start"]) <= MAX_ACTIVATION, f"{rid}: activation longer than PT8H (D7)"
        assert dt(el["start"]) <= dt(act["start"]) and dt(act["end"]) <= dt(el["end"]), f"{rid}: activation outside eligibility"
        item_v = validity(act)                                             # per-item validity (azure-style › Privileged access r3)
    else:
        assert not unmask, f"{rid}: pii-reader and restricted-reader are eligible-only (D7)"
    if unmask:                                                             # D2: lands in {zone}-tag-mask, never in a resource policy
        return [], [{**who(pr), "roles": [], "accesses": accesses([f"{e}:select"]), "dataMaskInfo": {"dataMaskType": f"{e}:MASK_NONE"},
                     "conditions": [{"type": "expression", "values": [f"TAG._type == '{t}'"]}], "validitySchedules": item_v}
                    for t in unmask for e in MASK_ENGINES]
    out, resources = [], scope_resources(inst, s)
    for key in ["trino"] if "agreement" in s else inst["services"]:       # D4: shares only on Trino interactive
        engine, pick = SERVICES[key]
        r, svc = resources.get(engine), inst["services"][key]
        acts = sorted(pick({x.split(":", 1)[1] for x in data if x.startswith(engine + ":")}))
        if r is None or not acts: continue
        if engine == "kafka" and pr["type"] == "group" and any(".int." in t for t in res(r)["topic"]["values"]):
            acts = [x for x in acts if x != "consume"]                     # D3: clear .int. topics are read only by pipelines
        targets = [("", r, acts)]
        if engine == "kafka" and "consume" in acts:                        # consumers also need their consumer groups
            targets.append(("-cg", {"consumergroup": f"{inst['topicPrefix']}.*"}, ["consume", "describe"]))
        for suffix, rr, ac in targets:
            item = {**who(pr), "roles": [], "accesses": accesses(ac), "delegateAdmin": False}
            pol = {
                "service": svc, "zoneName": inst["zone"], "guid": guid(rid, svc + suffix), "name": f"{inst['zone']}-{rid}-{svc}{suffix}",
                "description": f"{rid}: {s.get('description', '')}", "isEnabled": True, "isAuditEnabled": True,
                "policyLabels": labels(inst_id, inst, inst_doc, [a]) + ([f"agreement:{s['agreement']['id']}"] if "agreement" in s else []),
                "resources": res(rr), "policyType": 0, "policyItems": [item],
            }
            if item_v: item["validitySchedules"] = item_v
            elif "validity" in s: pol["validitySchedules"] = validity(s["validity"])
            out.append(pol)
    return out, []

def covers(scope, inst_id, domain, dataset) -> bool:
    p = scope.strip("/").split("/")
    return p[1] == inst_id and (len(p) == 2 or (p[3] == domain and (len(p) == 4 or p[4:] == ["datasets", dataset])))

def compile_row_filters(ras: list[dict], roles: dict, insts: dict) -> list[dict]:
    """One row-filter policy per shared table, in the owning zone, Trino interactive only (D4, D6, F26)."""
    shares, out = {}, []
    for a in ras:
        if "agreement" in a["spec"]:
            _, _, inst_id, _, dom, _, ds = a["spec"]["scope"].split("/")
            shares.setdefault((inst_id, dom, ds), []).append(a)
    for (inst_id, dom, ds), agreed in shares.items():
        inst, inst_doc = insts[inst_id]
        holders = [a for a in ras if "agreement" not in a["spec"] and active(a) and covers(a["spec"]["scope"], inst_id, dom, ds)
                   and "trino:select" in roles[a["spec"]["roleDefinition"]]["dataActions"]]
        owners = sorted({a["spec"]["principal"]["name"] for a in holders if a["spec"]["principal"]["type"] == "group"})
        pipes = sorted({a["spec"]["principal"]["name"] for a in holders
                        if a["spec"]["principal"]["type"] == "workloadIdentity" and a["spec"]["roleDefinition"] == "data-contributor"})
        row = lambda groups, users, expr, **kw: {"groups": groups, "users": users, "roles": [], "accesses": accesses(["select"]),
                                                 "rowFilterInfo": {"filterExpr": expr}, **kw}
        items = [row([a["spec"]["principal"]["name"]], [], a["spec"]["agreement"]["rowFilter"], validitySchedules=validity(a["spec"]["validity"]))
                 for a in agreed]
        if owners: items.append(row(owners, [], "1 = 1"))                  # owning institution's assigned groups: all rows
        if pipes: items.append(row([], pipes, ""))                         # D6: owning pipeline, no filter, reviewed quarterly
        items.append(row(["public"], [], "1 = 0"))                         # default: no rows
        lab = labels(inst_id, inst, inst_doc, agreed + holders) + [f"agreement:{a['spec']['agreement']['id']}" for a in agreed]
        out.append({"service": inst["services"]["trino"], "zoneName": inst["zone"], "guid": guid(inst_id, ds, "rowfilter"),
                    "name": f"{inst['zone']}-{ds}-rowfilter", "isEnabled": True, "isAuditEnabled": True, "policyType": 2,
                    "description": f"Row filter for {ds}: agreements filtered, owner groups all rows, owning pipeline exempt (reviewed quarterly), everyone else no rows",
                    "policyLabels": lab + (["exemption:owning-pipeline"] if pipes else []),
                    "resources": res({"catalog": inst["catalog"], "schema": inst["domains"][dom]["datasets"][ds], "table": ds}),
                    "rowFilterPolicyItems": items})
    return out

def zone_policies(inst_id: str, inst: dict, inst_doc: dict, unmask_items: list[dict], unmask_docs: list[dict]) -> list[dict]:
    """Per zone: tag mask policy (D2) and break-glass allows with explicit access types (F11)."""
    zone, out = inst["zone"], []
    defaults = [{"groups": ["public"], "users": [], "roles": [], "accesses": accesses([f"{e}:select"]),
                 "dataMaskInfo": {"dataMaskType": f"{e}:{m}"}, "conditions": [{"type": "expression", "values": [c]}]}
                for c, m in MASKS for e in MASK_ENGINES]
    out.append({"service": TAG_SERVICE, "zoneName": zone, "guid": guid(zone, "tag-mask"), "name": f"{zone}-tag-mask",
                "isEnabled": True, "isAuditEnabled": True, "policyType": 1,
                "description": "Zone tag mask (D2, lab check: zone-scoped tag policies, TAG._type conditions); unmasked items only for activated assignments",
                "policyLabels": labels(inst_id, inst, inst_doc, unmask_docs, "tag-mask"),
                "resources": res({"tag": ["PII", "RESTRICTED"]}), "dataMaskPolicyItems": unmask_items + defaults})
    whole = scope_resources(inst, {"scope": f"/institutions/{inst_id}"})
    for key, engine in (("trino", "trino"), ("spark", "spark")):
        if key in inst["services"]:
            svc = inst["services"][key]
            out.append({"service": svc, "zoneName": zone, "guid": guid(zone, "breakglass", svc), "name": f"{zone}-breakglass-{svc}",
                        "isEnabled": True, "isAuditEnabled": True, "policyType": 0,
                        "description": "Emergency read: members only while a break-glass activation is open (conflicts #8)",
                        "policyLabels": labels(inst_id, inst, inst_doc, [], "breakglass"), "resources": res(whole[engine]),
                        "policyItems": [{"groups": ["platform_breakglass"], "users": [], "roles": [], "accesses": accesses(["select"]), "delegateAdmin": False}]})
    return out

def compile_docs(docs: list[dict]) -> list[dict]:
    for d in docs: assert d.get("kind") in ("Institution", "RoleDefinition", "RoleAssignment"), f"unknown kind {d.get('kind')} (RowFilterPolicy removed, F26)"
    roles = {d["metadata"]["id"]: d["spec"] for d in docs if d["kind"] == "RoleDefinition"}
    insts = {d["metadata"]["id"]: (d["spec"], d) for d in docs if d["kind"] == "Institution"}
    ras = [d for d in docs if d["kind"] == "RoleAssignment"]
    out, unmask = [], {i: ([], []) for i in insts}
    for a in ras:
        pols, items = compile_assignment(a, roles, insts)
        out += pols
        if items:
            u = unmask[a["spec"]["scope"].split("/")[2]]; u[0].extend(items); u[1].append(a)
    out += compile_row_filters(ras, roles, insts)
    for i, (inst, doc) in insts.items(): out += zone_policies(i, inst, doc, *unmask[i])
    return out

if __name__ == "__main__":
    json.dump(compile_docs(load(sys.argv[1:])), sys.stdout, indent=2); print()

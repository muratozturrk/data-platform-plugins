#!/usr/bin/env python3
"""Option B identity sync: Keycloak groups (platform_*) and memberships → Ranger groups/users via xusers API.
Env: KC_URL, KC_REALM, KC_TOKEN, RANGER_URL, RANGER_USER, RANGER_PASSWORD"""
import os, httpx

KC, REALM, KH = os.environ["KC_URL"].rstrip("/"), os.environ["KC_REALM"], {"Authorization": f"Bearer {os.environ['KC_TOKEN']}"}
R, AUTH = os.environ["RANGER_URL"].rstrip("/"), (os.environ["RANGER_USER"], os.environ["RANGER_PASSWORD"])

def kc(path, **params): return httpx.get(f"{KC}/admin/realms/{REALM}{path}", headers=KH, params=params, timeout=60).raise_for_status().json()

def main():
    with httpx.Client(auth=AUTH, timeout=60, headers={"Content-Type": "application/json", "Accept": "application/json"}) as c:
        have_groups = {g["name"] for g in c.get(f"{R}/service/xusers/groups", params={"pageSize": 10000}).json().get("vXGroups", [])}
        have_users = {u["name"] for u in c.get(f"{R}/service/xusers/users", params={"pageSize": 100000}).json().get("vXUsers", [])}
        for g in kc("/groups", search="platform_", max=5000):
            name = g["name"].lower()
            if name not in have_groups:
                c.post(f"{R}/service/xusers/secure/groups", json={"name": name, "description": "synced from Keycloak", "groupSource": 1}).raise_for_status()
            for m in kc(f"/groups/{g['id']}/members", max=10000):
                user = m["username"].lower()
                if user not in have_users:
                    c.post(f"{R}/service/xusers/secure/users", json={"name": user, "firstName": m.get("firstName") or user, "lastName": m.get("lastName") or "", "emailAddress": m.get("email", ""), "userRoleList": ["ROLE_USER"], "userSource": 1, "password": os.urandom(16).hex() + "aA1!", "groupNameList": [name]}).raise_for_status()
                    have_users.add(user)
                else:
                    c.post(f"{R}/service/xusers/groupusers", json={"name": name, "userId": c.get(f"{R}/service/xusers/users/userName/{user}").json()["id"], "parentGroupId": c.get(f"{R}/service/xusers/groups/groupName/{name}").json()["id"]})
    print("sync done (removals are reconciled by the Tenant controller)")

if __name__ == "__main__":
    main()

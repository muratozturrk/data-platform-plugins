#!/bin/sh
# Runs inside the superset container after db upgrade/init. Seeds two institutions from the same bundles production uses.
set -e
BASE=http://localhost:8088
superset fab create-admin --username admin --firstname a --lastname d --email admin@test.local --password "${TEST_USERS_PWD:-test}" || true
for U in t-acme-analyst t-acme-pii-reader t-acme-engineer t-globex-analyst; do
  superset fab create-user --username "$U" --firstname t --lastname u --email "$U@test.local" --password "${TEST_USERS_PWD:-test}" --role Gamma || true
done
gunicorn -w 1 -b 127.0.0.1:8088 'superset.app:create_app()' --daemon --pid /tmp/seed.pid
until curl -sf $BASE/health >/dev/null; do sleep 2; done
TOKEN=$(curl -s -X POST $BASE/api/v1/security/login -H 'Content-Type: application/json' -d "{\"username\":\"admin\",\"password\":\"${TEST_USERS_PWD:-test}\",\"provider\":\"db\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
for T in t-acme t-globex; do
  TU=$(echo $T | tr '-' '_')
  superset-cli --jwt-token "$TOKEN" $BASE sync native /fixtures/assets/$T --option tenant=$T --option tenant_catalog=$TU --option expose_sqllab=true --disallow-edits --overwrite
  superset-cli --jwt-token "$TOKEN" $BASE import-rls /fixtures/assets/$T/policies/rls.yaml
done
python3 /fixtures/apply_roles.py "$BASE" "$TOKEN" /fixtures/assets/roles.yaml          # clone Gamma → {inst}_gamma with DAR; assign users
python3 /fixtures/register_embedded.py "$BASE" "$TOKEN" sales-overview http://host-app.test
kill "$(cat /tmp/seed.pid)"

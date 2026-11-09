// T-bi-003: chart-data load per dashboard; run: k6 run -e SUPERSET_URL=... -e TOKEN=... -e QC_FILE=qc/sales-overview.json k6_dashboard_load.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

const queryContexts = new SharedArray('qc', () => JSON.parse(open(__ENV.QC_FILE)));   // one query_context per chart on the dashboard
export const options = {
  scenarios: { analysts: { executor: 'ramping-vus', stages: [{ duration: '2m', target: 50 }, { duration: '5m', target: 50 }, { duration: '1m', target: 0 }] } },
  thresholds: {
    'http_req_duration{kind:chart_data}': ['p(95)<3000'],
    'checks': ['rate>0.99'],
    'http_req_failed': ['rate<0.01'],
  },
};
export default function () {
  for (const qc of queryContexts) {
    const res = http.post(`${__ENV.SUPERSET_URL}/api/v1/chart/data`, JSON.stringify(qc),
      { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${__ENV.TOKEN}`, 'X-Institution': 't-acme' }, tags: { kind: 'chart_data' } });
    check(res, { 'status 200': r => r.status === 200, 'has rows': r => r.json('result.0.rowcount') !== undefined });
  }
  sleep(1);
}

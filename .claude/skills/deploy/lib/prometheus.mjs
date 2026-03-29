/**
 * Prometheus query helpers for BitDex metrics.
 */
import { run, portForward } from './kubectl.mjs';

const PROM_NS = 'monitoring';
const PROM_SVC = 'svc/kube-prometheus-stack-prometheus';
const PROM_PORT = 9090;
const PROM_URL = `http://localhost:${PROM_PORT}`;

/** Ensure Prometheus port-forward is running. */
export function ensurePromPortForward() {
  return portForward({
    namespace: PROM_NS,
    target: PROM_SVC,
    localPort: PROM_PORT,
    remotePort: PROM_PORT,
    healthCmd: `curl -sf ${PROM_URL}/api/v1/status/runtimeinfo 2>/dev/null | head -c 20`,
    retries: 15,
  });
}

/** Run an instant PromQL query. */
export function promQuery(query) {
  const encoded = encodeURIComponent(query);
  const result = run(`curl -sf "${PROM_URL}/api/v1/query?query=${encoded}"`, { throws: false });
  try { return JSON.parse(result); } catch { return { error: 'Failed to parse response', raw: result }; }
}

/** Run a range PromQL query. */
export function promQueryRange(query, start, end, step) {
  const encoded = encodeURIComponent(query);
  const result = run(`curl -sf "${PROM_URL}/api/v1/query_range?query=${encoded}&start=${start}&end=${end}&step=${step}"`, { throws: false });
  try { return JSON.parse(result); } catch { return { error: 'Failed to parse response', raw: result }; }
}

/** Extract a scalar value from a Prometheus query result. */
export function extractScalar(promResult) {
  if (promResult?.data?.result?.[0]?.value?.[1]) return parseFloat(promResult.data.result[0].value[1]);
  return null;
}

/** Parse a duration string (e.g., '15m', '1h', '30s') into seconds. */
export function parseDuration(s) {
  const m = s.match(/^(\d+)(s|m|h|d)$/);
  if (!m) return 900;
  const n = parseInt(m[1], 10);
  switch (m[2]) {
    case 's': return n;
    case 'm': return n * 60;
    case 'h': return n * 3600;
    case 'd': return n * 86400;
    default: return 900;
  }
}

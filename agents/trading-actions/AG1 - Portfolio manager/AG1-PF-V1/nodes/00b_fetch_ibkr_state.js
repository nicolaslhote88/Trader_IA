// Fetch read-only IBKR state before the PF dashboard reads DuckDB.
// This node never places, confirms, or modifies orders.

const input = $json || {};
const brokerUrl = String($env.IBKR_BROKER_URL || "http://ibkr-broker:8080").replace(/\/+$/, "");
const ctx = this;

async function getJson(path) {
  const url = `${brokerUrl}${path}`;
  if (ctx?.helpers?.httpRequest) {
    return await ctx.helpers.httpRequest({
      method: "GET",
      url,
      headers: { Accept: "application/json" },
      json: true,
      timeout: 20000,
    });
  }
  if (typeof fetch === "function") {
    const response = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
    const text = await response.text();
    const data = text ? JSON.parse(text) : {};
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${data?.detail || data?.error || text}`);
    }
    return data;
  }
  throw new Error("No HTTP client available in this n8n Code node");
}

const out = {
  ...input,
  ibkr_reconcile_fetch_ok: false,
  ibkr_reconcile_broker_url: brokerUrl,
  ibkr_reconcile_fetch_error: "",
  ibkr_health: {},
  ibkr_positions: [],
  ibkr_fills: [],
  ibkr_ledger: {},
};

try {
  const health = await getJson("/health");
  out.ibkr_health = health || {};

  if (health?.authenticated !== true) {
    out.ibkr_reconcile_fetch_error = "IBKR_NOT_AUTHENTICATED";
    return [{ json: out }];
  }

  const [positions, fills, ledger] = await Promise.all([
    getJson("/positions"),
    getJson("/fills"),
    getJson("/account/ledger"),
  ]);

  out.ibkr_positions = Array.isArray(positions) ? positions : [];
  out.ibkr_fills = Array.isArray(fills) ? fills : [];
  out.ibkr_ledger = ledger && typeof ledger === "object" ? ledger : {};
  out.ibkr_reconcile_fetch_ok = true;
} catch (err) {
  out.ibkr_reconcile_fetch_error = String(err?.message || err || "IBKR_FETCH_FAILED");
}

return [{ json: out }];

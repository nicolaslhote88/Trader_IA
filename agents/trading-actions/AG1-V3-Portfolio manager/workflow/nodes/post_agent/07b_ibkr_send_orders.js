// Node 07b - IBKR Send Orders (Actions/ETF)
//
// Insert between:
//   07 - Validate & Enforce Safety -> [07b] -> 08 - Build DuckDB Bundle
//
// Contract with node 07:
//   - reads input.orders (canonical AG1-V3 field)
//   - keeps the same output shape for node 08
//   - adds clientOrderId / ibkrStatus / ibkrResponse on each real order
//
// Dry-run broker validation:
//   IBKR_SEND_DRY_RUN_TO_BROKER=true sends validated workflow orders to
//   ibkr-broker while IBKR_DRY_RUN=true still prevents live IBKR submission.
//   FORCE_IBKR_CONNECTIVITY_TEST_ORDER is a canary and must stay false for
//   normal model runs.

const BROKER_URL = $env.IBKR_BROKER_URL || "http://ibkr-broker:8080";
const DRY_RUN = String($env.IBKR_DRY_RUN || "true").toLowerCase() !== "false";
const N8N_CONTEXT = this;

const SEND_DRY_RUN_TO_BROKER = String($env.IBKR_SEND_DRY_RUN_TO_BROKER || "false").toLowerCase() === "true";
const LIVE_ORDERS_ENABLED = String($env.AG1_ACTIONS_LIVE_ORDERS_ENABLED || "false").toLowerCase() === "true";
const FORCE_IBKR_CONNECTIVITY_TEST_ORDER = false;

function toNum(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function normalizeOrderType(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (s === "LIMIT") return "LMT";
  if (s === "MARKET") return "MKT";
  if (s === "LMT" || s === "MKT") return s;
  return "MKT";
}

function fnv1a32(text) {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}

function deterministicClientOrderId(seed) {
  const a = fnv1a32(`${seed}:a`);
  const b = fnv1a32(`${seed}:b`);
  const c = fnv1a32(`${seed}:c`);
  const d = fnv1a32(`${seed}:d`);
  return `${a}-${b.slice(0, 4)}-4${b.slice(5, 8)}-a${c.slice(1, 4)}-${d}${c.slice(0, 4)}`;
}

function brokerOrderId(result) {
  const raw = result?.ibkr_response || result?.details || result;
  if (Array.isArray(raw) && raw.length > 0) {
    return raw[0]?.order_id || raw[0]?.orderId || raw[0]?.id || null;
  }
  return raw?.order_id || raw?.orderId || raw?.id || null;
}

function orderKey(value) {
  return String(value || "").trim();
}

async function postJson(url, payload) {
  if (typeof fetch === "function") {
    const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    });

    const text = await response.text();
    let data = {};
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (err) {
        data = { raw: text };
      }
    }

    if (!response.ok) {
      const message = data?.detail || data?.error || text || `HTTP ${response.status}`;
      throw new Error(`HTTP ${response.status}: ${message}`);
    }

    return data;
  }

  if (N8N_CONTEXT?.helpers?.httpRequest) {
    return await N8N_CONTEXT.helpers.httpRequest({
      method: "POST",
      url,
      headers: { "Content-Type": "application/json" },
      body: payload,
      json: true,
      timeout: 15000,
    });
  }

  throw new Error("No HTTP client available in this n8n Code node");
}

const input = $json || {};
const orders = Array.isArray(input.orders)
  ? input.orders
  : (Array.isArray(input.validatedOrders) ? input.validatedOrders : []);
const runId = String(input?.ctx?.run?.runId || input.runId || input.run_id || "").trim() || `RUN_${Date.now()}`;

for (let i = 0; i < orders.length; i += 1) {
  const order = orders[i];
  if (!order.orderId) order.orderId = `ORD_${runId}_${String(i + 1).padStart(3, "0")}`;
  if (!order.clientOrderId) {
    const seed = `${runId}|${order.orderId}|${order.symbol || ""}|${order.side || ""}|${order.quantity || ""}`;
    order.clientOrderId = deterministicClientOrderId(seed);
  }
}

let ibkrResults = [];
let ibkrErrors = [];
let brokerCalled = false;
let connectivityTestInjected = false;

let actionableOrders = orders.filter((o) => (
  !["HOLD", "WATCH"].includes(String(o.signal || o.action || "").toUpperCase())
  && o.riskCheckPassed !== false
  && toNum(o.quantity) > 0
));

if (
  DRY_RUN
  && SEND_DRY_RUN_TO_BROKER
  && FORCE_IBKR_CONNECTIVITY_TEST_ORDER
  && actionableOrders.length === 0
) {
  connectivityTestInjected = true;
  actionableOrders = [{
    symbol: "AAPL",
    side: "BUY",
    quantity: 1,
    orderId: `IBKR_CONNECTIVITY_TEST_${runId}`,
    clientOrderId: deterministicClientOrderId(`${runId}|IBKR_CONNECTIVITY_TEST|AAPL|BUY|1`),
    orderType: "MKT",
    limitPrice: null,
    signal: "OPEN",
    riskCheckPassed: true,
    _ibkrConnectivityTest: true,
  }];
}

if (!DRY_RUN && !LIVE_ORDERS_ENABLED && actionableOrders.length > 0) {
  ibkrErrors = actionableOrders.map((o) => ({
    order_id: o.orderId,
    client_order_id: o.clientOrderId,
    error: "AG1_ACTIONS_LIVE_ORDERS_DISABLED",
  }));
} else if (actionableOrders.length > 0 && (!DRY_RUN || SEND_DRY_RUN_TO_BROKER)) {
  const payload = {
    orders: actionableOrders.map((o) => ({
      symbol: o.symbol,
      side: o.side || (["BUY", "OPEN", "INCREASE"].includes(String(o.signal || o.action || "").toUpperCase()) ? "BUY" : "SELL"),
      quantity: toNum(o.quantity),
      order_id: o.orderId,
      client_order_id: o.clientOrderId,
      order_type: normalizeOrderType(o.orderType),
      limit_price: o.limitPrice ?? null,
      isin: o.isin || null,
      exchange: o.exchange || null,
    })),
    run_id: runId,
  };

  try {
    brokerCalled = true;
    const response = await postJson(`${BROKER_URL}/orders/equity`, payload);
    ibkrResults = response.results || [];
    ibkrErrors = response.errors || [];
  } catch (err) {
    ibkrErrors.push({ error: `ibkr-broker unreachable: ${err.message}`, fallback: "sandbox_only" });
  }
}

const resultMap = Object.fromEntries(
  ibkrResults
    .filter((r) => r?.order_id || r?.client_order_id)
    .map((r) => [orderKey(r.order_id || r.client_order_id), r])
);
const errorMap = Object.fromEntries(
  ibkrErrors
    .filter((e) => e?.order_id || e?.client_order_id)
    .map((e) => [orderKey(e.order_id || e.client_order_id), e])
);

for (const order of orders) {
  const oid = orderKey(order.orderId);
  const cid = orderKey(order.clientOrderId);
  const result = resultMap[oid] || resultMap[cid];
  const error = errorMap[oid] || errorMap[cid];

  if (result) {
    order.ibkrStatus = result.status || "unknown";
    order.ibkrResponse = result;
    order.broker = "IBKR";
    order.brokerOrderId = brokerOrderId(result);
  } else if (error) {
    order.ibkrStatus = "error";
    order.ibkrError = error.error || "";
    order.broker = "IBKR";
  } else if (DRY_RUN) {
    order.ibkrStatus = "dry_run";
    order.broker = "SIM";
  } else {
    order.ibkrStatus = "not_sent";
    order.broker = "IBKR";
  }
}

const liveFailedOrderIds = new Set(
  !DRY_RUN
    ? orders
      .filter((o) => ["error", "not_sent", "blocked_live_disabled"].includes(String(o.ibkrStatus || "").toLowerCase()))
      .map((o) => o.orderId)
    : []
);

const ledgerOrders = orders.filter((o) => !liveFailedOrderIds.has(o.orderId));
const warnings = [
  ...(Array.isArray(input.warnings) ? input.warnings : []),
  ...orders
    .filter((o) => liveFailedOrderIds.has(o.orderId))
    .map((o) => `IBKR_ORDER_NOT_LEDGERED:${o.symbol}:${o.ibkrStatus}:${o.ibkrError || ""}`),
];

return [{
  json: {
    ...input,
    orders: ledgerOrders,
    allOrdersBeforeIbkrFilter: orders,
    warnings,
    ibkrSendSummary: {
      dryRun: DRY_RUN,
      liveOrdersEnabled: LIVE_ORDERS_ENABLED,
      sendDryRunToBroker: SEND_DRY_RUN_TO_BROKER,
      forceConnectivityTestOrder: FORCE_IBKR_CONNECTIVITY_TEST_ORDER,
      connectivityTestInjected,
      brokerCalled,
      ordersConsidered: actionableOrders.length,
      ordersSent: ibkrResults.length,
      errors: ibkrErrors.length,
      brokerUrl: BROKER_URL,
      errorsDetail: ibkrErrors,
      brokerResults: ibkrResults,
      dryRunBrokerResults: ibkrResults,
    },
  },
}];

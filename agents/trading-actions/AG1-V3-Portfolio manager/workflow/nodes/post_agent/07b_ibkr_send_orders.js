// Node 07b - IBKR Send Orders (Actions/ETF)
//
// Insert between:
//   07 - Validate & Enforce Safety -> [07b] -> 08 - Build DuckDB Bundle
//
// Contract with node 07:
//   - reads input.orders (canonical AG1-V3 field)
//   - keeps the same output shape for node 08
//   - adds clientOrderId / ibkrStatus / ibkrResponse on each order
//
// Dry-run behavior:
//   IBKR_DRY_RUN=true keeps the workflow sandbox-only and does not call the
//   broker by default. Set IBKR_SEND_DRY_RUN_TO_BROKER=true to exercise the
//   ibkr-broker dry-run endpoint while still sending no live IBKR order.

const BROKER_URL = $env.IBKR_BROKER_URL || "http://ibkr-broker:8080";
const DRY_RUN = String($env.IBKR_DRY_RUN || "true").toLowerCase() !== "false";
const SEND_DRY_RUN_TO_BROKER = String($env.IBKR_SEND_DRY_RUN_TO_BROKER || "false").toLowerCase() === "true";

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

const actionableOrders = orders.filter((o) => (
  !["HOLD", "WATCH"].includes(String(o.signal || o.action || "").toUpperCase())
  && o.riskCheckPassed !== false
  && toNum(o.quantity) > 0
));

if (actionableOrders.length > 0 && (!DRY_RUN || SEND_DRY_RUN_TO_BROKER)) {
  const payload = {
    orders: actionableOrders.map((o) => ({
      symbol: o.symbol,
      side: o.side || (["BUY", "OPEN", "INCREASE"].includes(String(o.signal || o.action || "").toUpperCase()) ? "BUY" : "SELL"),
      quantity: toNum(o.quantity),
      order_id: o.orderId,
      client_order_id: o.clientOrderId,
      order_type: normalizeOrderType(o.orderType),
      limit_price: o.limitPrice || null,
    })),
    run_id: runId,
  };

  try {
    brokerCalled = true;
    const response = await $http.request({
      method: "POST",
      url: `${BROKER_URL}/orders/equity`,
      body: payload,
      json: true,
      timeout: 15000,
    });
    ibkrResults = response.results || [];
    ibkrErrors = response.errors || [];
  } catch (err) {
    ibkrErrors.push({ error: `ibkr-broker unreachable: ${err.message}`, fallback: "sandbox_only" });
  }
}

const resultMap = Object.fromEntries(ibkrResults.map((r) => [r.order_id || r.client_order_id, r]));
const errorMap = Object.fromEntries(ibkrErrors.filter((e) => e.order_id || e.client_order_id).map((e) => [e.order_id || e.client_order_id, e]));

for (const order of orders) {
  const oid = order.orderId;
  const cid = order.clientOrderId;
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
      .filter((o) => ["error", "not_sent"].includes(String(o.ibkrStatus || "").toLowerCase()))
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
      brokerCalled,
      ordersConsidered: actionableOrders.length,
      ordersSent: ibkrResults.length,
      errors: ibkrErrors.length,
      brokerUrl: BROKER_URL,
      errorsDetail: ibkrErrors,
    },
  },
}];

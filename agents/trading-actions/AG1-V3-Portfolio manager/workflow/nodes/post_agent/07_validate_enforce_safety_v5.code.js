// Node 7 - Validate & Enforce Safety (actions/ETF/crypto only)

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }
function safeJsonParse(s) { try { return JSON.parse(s); } catch { return null; } }
function toNumOrNull(x) { const n = Number(x); return Number.isFinite(n) ? n : null; }
function toNum(x, dflt = 0) { const n = Number(x); return Number.isFinite(n) ? n : dflt; }
function normSymbol(v) { return String(v ?? "").trim().toUpperCase(); }

function normAssetClass(v) {
  const raw = String(v ?? "").trim().toUpperCase();
  if (raw === "CRYPTO") return "CRYPTO";
  if (raw === "ETF") return "ETF";
  return "EQUITY";
}

function deepClone(obj) { try { return JSON.parse(JSON.stringify(obj)); } catch { return obj; } }

function extractAgentDecisionObject(input) {
  let decisionRaw = null;

  if (isObj(input.output) && Array.isArray(input.output.actions)) return { ...input.output, _parseStatus: "OK_OBJECT_OUTPUT" };
  if (isObj(input.agentDecision)) return { ...input.agentDecision, _parseStatus: "OK_AGENTDECISION" };
  if (Array.isArray(input.output) && input.output.length > 0 && input.output.every((x) => isObj(x) && String(x.action ?? "").length)) {
    return { actions: input.output, dataCaveats: [], backfillRequests: [], _parseStatus: "OK_ACTIONS_ARRAY_OUTPUT" };
  }

  if (typeof input.output === "string") decisionRaw = input.output;
  else if (input.output?.text) decisionRaw = input.output.text;
  else if (Array.isArray(input.output) && input.output[0]?.content?.[0]?.text) decisionRaw = input.output[0].content[0].text;
  else if (input.text) decisionRaw = input.text;

  if (typeof decisionRaw === "string") {
    let cleaned = decisionRaw.trim();
    if (cleaned.includes("```")) {
      const parts = cleaned.split("```");
      if (parts.length >= 3) {
        cleaned = parts[1];
        if (cleaned.toLowerCase().startsWith("json")) cleaned = cleaned.substring(4).trim();
      }
    }
    const firstBrace = cleaned.indexOf("{");
    const lastBrace = cleaned.lastIndexOf("}");
    if (firstBrace !== -1 && lastBrace !== -1) cleaned = cleaned.substring(firstBrace, lastBrace + 1);

    const parsed = safeJsonParse(cleaned);
    if (isObj(parsed)) return { ...parsed, _parseStatus: "OK_CLEANED_JSON" };
    return { _rawText: decisionRaw, _parseStatus: "TEXT_NOT_JSON" };
  }

  return { _parseStatus: "MISSING_DECISION", _why: "No decision payload found." };
}

function coerceAgentDecisionToExpectedShape(agentDecision) {
  if (!isObj(agentDecision)) return { _parseStatus: "NOT_OBJECT", actions: [], dataCaveats: [], backfillRequests: [] };
  const d = deepClone(agentDecision);
  if (!Array.isArray(d.actions)) d.actions = [];
  if (!Array.isArray(d.dataCaveats)) d.dataCaveats = [];
  if (!Array.isArray(d.backfillRequests)) d.backfillRequests = [];
  return d;
}

function normalizeOrderType(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (s === "LMT" || s === "LIMIT") return "LIMIT";
  if (s === "MKT" || s === "MARKET") return "MARKET";
  return s || "MARKET";
}

function inferQtyFromWeightPct(portfolioSummary, weightPct, priceHint) {
  const tv = toNum(portfolioSummary?.totalPortfolioValueEUR, 0);
  const w = toNumOrNull(weightPct);
  const px = toNumOrNull(priceHint);
  if (!tv || !w || !px || px <= 0) return null;
  const eur = tv * (w / 100.0);
  const q = Math.floor(eur / px);
  return Number.isFinite(q) && q > 0 ? q : null;
}

const input = $json ?? {};
const transfer_pack = input.transfer_pack || {};
const final_db_path = transfer_pack.db_path || input.db_path || "";

const portfolioSummary =
  input.portfolioSummary ??
  input.ctx?.portfolioSummary ??
  transfer_pack.portfolioSummary ??
  { cashEUR: 0, totalPortfolioValueEUR: null, positions: [] };

const configRaw = input.config ?? input.ctx?.config ?? transfer_pack.config ?? {};
const ts = new Date().toISOString();
const runId = String(input?.run?.runId ?? input?.runId ?? transfer_pack?.run?.runId ?? "").trim() || `RUN_${Date.now()}`;
const model = String(input?.run?.model ?? transfer_pack?.run?.model ?? "UNKNOWN").trim();

let agentDecision = extractAgentDecisionObject(input);
agentDecision = coerceAgentDecisionToExpectedShape(agentDecision);

const execSet = new Set(["OPEN", "INCREASE", "DECREASE", "CLOSE"]);
const posList = portfolioSummary?.positions || [];
const posQty = {};
const posLast = {};
for (const p of posList) {
  const s = normSymbol(p.Symbol || p.symbol);
  if (!s || s === "CASH_EUR") continue;
  posQty[s] = toNum(p.Quantity ?? p.quantity, 0);
  posLast[s] = toNumOrNull(p.LastPrice ?? p.lastPrice ?? p.price);
}

const orders = [];
const warnings = [];

for (const a of agentDecision.actions || []) {
  const action = String(a.action || "").toUpperCase();
  if (!execSet.has(action)) continue;

  const symbol = normSymbol(a.symbol_internal || a.symbol);
  if (!symbol || symbol === "CASH_EUR") { warnings.push("ORDER_SKIP:NO_SYMBOL"); continue; }
  if (String(a.symbol_yahoo || "").toUpperCase().endsWith("=X") || symbol.startsWith("F" + "X:")) {
    warnings.push(`ORDER_SKIP:UNSUPPORTED_ASSET:${symbol}`);
    continue;
  }

  const assetClass = normAssetClass(a.assetClass);
  const currentQty = toNum(posQty[symbol], 0);
  const limitPx = toNumOrNull(a.entryPlan?.limitPrice);
  const lastPx = toNumOrNull(posLast[symbol]);
  const priceHint = limitPx ?? lastPx;
  let qty = null;

  if (action === "CLOSE") {
    qty = currentQty;
  } else if (action === "DECREASE") {
    const targetFinal = toNumOrNull(a.targetQty);
    if (targetFinal === null) { warnings.push(`ORDER_SKIP:DECREASE_NO_TARGETQTY:${symbol}`); continue; }
    qty = Math.max(0, currentQty - targetFinal);
  } else if (action === "INCREASE") {
    const targetFinal = toNumOrNull(a.targetQty);
    if (targetFinal === null) { warnings.push(`ORDER_SKIP:INCREASE_NO_TARGETQTY:${symbol}`); continue; }
    if (currentQty <= 0) {
      qty = Math.max(0, targetFinal);
      if (qty > 0) a.__normalized_action = "OPEN";
    } else {
      qty = Math.max(0, targetFinal - currentQty);
    }
  } else if (action === "OPEN") {
    const tq = toNumOrNull(a.targetQty);
    qty = tq !== null && tq > 0 ? tq : inferQtyFromWeightPct(portfolioSummary, a.targetWeightPct, priceHint);
  }

  if (!qty || qty <= 0) { warnings.push(`ORDER_SKIP:QTY_NONPOSITIVE:${symbol}:${action}`); continue; }

  const effectiveAction = a.__normalized_action || action;
  const side = effectiveAction === "OPEN" || effectiveAction === "INCREASE" ? "BUY" : "SELL";
  const orderType = normalizeOrderType(a.entryPlan?.orderType);

  orders.push({
    symbol,
    action: effectiveAction,
    side,
    quantity: qty,
    assetClass,
    orderType,
    limitPrice: orderType === "LIMIT" ? limitPx : null,
  });
}

let availableCash = toNum(portfolioSummary?.cashEUR, 0);
for (const o of orders) {
  if (o.side !== "SELL") continue;
  const sellPx = (normalizeOrderType(o.orderType) === "LIMIT" ? toNumOrNull(o.limitPrice) : null) ?? toNumOrNull(posLast[o.symbol]) ?? 0;
  availableCash += toNum(o.quantity, 0) * sellPx;
}

const cashSafeOrders = [];
for (const o of orders) {
  if (o.side === "SELL") {
    cashSafeOrders.push(o);
    continue;
  }

  const buyPx = (normalizeOrderType(o.orderType) === "LIMIT" ? toNumOrNull(o.limitPrice) : null) ?? toNumOrNull(posLast[o.symbol]);
  if (!buyPx || buyPx <= 0) {
    warnings.push(`ORDER_SKIP:NO_BUY_PRICE:${o.symbol}`);
    continue;
  }

  const requestedQty = toNum(o.quantity, 0);
  const affordableQty = Math.floor((availableCash + 1e-9) / buyPx);
  if (affordableQty <= 0) {
    warnings.push(`ORDER_SKIP:INSUFFICIENT_CASH:${o.symbol}:need=${(requestedQty * buyPx).toFixed(2)}:avail=${availableCash.toFixed(2)}`);
    continue;
  }
  if (affordableQty < requestedQty) {
    warnings.push(`ORDER_RESIZED:CASH_CAP:${o.symbol}:from=${requestedQty}:to=${affordableQty}`);
    o.quantity = affordableQty;
  }
  availableCash -= toNum(o.quantity, 0) * buyPx;
  cashSafeOrders.push(o);
}

orders.length = 0;
orders.push(...cashSafeOrders);

const decision = orders.length ? "TRADE" : "NO_TRADE";
if (agentDecision?._parseStatus && !String(agentDecision._parseStatus).startsWith("OK")) {
  warnings.push(`AGENT_DECISION_PARSE:${agentDecision._parseStatus}`);
}

const ctx = {
  meta: input.meta ?? transfer_pack.meta ?? {},
  config: configRaw,
  run: { runId, timestampParis: ts, model },
  portfolioSummary,
};

return [{
  json: {
    decision,
    commentary: `Decision: ${decision} | Orders: ${orders.length} | Actions: ${(agentDecision.actions || []).length}`,
    agentDecision,
    orders,
    metrics: {},
    warnings,
    ctx,
    portfolioSummary,
    transfer_pack,
    db_path: final_db_path,
  },
}];

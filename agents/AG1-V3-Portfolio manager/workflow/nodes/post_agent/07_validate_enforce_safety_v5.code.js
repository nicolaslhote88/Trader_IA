// Node 7 - Validate & Enforce Safety (v6.1) + Executable Orders Only + CLOSE=full qty

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }
function safeJsonParse(s) { try { return JSON.parse(s); } catch { return null; } }
function toNumOrNull(x) { const n = Number(x); return Number.isFinite(n) ? n : null; }
function toNum(x, dflt = 0) { const n = Number(x); return Number.isFinite(n) ? n : dflt; }
function toBool(v, dflt = false) {
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return dflt;
  if (["1", "true", "yes", "y", "on", "enabled"].includes(s)) return true;
  if (["0", "false", "no", "n", "off", "disabled"].includes(s)) return false;
  return dflt;
}
function clampText(s, n) { return String(s ?? "").replace(/\s+/g, " ").trim().slice(0, n); }
function normSymbol(v) { return String(v ?? "").trim(); }

function normAssetClass(v, symbol) {
  const raw = String(v ?? "").trim().toUpperCase();
  if (raw === "FX" || raw === "FOREX") return "FX";
  if (raw === "CRYPTO") return "CRYPTO";
  if (raw === "EQUITY" || raw === "STOCK" || raw === "ETF") return "EQUITY";
  const s = String(symbol ?? "").toUpperCase();
  if (s.startsWith("FX:") || s.endsWith("=X")) return "FX";
  return "EQUITY";
}

function fxSymbolInternal(symbol) {
  const s = String(symbol ?? "").toUpperCase();
  if (s.startsWith("FX:")) {
    const pair = s.replace("FX:", "").replace(/[^A-Z]/g, "").slice(0, 6);
    return pair.length === 6 ? `FX:${pair}` : s;
  }
  const pair = s.replace("=X", "").replace("/", "").replace(/[^A-Z]/g, "").slice(0, 6);
  return pair.length === 6 ? `FX:${pair}` : s;
}
function parseFxMeta(symbol) {
  const internal = fxSymbolInternal(symbol);
  const pair6 = internal.replace("FX:", "").replace(/[^A-Z]/g, "").slice(0, 6);
  if (pair6.length !== 6) return null;
  return { symbolInternal: `FX:${pair6}`, symbolYahoo: `${pair6}=X`, pair6 };
}

function normalizeDependencies(dep) {
  if (Array.isArray(dep)) return Array.from(new Set((dep || []).filter(Boolean).map((x) => clampText(x, 64))));
  if (isObj(dep)) {
    const out = [];
    if (dep.needPrice) out.push("PRICE");
    if (dep.needNote) out.push("NOTE");
    if (dep.needNews) out.push("NEWS");
    if (dep.needTechnical) out.push("TECHNICAL");
    if (dep.needConsensus) out.push("CONSENSUS");
    return out;
  }
  return [];
}
function deepClone(obj) { try { return JSON.parse(JSON.stringify(obj)); } catch { return obj; } }

function normalizeAgentActionsForFx(agentDecision, enableFx) {
  if (!Array.isArray(agentDecision?.actions)) return agentDecision;
  const next = [];
  for (const raw of agentDecision.actions) {
    const a = isObj(raw) ? deepClone(raw) : {};
    const symRaw = normSymbol(a.symbol_internal || a.symbol || a.symbol_yahoo);
    const assetClass = normAssetClass(a.assetClass ?? a.asset_class, symRaw);
    const actIn = String(a.action ?? "").toUpperCase();

    a.assetClass = assetClass;
    a.asset_class = undefined;

    if (assetClass === "FX") {
      const meta = parseFxMeta(symRaw);
      if (meta) {
        a.symbol = meta.symbolInternal;
        a.symbol_internal = meta.symbolInternal;
        a.symbol_yahoo = meta.symbolYahoo;
      }

      if (!enableFx) {
        let fxAction = actIn;
        if (["OPEN", "INCREASE", "BUY"].includes(actIn)) fxAction = "PROPOSE_OPEN";
        else if (["DECREASE", "CLOSE", "SELL"].includes(actIn)) fxAction = "PROPOSE_CLOSE";
        else if (!["WATCH", "HOLD"].includes(actIn)) fxAction = "WATCH";
        a.action = fxAction;
        a.execution_required = false;
        a.needs_risk_approval = true;
        const deps = normalizeDependencies(a.dependencies);
        if (!deps.includes("AG5_RISK_APPROVAL")) deps.push("AG5_RISK_APPROVAL");
        if (!deps.includes("AG6_EXECUTION")) deps.push("AG6_EXECUTION");
        a.dependencies = deps;
      } else {
        let fxAction = actIn;
        if (actIn === "BUY") fxAction = "OPEN";
        if (actIn === "SELL") fxAction = "CLOSE";
        if (!["OPEN", "INCREASE", "DECREASE", "CLOSE", "WATCH", "HOLD"].includes(fxAction)) fxAction = "WATCH";
        a.action = fxAction;
        a.dependencies = normalizeDependencies(a.dependencies).filter((d) => d !== "AG5_RISK_APPROVAL" && d !== "AG6_EXECUTION");
        a.execution_required = ["OPEN", "INCREASE", "DECREASE", "CLOSE"].includes(fxAction);
        a.needs_risk_approval = false;
      }
    } else {
      if (a.execution_required === undefined) a.execution_required = null;
      if (a.needs_risk_approval === undefined) a.needs_risk_approval = null;
    }

    next.push(a);
  }
  agentDecision.actions = next;
  return agentDecision;
}

function extractAgentDecisionObject(input) {
  let decisionRaw = null;

  // 1) output = objet deja conforme
  if (isObj(input.output) && Array.isArray(input.output.actions)) return { ...input.output, _parseStatus: "OK_OBJECT_OUTPUT" };

  // 2) agentDecision deja present
  if (isObj(input.agentDecision)) return { ...input.agentDecision, _parseStatus: "OK_AGENTDECISION" };

  // 3) output = array d'actions
  if (Array.isArray(input.output) && input.output.length > 0 && input.output.every((x) => isObj(x) && String(x.action ?? "").length)) {
    return { actions: input.output, dataCaveats: [], backfillRequests: [], _parseStatus: "OK_ACTIONS_ARRAY_OUTPUT" };
  }

  // 4) output = string JSON
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

// ------------------------------
// Main
// ------------------------------
const input = $json ?? {};
const transfer_pack = input.transfer_pack || {};
const final_db_path = transfer_pack.db_path || input.db_path || "";

// IMPORTANT: portfolioSummary est en racine (merge)
const portfolioSummary =
  input.portfolioSummary ??
  input.ctx?.portfolioSummary ??
  transfer_pack.portfolioSummary ??
  { cashEUR: 0, totalPortfolioValueEUR: null, positions: [] };

const configRaw = input.config ?? input.ctx?.config ?? transfer_pack.config ?? {};
const enableFx = toBool(input?.run?.enable_fx ?? input?.run?.enableFx ?? configRaw.enable_fx, true);

const ts = new Date().toISOString();
const runId = String(input?.run?.runId ?? input?.runId ?? transfer_pack?.run?.runId ?? "").trim() || `RUN_${Date.now()}`;
const model = String(input?.run?.model ?? transfer_pack?.run?.model ?? "UNKNOWN").trim();

let agentDecision = extractAgentDecisionObject(input);
agentDecision = coerceAgentDecisionToExpectedShape(agentDecision);
agentDecision = normalizeAgentActionsForFx(agentDecision, enableFx);

// ------------------------------
// Build executable orders only
// ------------------------------
const execSet = new Set(["OPEN", "INCREASE", "DECREASE", "CLOSE"]);

const posList = portfolioSummary?.positions || [];
const posQty = {};
const posLast = {};
for (const p of posList) {
  const s = normSymbol(p.Symbol);
  if (!s || s === "CASH_EUR") continue;
  posQty[s] = toNum(p.Quantity, 0);
  posLast[s] = toNumOrNull(p.LastPrice);
}

function inferQtyFromWeightPct(weightPct, priceHint) {
  const tv = toNum(portfolioSummary?.totalPortfolioValueEUR, 0);
  const w = toNumOrNull(weightPct);
  const px = toNumOrNull(priceHint);
  if (!tv || !w || !px || px <= 0) return null;
  const eur = tv * (w / 100.0);
  const q = Math.floor(eur / px);
  return Number.isFinite(q) && q > 0 ? q : null;
}

function normalizeOrderType(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (s === "LMT" || s === "LIMIT") return "LIMIT";
  if (s === "MKT" || s === "MARKET") return "MARKET";
  return s || "MARKET";
}

const orders = [];
const warnings = [];

for (const a of agentDecision.actions || []) {
  const action = String(a.action || "").toUpperCase();
  if (!execSet.has(action)) continue; // skip HOLD/WATCH/PROPOSE_*

  const symbol = normSymbol(a.symbol_internal || a.symbol);
  if (!symbol || symbol === "CASH_EUR") { warnings.push("ORDER_SKIP:NO_SYMBOL"); continue; }

  const assetClass = String(a.assetClass || "EQUITY").toUpperCase();
  if (assetClass === "FX" && !enableFx) { warnings.push(`ORDER_SKIP:FX_DISABLED:${symbol}`); continue; }

  const currentQty = toNum(posQty[symbol], 0);
  const limitPx = toNumOrNull(a.entryPlan?.limitPrice);
  const lastPx = toNumOrNull(posLast[symbol]);
  const priceHint = limitPx ?? lastPx;

  let qty = null;

  if (action === "CLOSE") {
    qty = currentQty; // FULL CLOSE
  } else if (action === "DECREASE") {
    const targetFinal = toNumOrNull(a.targetQty);
    if (targetFinal === null) { warnings.push(`ORDER_SKIP:DECREASE_NO_TARGETQTY:${symbol}`); continue; }
    qty = Math.max(0, currentQty - targetFinal);
  } else if (action === "INCREASE") {
    const targetFinal = toNumOrNull(a.targetQty);
    if (targetFinal === null) { warnings.push(`ORDER_SKIP:INCREASE_NO_TARGETQTY:${symbol}`); continue; }
    if (currentQty <= 0) {
      // Robust fallback: treat INCREASE as OPEN if no existing position was detected.
      qty = Math.max(0, targetFinal);
      if (qty > 0) a.__normalized_action = "OPEN";
    } else {
      qty = Math.max(0, targetFinal - currentQty);
    }
  } else if (action === "OPEN") {
    const tq = toNumOrNull(a.targetQty);
    if (tq !== null && tq > 0) qty = tq;
    else qty = inferQtyFromWeightPct(a.targetWeightPct, priceHint);
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
// ------------------------------
// Cash affordability guardrail
// ------------------------------
const startingCash = toNum(portfolioSummary?.cashEUR, 0);
let availableCash = startingCash;

// 1) crediter d'abord les ventes estimees
for (const o of orders) {
  if (o.side !== "SELL") continue;

  const sellPx =
    (normalizeOrderType(o.orderType) === "LIMIT" ? toNumOrNull(o.limitPrice) : null) ??
    toNumOrNull(posLast[o.symbol]) ??
    0;

  availableCash += toNum(o.quantity, 0) * sellPx;
}

const cashSafeOrders = [];

for (const o of orders) {
  // les SELL passent tels quels
  if (o.side === "SELL") {
    cashSafeOrders.push(o);
    continue;
  }

  const buyPx =
    (normalizeOrderType(o.orderType) === "LIMIT" ? toNumOrNull(o.limitPrice) : null) ??
    toNumOrNull(posLast[o.symbol]);

  if (!buyPx || buyPx <= 0) {
    warnings.push(`ORDER_SKIP:NO_BUY_PRICE:${o.symbol}`);
    continue;
  }

  const requestedQty = toNum(o.quantity, 0);
  const affordableQty = Math.floor((availableCash + 1e-9) / buyPx);

  if (affordableQty <= 0) {
    warnings.push(
      `ORDER_SKIP:INSUFFICIENT_CASH:${o.symbol}:need=${(requestedQty * buyPx).toFixed(2)}:avail=${availableCash.toFixed(2)}`
    );
    continue;
  }

  if (affordableQty < requestedQty) {
    warnings.push(
      `ORDER_RESIZED:CASH_CAP:${o.symbol}:from=${requestedQty}:to=${affordableQty}`
    );
    o.quantity = affordableQty;
  }

  availableCash -= toNum(o.quantity, 0) * buyPx;
  cashSafeOrders.push(o);
}

// remplace les ordres initiaux par la version cash-safe
orders.length = 0;
orders.push(...cashSafeOrders);

// Decision = TRADE only if executable orders exist
const decision = orders.length ? "TRADE" : "NO_TRADE";

if (agentDecision?._parseStatus && !String(agentDecision._parseStatus).startsWith("OK")) {
  warnings.push(`AGENT_DECISION_PARSE:${agentDecision._parseStatus}`);
}

const ctx = {
  meta: input.meta ?? transfer_pack.meta ?? {},
  config: configRaw,
  run: { runId, timestampParis: ts, model, enable_fx: enableFx },
  portfolioSummary
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
    // IMPORTANT: republie portfolioSummary en RACINE pour que Node 8 priceMap fonctionne meme sans modif
    portfolioSummary,
    transfer_pack,
    db_path: final_db_path
  }
}];

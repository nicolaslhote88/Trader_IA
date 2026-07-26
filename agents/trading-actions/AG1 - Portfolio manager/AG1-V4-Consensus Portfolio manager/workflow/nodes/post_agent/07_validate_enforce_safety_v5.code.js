// Node 7 - Validate & Enforce Safety (actions/ETF only)

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }
function safeJsonParse(s) { try { return JSON.parse(s); } catch { return null; } }
function toNumOrNull(x) { const n = Number(x); return Number.isFinite(n) ? n : null; }
function toNum(x, dflt = 0) { const n = Number(x); return Number.isFinite(n) ? n : dflt; }
function normSymbol(v) { return String(v ?? "").trim().toUpperCase(); }
function clampText(v, max = 0) {
  const s = String(v ?? "").replace(/\s+/g, " ").trim();
  return max > 0 ? s.slice(0, max) : s;
}

function normAssetClass(v) {
  const raw = String(v ?? "").trim().toUpperCase();
  if (raw === "ETF") return "ETF";
  if (raw === "EQUITY" || raw === "STK" || raw === "STOCK" || !raw) return "EQUITY";
  return raw;
}

function deepClone(obj) { try { return JSON.parse(JSON.stringify(obj)); } catch { return obj; } }

function cfgNumber(config, keys, dflt) {
  for (const key of keys) {
    const n = toNumOrNull(config?.[key]);
    if (n !== null) return n;
  }
  return dflt;
}

function cfgBool(config, keys, dflt = false) {
  for (const key of keys) {
    if (config?.[key] === undefined || config?.[key] === null) continue;
    const s = String(config[key]).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on", "enabled"].includes(s)) return true;
    if (["0", "false", "no", "n", "off", "disabled"].includes(s)) return false;
  }
  return dflt;
}

function pick(row, ...keys) {
  for (const key of keys) {
    const v = row?.[key];
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return null;
}

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

function actionDataAge(action, keyVariants) {
  for (const key of keyVariants) {
    const n = toNumOrNull(action?.[key]);
    if (n !== null) return n;
  }
  return null;
}

function actionFlagText(action) {
  const raw = action?.dataQualityFlags ?? action?.Data_Quality_Flags ?? action?.data_quality_flags ?? "";
  return Array.isArray(raw) ? raw.join(",") : String(raw || "");
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

const limits = {
  maxPosPct: cfgNumber(configRaw, ["max_pos_pct", "maxPositionPct", "max_position_pct"], 25),
  maxSectorPct: cfgNumber(configRaw, ["max_sector_pct", "maxSectorPct"], 40),
  maxOrderValuePct: cfgNumber(configRaw, ["max_order_value_pct", "maxOrderValuePct"], 15),
  minOrderValueEUR: cfgNumber(configRaw, ["min_order_value_eur", "minOrderValueEUR"], 1000),
  maxOpenPositions: cfgNumber(configRaw, ["max_open_positions", "maxOpenPositions"], 10),
  defaultFeeBps: cfgNumber(configRaw, ["default_fee_bps", "fee_bps", "defaultFeeBps"], 10),
  maxSpreadPct: cfgNumber(configRaw, ["max_spread_pct", "maxSpreadPct"], 1.5),
  maxH1AgeHours: cfgNumber(configRaw, ["max_h1_age_hours", "maxH1AgeHours"], 96),
  maxD1AgeHours: cfgNumber(configRaw, ["max_d1_age_hours", "maxD1AgeHours"], 96),
  requireLimitBuys: cfgBool(configRaw, ["require_limit_buys", "requireLimitBuys"], true),
  killSwitchActive: cfgBool(configRaw, ["kill_switch_active", "killSwitchActive"], false),
};

let agentDecision = extractAgentDecisionObject(input);
agentDecision = coerceAgentDecisionToExpectedShape(agentDecision);

const execSet = new Set(["OPEN", "INCREASE", "DECREASE", "CLOSE"]);
const posList = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
const posQty = {};
const posLast = {};
const posSector = {};
const posMarketValue = {};
let inferredEquity = 0;
for (const p of posList) {
  const s = normSymbol(pick(p, "Symbol", "symbol"));
  if (!s || s === "CASH_EUR" || s === "__META__") continue;
  const qty = toNum(pick(p, "Quantity", "quantity", "qty"), 0);
  const px = toNumOrNull(pick(p, "LastPrice", "lastPrice", "price"));
  const mv = toNum(pick(p, "MarketValue", "marketValue", "market_value", "MarketValueEUR", "marketValueEUR", "value"), qty * (px || 0));
  posQty[s] = qty;
  posLast[s] = px;
  posSector[s] = clampText(pick(p, "Sector", "sector") || "UNKNOWN", 128) || "UNKNOWN";
  posMarketValue[s] = mv;
  inferredEquity += Math.max(0, mv);
}

const cashEUR = toNum(portfolioSummary?.cashEUR, 0);
const portfolioValue = toNum(portfolioSummary?.totalPortfolioValueEUR, cashEUR + inferredEquity) || Math.max(1, cashEUR + inferredEquity);
// Nombre de lignes distinctes reellement detenues (pour plafonner les nouvelles ouvertures).
const openPositionsCount = Object.values(posQty).filter((q) => toNum(q, 0) > 0).length;
const sectorValue = {};
for (const [sym, mv] of Object.entries(posMarketValue)) {
  const sector = posSector[sym] || "UNKNOWN";
  sectorValue[sector] = (sectorValue[sector] || 0) + Math.max(0, mv);
}

const orders = [];
const warnings = [];
const rejects = [];

function reject(symbol, code, detail = "") {
  const msg = `ORDER_REJECT:${code}:${symbol}${detail ? `:${detail}` : ""}`;
  warnings.push(msg);
  rejects.push(msg);
}

for (const a of agentDecision.actions || []) {
  const action = String(a.action || "").toUpperCase();
  if (!execSet.has(action)) continue;

  const symbol = normSymbol(a.symbol_internal || a.symbol);
  if (!symbol || symbol === "CASH_EUR" || symbol === "__META__") { reject("UNKNOWN", "NO_SYMBOL"); continue; }
  if (String(a.symbol_yahoo || "").toUpperCase().endsWith("=X") || symbol.startsWith("F" + "X:")) {
    reject(symbol, "UNSUPPORTED_ASSET");
    continue;
  }

  const assetClass = normAssetClass(a.assetClass ?? a.AssetClass);
  if (!["EQUITY", "ETF"].includes(assetClass)) {
    reject(symbol, "UNSUPPORTED_ASSET_CLASS", assetClass);
    continue;
  }

  const currentQty = toNum(posQty[symbol], 0);
  const limitPx = toNumOrNull(a.entryPlan?.limitPrice ?? a.limitPrice);
  const lastPx = toNumOrNull(posLast[symbol]);
  const priceHint = limitPx ?? toNumOrNull(a.priceHint) ?? lastPx;
  let qty = null;

  if (action === "CLOSE") {
    qty = currentQty;
  } else if (action === "DECREASE") {
    const targetFinal = toNumOrNull(a.targetQty);
    if (targetFinal === null) { reject(symbol, "DECREASE_NO_TARGETQTY"); continue; }
    qty = Math.max(0, currentQty - targetFinal);
  } else if (action === "INCREASE") {
    const targetFinal = toNumOrNull(a.targetQty);
    if (targetFinal === null) { reject(symbol, "INCREASE_NO_TARGETQTY"); continue; }
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

  qty = Math.floor(toNum(qty, 0));
  if (!qty || qty <= 0) { reject(symbol, "QTY_NONPOSITIVE", action); continue; }

  const effectiveAction = a.__normalized_action || action;
  const side = effectiveAction === "OPEN" || effectiveAction === "INCREASE" ? "BUY" : "SELL";
  const orderType = normalizeOrderType(a.entryPlan?.orderType ?? a.orderType);
  const buyPx = orderType === "LIMIT" ? limitPx : priceHint;
  const notionalPx = buyPx ?? priceHint;
  if (!notionalPx || notionalPx <= 0) { reject(symbol, "NO_PRICE"); continue; }

  if (side === "BUY" && limits.killSwitchActive) {
    reject(symbol, "KILL_SWITCH_BUY_BLOCKED");
    continue;
  }
  if (side === "BUY" && limits.requireLimitBuys && orderType !== "LIMIT") {
    reject(symbol, "BUY_REQUIRES_LIMIT");
    continue;
  }
  if (side === "SELL" && currentQty <= 0) {
    reject(symbol, "SELL_WITHOUT_POSITION");
    continue;
  }
  if (side === "SELL" && qty > currentQty) {
    warnings.push(`ORDER_RESIZED:SELL_POSITION_CAP:${symbol}:from=${qty}:to=${currentQty}`);
    qty = Math.floor(currentQty);
  }

  const h1Age = actionDataAge(a, ["Data_Age_H1_Hours", "dataAgeH1Hours", "data_age_h1_hours"]);
  const d1Age = actionDataAge(a, ["Data_Age_D1_Hours", "dataAgeD1Hours", "data_age_d1_hours"]);
  const spreadPct = actionDataAge(a, ["SpreadPct", "spreadPct", "spread_pct"]);
  const flags = actionFlagText(a).toUpperCase();
  const liquidityStatus = String(a?.liquidity?.status || "").trim().toUpperCase();
  const contractResolved = a?.liquidity?.contractResolved;
  const orderToVolumePct = toNumOrNull(a?.liquidity?.estimatedOrderToVolumePct);
  if (side === "BUY" && h1Age !== null && h1Age > limits.maxH1AgeHours) { reject(symbol, "STALE_H1", String(h1Age)); continue; }
  if (side === "BUY" && d1Age !== null && d1Age > limits.maxD1AgeHours) { reject(symbol, "STALE_D1", String(d1Age)); continue; }
  if (side === "BUY" && flags.includes("STALE_YF")) { reject(symbol, "STALE_YF"); continue; }
  if (side === "BUY" && (flags.includes("TECH_BARS_NOT_CLOSED") || flags.includes("TECH_STATUS_NOT_OK"))) {
    reject(symbol, "TECH_CLOSED_BAR_GATE", flags);
    continue;
  }
  if (side === "BUY" && ["UNKNOWN", "STRESS"].includes(liquidityStatus)) { reject(symbol, `LIQUIDITY_${liquidityStatus}`); continue; }
  if (side === "BUY" && contractResolved !== true) { reject(symbol, "IBKR_CONTRACT_UNRESOLVED"); continue; }
  if (side === "BUY" && ["LIQUIDITY_UNKNOWN", "LIQUIDITY_STRESS", "IBKR_CONTRACT_UNRESOLVED", "STALE_QUOTE", "PRICE_DIVERGENCE"].some((code) => flags.includes(code))) {
    reject(symbol, "LIQUIDITY_GATE", flags);
    continue;
  }
  // A null spread is tolerated only when the preflight vouched for the name's
  // liquidity (SPREAD_UNQUOTED) and the status resolved to OK. BUYs are LIMIT-only.
  const spreadUnquotedOk = flags.includes("SPREAD_UNQUOTED") && liquidityStatus === "OK";
  if (side === "BUY" && spreadPct === null && !spreadUnquotedOk) { reject(symbol, "LIQUIDITY_UNKNOWN", "spread"); continue; }
  if (side === "BUY" && spreadPct !== null && spreadPct > limits.maxSpreadPct) { reject(symbol, "SPREAD_TOO_WIDE", String(spreadPct)); continue; }

  const grossNotional = qty * notionalPx;
  const expectedFeesEUR = grossNotional * limits.defaultFeeBps / 10000.0;
  const postSymbolValue = Math.max(0, (posMarketValue[symbol] || 0) + (side === "BUY" ? grossNotional : -grossNotional));
  const postSymbolPct = portfolioValue > 0 ? (postSymbolValue / portfolioValue) * 100 : 0;
  const sector = clampText(a.sector ?? a.Sector ?? posSector[symbol] ?? "UNKNOWN", 128) || "UNKNOWN";
  const postSectorValue = Math.max(0, (sectorValue[sector] || 0) + (side === "BUY" ? grossNotional : -grossNotional));
  const postSectorPct = portfolioValue > 0 ? (postSectorValue / portfolioValue) * 100 : 0;
  const orderValuePct = portfolioValue > 0 ? (grossNotional / portfolioValue) * 100 : 0;

  // Plancher de ticket : rejette les micro-ordres d'achat manges par les frais fixes.
  if (side === "BUY" && grossNotional < limits.minOrderValueEUR) { reject(symbol, "MIN_ORDER_VALUE_EUR", `${grossNotional.toFixed(0)}<${limits.minOrderValueEUR}`); continue; }
  // Plafond du nombre de lignes : rejette une NOUVELLE ouverture au-dela du max (concentration).
  const isNewOpen = side === "BUY" && (action === "OPEN" || currentQty <= 0);
  if (isNewOpen && openPositionsCount >= limits.maxOpenPositions) { reject(symbol, "MAX_OPEN_POSITIONS", `${openPositionsCount}>=${limits.maxOpenPositions}`); continue; }
  if (side === "BUY" && postSymbolPct > limits.maxPosPct) { reject(symbol, "MAX_POSITION_PCT", postSymbolPct.toFixed(2)); continue; }
  if (side === "BUY" && sector !== "UNKNOWN" && postSectorPct > limits.maxSectorPct) { reject(symbol, "MAX_SECTOR_PCT", `${sector}:${postSectorPct.toFixed(2)}`); continue; }
  if (side === "BUY" && orderValuePct > limits.maxOrderValuePct) { reject(symbol, "MAX_ORDER_VALUE_PCT", orderValuePct.toFixed(2)); continue; }

  orders.push({
    symbol,
    action: effectiveAction,
    side,
    quantity: qty,
    assetClass,
    sector,
    isin: clampText(a.isin ?? a.ISIN ?? "", 64) || null,
    orderType,
    limitPrice: orderType === "LIMIT" ? limitPx : null,
    priceHint: notionalPx,
    estNotionalEUR: Math.round(grossNotional * 100) / 100,
    expectedFeesEUR: Math.round(expectedFeesEUR * 100) / 100,
    riskCheckPassed: true,
    riskChecks: {
      postSymbolPct,
      postSectorPct,
      orderValuePct,
      h1Age,
      d1Age,
      spreadPct,
      liquidityStatus,
      contractResolved,
      orderToVolumePct,
      dataFlags: flags,
    },
  });
}

let availableCash = cashEUR;
for (const o of orders) {
  if (o.side !== "SELL") continue;
  const sellPx = (normalizeOrderType(o.orderType) === "LIMIT" ? toNumOrNull(o.limitPrice) : null) ?? toNumOrNull(posLast[o.symbol]) ?? toNumOrNull(o.priceHint) ?? 0;
  availableCash += toNum(o.quantity, 0) * sellPx;
}

const cashSafeOrders = [];
for (const o of orders) {
  if (o.side === "SELL") {
    cashSafeOrders.push(o);
    continue;
  }

  const buyPx = (normalizeOrderType(o.orderType) === "LIMIT" ? toNumOrNull(o.limitPrice) : null) ?? toNumOrNull(o.priceHint) ?? toNumOrNull(posLast[o.symbol]);
  if (!buyPx || buyPx <= 0) {
    reject(o.symbol, "NO_BUY_PRICE");
    continue;
  }

  const requestedQty = toNum(o.quantity, 0);
  const affordableQty = Math.floor((availableCash + 1e-9) / (buyPx * (1 + limits.defaultFeeBps / 10000.0)));
  if (affordableQty <= 0) {
    reject(o.symbol, "INSUFFICIENT_CASH", `need=${(requestedQty * buyPx).toFixed(2)}:avail=${availableCash.toFixed(2)}`);
    continue;
  }
  if (affordableQty < requestedQty) {
    warnings.push(`ORDER_RESIZED:CASH_CAP:${o.symbol}:from=${requestedQty}:to=${affordableQty}`);
    o.quantity = affordableQty;
    o.estNotionalEUR = Math.round(affordableQty * buyPx * 100) / 100;
    o.expectedFeesEUR = Math.round((affordableQty * buyPx * limits.defaultFeeBps / 10000.0) * 100) / 100;
  }
  availableCash -= toNum(o.estNotionalEUR, 0) + toNum(o.expectedFeesEUR, 0);
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
  safetyLimits: limits,
};

return [{
  json: {
    decision,
    commentary: `Decision: ${decision} | Orders: ${orders.length} | Actions: ${(agentDecision.actions || []).length} | Rejects: ${rejects.length}`,
    agentDecision,
    orders,
    metrics: {
      rejects: rejects.length,
      availableCashAfterChecksEUR: Math.round(availableCash * 100) / 100,
    },
    warnings,
    ctx,
    portfolioSummary,
    transfer_pack,
    db_path: final_db_path,
  },
}];

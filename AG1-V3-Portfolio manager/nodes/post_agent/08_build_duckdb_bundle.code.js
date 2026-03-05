function normalizeDbPath(v) {
  const s = String(v ?? "").trim().replace(/\\/g, "/");
  return s.replace("/local-files/", "/files/");
}

function toNum(v, dflt = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
}

function clampText(v, max = 0) {
  const s = String(v ?? "").replace(/\s+/g, " ").trim();
  return max > 0 ? s.slice(0, max) : s;
}

function mapActionToSignal(action) {
  const a = String(action || "").toUpperCase();
  if (["OPEN", "INCREASE", "BUY"].includes(a)) return "BUY";
  if (["DECREASE", "CLOSE", "SELL"].includes(a)) return "SELL";
  if (a === "WATCH") return "WATCH";
  if (a === "HOLD") return "HOLD";
  if (a === "PROPOSE_OPEN") return "PROPOSE_OPEN";
  if (a === "PROPOSE_CLOSE") return "PROPOSE_CLOSE";
  return "NEUTRAL";
}

function normalizeOrderType(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (s === "LMT" || s === "LIMIT") return "LIMIT";
  if (s === "MKT" || s === "MARKET") return "MARKET";
  return s || "MARKET";
}

function extractSymbolFromText(text) {
  const m = String(text || "").toUpperCase().match(/\bFX:[A-Z]{6}\b|\b[A-Z0-9]{1,10}(?:\.[A-Z]{1,4})?\b/);
  return m ? m[0] : "GLOBAL";
}

const input = $json || {};
const ctx = input.ctx || {};
const runCtx = ctx.run || {};
const agentDecision = input.agentDecision || {};
const ordersIn = Array.isArray(input.orders) ? input.orders : [];
const warnings = Array.isArray(input.warnings) ? input.warnings : [];
const ts_end = new Date().toISOString();
const run_id = runCtx.runId || `RUN_${Date.now()}`;

// IMPORTANT: portfolio is in ctx (from node 7)
const portfolioSummary = input.portfolioSummary || ctx.portfolioSummary || { positions: [] };

// --- PRICE MAP ---
const priceMap = {};
if (Array.isArray(portfolioSummary.positions)) {
  portfolioSummary.positions.forEach((p) => {
    const sym = String(p.Symbol ?? "").trim();
    const px = Number(p.LastPrice);
    if (sym && Number.isFinite(px) && px > 0) priceMap[sym] = px;
  });
}

// complete with limit prices from actions/orders
if (Array.isArray(agentDecision.actions)) {
  agentDecision.actions.forEach((a) => {
    const sym = String(a.symbol_internal || a.symbol || "").trim();
    const lp = Number(a.entryPlan?.limitPrice);
    if (sym && !(sym in priceMap) && Number.isFinite(lp) && lp > 0) priceMap[sym] = lp;
  });
}

ordersIn.forEach((o) => {
  const sym = String(o.symbol || "").trim();
  const lp = Number(o.limitPrice);
  if (sym && !(sym in priceMap) && Number.isFinite(lp) && lp > 0) priceMap[sym] = lp;
});

const ai_signals = [];
if (Array.isArray(agentDecision.actions)) {
  agentDecision.actions.forEach((a, i) => {
    const symbol = String(a.symbol_internal || a.symbol || "").trim();
    if (!symbol || symbol === "CASH_EUR") return;
    const confidence = toNum(a.confidence, null);
    const signal = mapActionToSignal(a.action);
    const horizonDays = toNum(a.horizonDays, null);
    ai_signals.push({
      signal_id: `SIG_${run_id}_${i}`,
      ts: ts_end,
      symbol,
      signal,
      confidence: confidence == null ? null : Math.max(0, Math.min(100, Math.round(confidence))),
      horizon: Number.isFinite(horizonDays) ? `D${Math.max(1, Math.round(horizonDays))}` : null,
      entry_zone: clampText(a.entryPlan?.orderType || "", 32) || null,
      stop_loss: toNum(a.riskPlan?.stopLossPct, null),
      take_profit: toNum(a.riskPlan?.takeProfitPct, null),
      risk_score: confidence == null ? null : Math.max(0, Math.min(100, 100 - Math.round(confidence))),
      catalyst: null,
      rationale: clampText(a.rationale, 2048) || null,
      payload_json: a,
    });
  });
}

const alerts = [];
warnings.forEach((w, i) => {
  const msg = clampText(w, 2048);
  if (!msg) return;
  alerts.push({
    alert_id: `ALT_${run_id}_${i}`,
    ts: ts_end,
    severity: "WARN",
    category: "AGENT",
    symbol: extractSymbolFromText(msg),
    message: msg,
    code: "AGENT_WARNING",
    payload_json: { warning: String(w) },
  });
});

if (String(input.decision || "").toUpperCase() === "NO_TRADE" && warnings.length === 0) {
  alerts.push({
    alert_id: `ALT_${run_id}_NOTRADE`,
    ts: ts_end,
    severity: "INFO",
    category: "EXECUTION",
    symbol: "GLOBAL",
    message: "No executable orders for this run",
    code: "NO_TRADE",
    payload_json: { decision: input.decision || "NO_TRADE" },
  });
}

const bundle = {
  run: {
    run_id,
    ts_start: runCtx.timestampParis || ts_end,
    ts_end,
    tz: "Europe/Paris",
    model: runCtx.model || "UNKNOWN",
    decision_summary: input.decision || "NO_TRADE",
    agent_output_json: agentDecision,
    warnings_json: warnings,
  },
  orders: ordersIn.map((o, i) => ({
    order_id: `ORD_${run_id}_${i}`,
    symbol: o.symbol,
    side: o.side,
    order_type: normalizeOrderType(o.orderType),
    qty: o.quantity,
    limit_price: o.limitPrice ?? null,
  })),
  fills: ordersIn.map((o, i) => {
    const sym = String(o.symbol || "").trim();
    const orderType = normalizeOrderType(o.orderType);
    const px =
      (orderType === "LIMIT" && o.limitPrice) ? Number(o.limitPrice) :
      (priceMap[sym] ? Number(priceMap[sym]) : null);

    return {
      fill_id: `FIL_${run_id}_${i}`,
      order_id: `ORD_${run_id}_${i}`,
      symbol: sym,
      side: o.side,
      qty: o.quantity,
      price: (Number.isFinite(px) && px > 0) ? px : 1.0, // dernier fallback
    };
  }),
  cash_ledger: [],
  market_prices: Object.entries(priceMap).map(([sym, px]) => ({ symbol: sym, close: px })),
  ai_signals,
  alerts,
};

return [{
  json: {
    run_id,
    db_path: normalizeDbPath(input.db_path || "/files/duckdb/ag1_v3.duckdb"),
    bundle,
    summary: {
      decision: input.decision,
      orders: ordersIn.length,
      fills: bundle.fills.length,
      ai_signals: ai_signals.length,
      alerts: alerts.length,
    }
  }
}];

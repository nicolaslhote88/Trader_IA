// 8 - Build DuckDB Bundle (Portfolio Ledger v2)
// Input: Node 7 output (decision, orders, warnings, ctx, agentDecision, metrics)
// Output: [{ json: { run_id, db_path, bundle, summary } }]

function isObj(x) {
  return x && typeof x === "object" && !Array.isArray(x);
}

function toNum(v, d = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function toInt(v, d = null) {
  const n = toNum(v, d);
  return Number.isFinite(n) ? Math.round(n) : d;
}

function clampText(v, max = 0) {
  const s = String(v ?? "").replace(/\s+/g, " ").trim();
  return max > 0 ? s.slice(0, max) : s;
}

function normSymbol(v) {
  return String(v ?? "").trim().toUpperCase();
}

function uniq(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function normPctTo100(v) {
  const n = toNum(v, null);
  if (n == null) return null;
  if (n <= 1) return Math.max(0, Math.min(100, n * 100));
  return Math.max(0, Math.min(100, n));
}

function clamp0100(v) {
  const n = toNum(v, null);
  if (n == null) return null;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function makeId(prefix, ...parts) {
  const tokens = [prefix, ...parts]
    .map((x) => clampText(x, 80).replace(/[^\w.\-:]/g, "_"))
    .filter(Boolean);
  return tokens.join("|");
}

function parseAgentDecision(input) {
  if (isObj(input.agentDecision)) return input.agentDecision;
  if (isObj(input.output)) return input.output;
  return {};
}

function buildRunId(ctxRun, agentDecision, nowIso, executionId) {
  const c1 = clampText(ctxRun?.runId, 96);
  if (c1) return c1;
  const c2 = clampText(agentDecision?.decisionMeta?.run_id, 96);
  if (c2) return c2;
  const stampBase = clampText(ctxRun?.timestampParis, 64) || nowIso;
  const stamp = stampBase.replace(/[-:.TZ]/g, "").slice(0, 14);
  const exec = clampText(executionId, 32).replace(/[^\w.\-:]/g, "_") || "NOEXEC";
  return `RUN_${stamp}_${exec}`;
}

function getOrderActionMap(actions) {
  const out = new Map();
  for (const a of actions || []) {
    const s = normSymbol(a?.symbol);
    if (!s || out.has(s)) continue;
    out.set(s, String(a?.action ?? "").toUpperCase());
  }
  return out;
}

function mapIntent(action, side) {
  const a = String(action || "").toUpperCase();
  if (a === "OPEN") return "OPEN";
  if (a === "INCREASE") return "INCREASE";
  if (a === "DECREASE") return "REDUCE";
  if (a === "CLOSE") return "CLOSE";
  return side === "BUY" ? "OPEN" : "REDUCE";
}

function mapSignal(action) {
  const a = String(action || "").toUpperCase();
  if (a === "OPEN" || a === "INCREASE" || a === "BUY") return "BUY";
  if (a === "DECREASE" || a === "CLOSE" || a === "SELL") return "SELL";
  if (a === "WATCH") return "WATCH";
  if (a === "HOLD") return "HOLD";
  return "NEUTRAL";
}

function extractPrice(symbol, order, prices) {
  const pOrder = toNum(order?.price, null);
  if (pOrder != null && pOrder > 0) return { price: pOrder, source: "order.price" };
  const pCtx = toNum(prices?.[symbol]?.lastPrice, null);
  if (pCtx != null && pCtx > 0) return { price: pCtx, source: "market.lastPrice" };
  const pLimit = toNum(order?.limitPrice, null);
  if (pLimit != null && pLimit > 0) return { price: pLimit, source: "order.limitPrice" };
  return { price: null, source: "missing" };
}

function buildSymbolRegex() {
  return /\b[A-Z0-9]{1,10}(?:\.[A-Z]{1,4})?\b/g;
}

const input = $json || {};
const ctx = input.ctx || {};
const runCtx = ctx.run || {};
const market = ctx.market || {};
const prices = isObj(market.prices) ? market.prices : {};
const dataQuality = isObj(market.dataQuality) ? market.dataQuality : {};
const nowIso = new Date().toISOString();
const n8nExecutionId =
  typeof $execution !== "undefined" && $execution && $execution.id != null
    ? clampText($execution.id, 128)
    : null;

const agentDecision = parseAgentDecision(input);
const actions = Array.isArray(agentDecision.actions) ? agentDecision.actions : [];
const ordersIn = Array.isArray(input.orders) ? input.orders : [];
const warnings = Array.isArray(input.warnings) ? input.warnings.map((x) => String(x)) : [];
const hasSafetyVeto = warnings.some((w) => String(w).startsWith("SAFETY_VETO_"));
const decision = clampText(input.decision, 32) || "NO_TRADE";
const commentary = clampText(input.commentary, 600);

const run_id = buildRunId(runCtx, agentDecision, nowIso, n8nExecutionId);
const ts_start = clampText(runCtx.timestampParis, 64) || nowIso;
const ts_end = nowIso;

const config = isObj(ctx.config) ? ctx.config : {};
const feesConfig = isObj(ctx.feesConfig) ? ctx.feesConfig : {};
const fixedFee = Math.max(0, toNum(feesConfig.orderFeeFixedEUR, 0));
const pctFee = Math.max(0, toNum(feesConfig.orderFeePct, 0));

const strategyVersion =
  clampText(config.strategyVersion, 64) ||
  clampText(agentDecision?.decisionMeta?.strategy_version, 64) ||
  "strategy_v1";
const configVersion = clampText(config.configVersion, 64) || "config_v1";
const promptVersion =
  clampText(runCtx.promptVersion, 64) ||
  clampText(agentDecision?.decisionMeta?.prompt_version, 64) ||
  "prompt_v1";
const model =
  clampText(runCtx.model, 64) ||
  clampText(agentDecision?.decisionMeta?.model, 64) ||
  "gpt-5.2";

const coveragePct = normPctTo100(dataQuality?.pricesCoverageRequested);
const newsCount = Array.isArray(market.newsItems) ? market.newsItems.length : 0;
const aiCost = Math.max(0, toNum(input?.metrics?.aiCostEUR, 0));
const expectedFees = Math.max(0, toNum(input?.metrics?.expectedFeesEUR, 0));
const dataOkForTrading = !!(dataQuality?.okForTrading ?? !hasSafetyVeto);

const run = {
  run_id,
  ts_start,
  ts_end,
  tz: "Europe/Paris",
  strategy_version: strategyVersion,
  config_version: configVersion,
  prompt_version: promptVersion,
  model,
  n8n_execution_id: n8nExecutionId || null,
  decision_summary: `${decision} | orders=${ordersIn.length}`,
  data_ok_for_trading: dataOkForTrading,
  price_coverage_pct: coveragePct,
  news_count: newsCount,
  ai_cost_eur: aiCost,
  expected_fees_eur: expectedFees,
  warnings_json: warnings,
  agent_output_json: agentDecision,
  risk_gate_json: {
    ok_for_trading: dataOkForTrading,
    data_quality: dataQuality,
    decision,
  },
};

const actionBySymbol = getOrderActionMap(actions);

const orders = [];
const fills = [];
const cash_ledger = [];
const alerts = [];
const ai_signals = [];
const market_prices = [];

for (let i = 0; i < ordersIn.length; i++) {
  const o = ordersIn[i] || {};
  const symbol = normSymbol(o.symbol);
  const side = clampText(o.side, 8).toUpperCase();
  const qty = toNum(o.quantity, null);
  if (!symbol || !["BUY", "SELL"].includes(side) || qty == null || qty <= 0) continue;

  const action = actionBySymbol.get(symbol) || "";
  const order_id = makeId("ORD", run_id, String(i + 1).padStart(3, "0"), symbol, side);
  const order_type = clampText(o.orderType, 16).toUpperCase() || "MARKET";

  const baseOrder = {
    order_id,
    run_id,
    ts_created: ts_end,
    symbol,
    side,
    intent: mapIntent(action, side),
    order_type,
    qty,
    limit_price: toNum(o.limitPrice, null),
    stop_price: toNum(o.stopPrice, null),
    tif: clampText(o.timeInForce, 8) || "DAY",
    status: "PLANNED",
    broker: clampText(o.broker, 24) || "SIM",
    reason: clampText(o.reason, 160) || null,
    rationale_json: {
      confidence: clamp0100(o.confidence),
      priority: toInt(o.priority, null),
      stop_loss_pct: toNum(o.stopLossPct, null),
      rationale: clampText(o.rationale, 600),
      action,
    },
  };

  const px = extractPrice(symbol, o, prices);
  if (px.price == null || px.price <= 0) {
    baseOrder.status = "SKIPPED";
    baseOrder.reason = baseOrder.reason || "NO_EXEC_PRICE";
    orders.push(baseOrder);
    alerts.push({
      alert_id: makeId("ALT", run_id, "NO_PRICE", symbol, String(i + 1)),
      run_id,
      ts: ts_end,
      severity: "WARN",
      category: "EXECUTION",
      symbol,
      message: `Order skipped: no execution price for ${symbol}`,
      code: "ORDER_SKIPPED_NO_PRICE",
      payload_json: { order_id },
    });
    continue;
  }

  baseOrder.status = "FILLED";
  orders.push(baseOrder);

  const fill_id = makeId("FIL", order_id, "1");
  const notional = qty * px.price;
  const fees_eur = fixedFee + pctFee * notional;

  fills.push({
    fill_id,
    order_id,
    run_id,
    ts_fill: ts_end,
    qty,
    price: px.price,
    fees_eur,
    slippage_bps: 0,
    liquidity: "UNKNOWN",
    raw_fill_json: {
      mode: "SIM",
      price_source: px.source,
      decision,
    },
  });

  cash_ledger.push({
    cash_tx_id: makeId("TX", run_id, fill_id, "NOTIONAL"),
    run_id,
    ts: ts_end,
    currency: "EUR",
    amount: side === "BUY" ? -notional : notional,
    type: "TRADE_NOTIONAL",
    symbol,
    ref_id: fill_id,
    notes: `${side} ${qty} ${symbol} @ ${px.price}`,
    payload_json: { order_id, fill_id },
  });

  if (fees_eur > 0) {
    cash_ledger.push({
      cash_tx_id: makeId("TX", run_id, fill_id, "FEE"),
      run_id,
      ts: ts_end,
      currency: "EUR",
      amount: -fees_eur,
      type: "FEE",
      symbol,
      ref_id: fill_id,
      notes: `Execution fee ${symbol}`,
      payload_json: { order_id, fill_id },
    });
  }
}

if (aiCost > 0) {
  cash_ledger.push({
    cash_tx_id: makeId("TX", run_id, "AI_COST"),
    run_id,
    ts: ts_end,
    currency: "EUR",
    amount: -aiCost,
    type: "AI_COST",
    symbol: null,
    ref_id: run_id,
    notes: "LLM cost for run",
    payload_json: { model, promptVersion },
  });
}

for (let i = 0; i < actions.length; i++) {
  const a = actions[i] || {};
  const symbol = normSymbol(a.symbol);
  if (!symbol) continue;
  const entryPlan = isObj(a.entryPlan) ? a.entryPlan : {};
  const riskPlan = isObj(a.riskPlan) ? a.riskPlan : {};
  const confidence = clamp0100(a.confidence);
  ai_signals.push({
    signal_id: makeId("SIG", run_id, String(i + 1).padStart(3, "0"), symbol),
    run_id,
    ts: ts_end,
    symbol,
    signal: mapSignal(a.action),
    confidence,
    horizon: clampText(a.horizon || "", 24) || (toInt(a.horizonDays, null) ? `D${toInt(a.horizonDays, 0)}` : null),
    entry_zone: clampText(entryPlan.entryZone || "", 120) || null,
    stop_loss: toNum(entryPlan.stopLoss ?? riskPlan.stopLoss ?? riskPlan.stopLossPct, null),
    take_profit: toNum(entryPlan.takeProfit ?? riskPlan.takeProfit ?? riskPlan.takeProfitPct, null),
    risk_score: confidence == null ? null : 100 - confidence,
    catalyst: clampText(a.catalyst || "", 240) || null,
    rationale: clampText(a.rationale || "", 800) || null,
    payload_json: a,
  });
}

for (const [sym, p] of Object.entries(prices)) {
  const symbol = normSymbol(sym);
  if (!symbol) continue;
  market_prices.push({
    ts: ts_end,
    symbol,
    open: toNum(p?.open, null),
    high: toNum(p?.high, null),
    low: toNum(p?.low, null),
    close: toNum(p?.lastPrice ?? p?.close, null),
    adj_close: toNum(p?.adjClose, null),
    volume: toInt(p?.volume, null),
    source: clampText(p?.source, 32) || "YF",
    asof: clampText(p?.ts, 64) || ts_end,
  });
}

const symbolRegex = buildSymbolRegex();
if (!dataOkForTrading) {
  alerts.push({
    alert_id: makeId("ALT", run_id, "DATA_GATE"),
    run_id,
    ts: ts_end,
    severity: "CRITICAL",
    category: "DATA",
    symbol: "GLOBAL",
    message: "Market data safety gate failed",
    code: "DATA_GATE_FAIL",
    payload_json: dataQuality,
  });
}

for (let i = 0; i < warnings.length; i++) {
  const w = warnings[i];
  const m = w.match(symbolRegex);
  alerts.push({
    alert_id: makeId("ALT", run_id, "WARN", String(i + 1).padStart(3, "0")),
    run_id,
    ts: ts_end,
    severity: "WARN",
    category: "AGENT",
    symbol: (m && m[0]) || "GLOBAL",
    message: clampText(w, 2048),
    code: "AGENT_WARNING",
    payload_json: { warning: w },
  });
}

const backfill_queue = [];
const backfills = Array.isArray(agentDecision.backfillRequests) ? agentDecision.backfillRequests : [];
for (const bf of backfills) {
  const symbol = normSymbol(bf?.symbol);
  const needsArr = Array.isArray(bf?.needs) ? uniq(bf.needs.map((x) => clampText(x, 24)).filter(Boolean)) : [];
  if (!symbol || !needsArr.length) continue;
  const needsCsv = needsArr.join(",");
  backfill_queue.push({
    request_id: makeId("BF", symbol, needsCsv),
    run_id,
    ts: ts_end,
    symbol,
    needs: needsCsv,
    severity: "MEDIUM",
    status: "OPEN",
    why: clampText(bf?.why, 700) || null,
    completed_at: null,
    response_json: null,
    notes: null,
  });
}

const positions = Array.isArray(ctx?.portfolioSummary?.positions) ? ctx.portfolioSummary.positions : [];
const symbolsFromPositions = positions
  .map((p) => normSymbol(p.Symbol ?? p.symbol))
  .filter((s) => s && s !== "CASH_EUR" && s !== "__META__");
const symbolsAll = uniq([
  ...symbolsFromPositions,
  ...orders.map((o) => o.symbol),
  ...ai_signals.map((s) => s.symbol),
  ...market_prices.map((m) => m.symbol),
  ...backfill_queue.map((b) => b.symbol),
]);

const instruments = symbolsAll.map((symbol) => {
  const p = positions.find((x) => normSymbol(x.Symbol ?? x.symbol) === symbol) || {};
  return {
    symbol,
    name: clampText(p.Name ?? p.name, 240) || null,
    asset_class: clampText(p.AssetClass ?? p.assetClass, 64) || "Equity",
    currency: "EUR",
    isin: clampText(p.ISIN ?? p.isin, 64) || null,
    sector: clampText(p.Sector ?? p.sector, 120) || null,
    industry: clampText(p.Industry ?? p.industry, 120) || null,
  };
});

const bundle = {
  run,
  instruments,
  orders,
  fills,
  cash_ledger,
  ai_signals,
  market_prices,
  alerts,
  backfill_queue,
  snapshots: {},
};

const db_path =
  clampText(input?.db_path, 240) ||
  clampText(ctx?.db_path, 240) ||
  clampText(config?.ag1DbPath, 240) ||
  "/files/duckdb/ag1_v2.duckdb";

return [
  {
    json: {
      run_id,
      db_path,
      bundle,
      summary: {
        decision,
        commentary,
        orders: orders.length,
        fills: fills.length,
        cash_ledger: cash_ledger.length,
        alerts: alerts.length,
        backfill: backfill_queue.length,
        ai_signals: ai_signals.length,
      },
    },
  },
];

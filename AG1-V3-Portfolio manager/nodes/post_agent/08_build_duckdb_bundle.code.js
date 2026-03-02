// 8 - Build DuckDB Bundle (Portfolio Ledger v2, FX-aware)
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

function normAssetClass(v, symbol) {
  const raw = String(v ?? "").trim().toUpperCase();
  if (raw === "FX" || raw === "FOREX") return "FX";
  if (raw === "CRYPTO") return "CRYPTO";
  if (raw === "EQUITY" || raw === "ETF" || raw === "STOCK") return "EQUITY";
  const s = String(symbol ?? "").trim().toUpperCase();
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
  return {
    symbolInternal: `FX:${pair6}`,
    symbolYahoo: `${pair6}=X`,
    pair6,
    base: pair6.slice(0, 3),
    quote: pair6.slice(3, 6),
  };
}

function getPriceFromMap(prices, symbol) {
  const s = normSymbol(symbol);
  if (!s) return { price: null, symbol: null };
  const candidates = [s];
  if (s.startsWith("FX:") || s.endsWith("=X")) {
    const fx = parseFxMeta(s);
    if (fx) candidates.push(fx.symbolInternal, fx.symbolYahoo, fx.pair6);
  } else if (/^[A-Z]{6}$/.test(s)) {
    candidates.push(`FX:${s}`, `${s}=X`);
  }
  for (const c of uniq(candidates.map((x) => normSymbol(x)).filter(Boolean))) {
    const px = toNum(prices?.[c]?.lastPrice ?? prices?.[c]?.close, null);
    if (px != null && px > 0) return { price: px, symbol: c };
  }
  return { price: null, symbol: null };
}

function resolveQuoteToEur(quoteCcy, prices, fallbackRate = null) {
  const q = String(quoteCcy ?? "").trim().toUpperCase();
  if (!q) return { ok: false, quote_to_eur: null, source_symbol: null };
  if (q === "EUR") return { ok: true, quote_to_eur: 1, source_symbol: "EUR" };
  if (toNum(fallbackRate, null) != null && toNum(fallbackRate, null) > 0) {
    return { ok: true, quote_to_eur: toNum(fallbackRate, null), source_symbol: "ORDER_HINT" };
  }

  for (const c of [`FX:EUR${q}`, `EUR${q}=X`]) {
    const px = getPriceFromMap(prices, c).price;
    if (px != null && px > 0) return { ok: true, quote_to_eur: 1 / px, source_symbol: c };
  }
  for (const c of [`FX:${q}EUR`, `${q}EUR=X`]) {
    const px = getPriceFromMap(prices, c).price;
    if (px != null && px > 0) return { ok: true, quote_to_eur: px, source_symbol: c };
  }
  return { ok: false, quote_to_eur: null, source_symbol: null };
}

function getOrderActionMap(actions) {
  const out = new Map();
  for (const a of actions || []) {
    const ac = normAssetClass(a?.assetClass ?? a?.asset_class, a?.symbol_internal || a?.symbol);
    const s = ac === "FX"
      ? fxSymbolInternal(a?.symbol_internal || a?.symbol || "")
      : normSymbol(a?.symbol);
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
  if (a === "PROPOSE_OPEN") return "PROPOSE_OPEN";
  if (a === "PROPOSE_CLOSE") return "PROPOSE_CLOSE";
  if (a === "WATCH") return "WATCH";
  if (a === "HOLD") return "HOLD";
  return "NEUTRAL";
}

function extractPrice(symbol, order, prices) {
  const pOrder = toNum(order?.price, null);
  if (pOrder != null && pOrder > 0) return { price: pOrder, source: "order.price" };
  const pCtx = toNum(getPriceFromMap(prices, symbol).price, null);
  if (pCtx != null && pCtx > 0) return { price: pCtx, source: "market.lastPrice" };
  const pLimit = toNum(order?.limitPrice, null);
  if (pLimit != null && pLimit > 0) return { price: pLimit, source: "order.limitPrice" };
  return { price: null, source: "missing" };
}

function buildSymbolRegex() {
  return /\b[A-Z0-9]{1,10}(?:\.[A-Z]{1,4})?\b/g;
}

function buildInitialFxState(portfolioSummary, prices, leverageDefault) {
  const out = new Map();
  const positions = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
  const lev = Math.max(1, toNum(leverageDefault, 10));

  for (const p of positions) {
    const sRaw = normSymbol(p.Symbol ?? p.symbol);
    const ac = normAssetClass(p.AssetClass ?? p.assetClass ?? p.asset_class, sRaw);
    if (ac !== "FX") continue;

    const meta = parseFxMeta(sRaw);
    if (!meta) continue;
    const qty = Math.max(0, toNum(p.Quantity, 0));
    if (qty <= 0) continue;

    const avg = toNum(p.AvgPrice ?? p.avg_price ?? p.LastPrice ?? p.last_price, null);
    if (!(avg > 0)) continue;

    const conv = resolveQuoteToEur(meta.quote, prices, null);
    const marketValue = toNum(p.MarketValue ?? p.market_value, null);
    const unrealized = toNum(p.UnrealizedPnL ?? p.unrealized_pnl, null);
    let margin = toNum(p.MarginUsedEUR ?? p.margin_used_eur ?? p.marginUsedEUR, null);
    if (!(margin > 0) && marketValue != null && unrealized != null) {
      const implied = marketValue - unrealized;
      if (implied > 0) margin = implied;
    }
    if (!(margin > 0)) {
      const convRate = conv.ok ? conv.quote_to_eur : 1;
      margin = (qty * avg * convRate) / lev;
    }

    out.set(meta.symbolInternal, {
      qty_base: qty,
      avg_entry_price: avg,
      margin_used_eur: Math.max(0, margin),
      quote_to_eur: conv.ok ? conv.quote_to_eur : null,
      last_price: avg,
      pair6: meta.pair6,
      base_ccy: meta.base,
      quote_ccy: meta.quote,
    });
  }
  return out;
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
const fxLeverageDefault = Math.max(1, toNum(config.fx_leverage_default, 10));
const fxFeeBpsDefault = Math.max(0, toNum(config.fx_fee_bps ?? feesConfig.fx_fee_bps, 0));
const fxAllowShort = toBool(config.fx_allow_short, false);

const strategyVersion =
  clampText(config.strategyVersion, 64) ||
  clampText(agentDecision?.decisionMeta?.strategy_version, 64) ||
  "strategy_v3";
const configVersion = clampText(config.configVersion, 64) || "config_v3";
const promptVersion =
  clampText(runCtx.promptVersion, 64) ||
  clampText(agentDecision?.decisionMeta?.prompt_version, 64) ||
  "prompt_v3";
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
    input_snapshot: runCtx.inputSnapshot || null,
    universe_scope: runCtx.universe_scope || ["EQUITY", "CRYPTO"],
    enable_fx: runCtx.enable_fx === true,
  },
};

const actionBySymbol = getOrderActionMap(actions);

const orders = [];
const fills = [];
const cash_ledger = [];
const alerts = [];
const ai_signals = [];
const market_prices = [];
const fxState = buildInitialFxState(ctx?.portfolioSummary || {}, prices, fxLeverageDefault);

for (let i = 0; i < ordersIn.length; i++) {
  const o = ordersIn[i] || {};
  const symbolRaw = normSymbol(o.symbol);
  const actionSym = normSymbol(o.symbol_internal || o.symbol);
  const inferredAsset = normAssetClass(o.assetClass ?? o.asset_class, symbolRaw || actionSym);
  const isFx = inferredAsset === "FX";
  const symbol = isFx ? fxSymbolInternal(symbolRaw || actionSym) : symbolRaw;
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
      asset_class: isFx ? "FX" : "EQUITY",
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

  if (!isFx) {
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
    continue;
  }

  const fxMeta = parseFxMeta(symbol);
  if (!fxMeta) {
    baseOrder.status = "SKIPPED";
    baseOrder.reason = baseOrder.reason || "INVALID_FX_SYMBOL";
    orders.push(baseOrder);
    continue;
  }

  const conv = resolveQuoteToEur(
    fxMeta.quote,
    prices,
    toNum(o.quote_to_eur, null)
  );
  if (!conv.ok || !(conv.quote_to_eur > 0)) {
    baseOrder.status = "SKIPPED";
    baseOrder.reason = baseOrder.reason || "NO_FX_CONVERSION";
    orders.push(baseOrder);
    alerts.push({
      alert_id: makeId("ALT", run_id, "FX_CONV", symbol, String(i + 1)),
      run_id,
      ts: ts_end,
      severity: "WARN",
      category: "EXECUTION",
      symbol,
      message: `FX order skipped: missing quote->EUR conversion for ${symbol}`,
      code: "ORDER_SKIPPED_NO_FX_CONVERSION",
      payload_json: { order_id, quote_ccy: fxMeta.quote },
    });
    continue;
  }

  const leverage = Math.max(1, toNum(o.fx_leverage ?? o.leverage, fxLeverageDefault));
  const notional_eur = Math.max(0, toNum(o.notional_eur, Math.abs(qty * px.price * conv.quote_to_eur)));
  const margin_used_eur = Math.max(0, toNum(o.margin_used_eur, notional_eur / leverage));
  const fee_bps = Math.max(0, toNum(o.fx_fee_bps, fxFeeBpsDefault));
  const fees_eur = notional_eur * (fee_bps / 10000);

  const state = fxState.get(symbol) || {
    qty_base: 0,
    avg_entry_price: px.price,
    margin_used_eur: 0,
    quote_to_eur: conv.quote_to_eur,
    last_price: px.price,
    pair6: fxMeta.pair6,
    base_ccy: fxMeta.base,
    quote_ccy: fxMeta.quote,
  };

  const beforeQty = toNum(state.qty_base, 0);
  const beforeAvg = toNum(state.avg_entry_price, px.price);
  const beforeMargin = Math.max(0, toNum(state.margin_used_eur, 0));
  let marginPosted = 0;
  let marginReleased = 0;
  let realizedPnlEur = 0;
  let qtySigned = 0;
  let execQty = qty;

  if (side === "BUY") {
    marginPosted = margin_used_eur;
    const newQty = beforeQty + qty;
    const newAvg = newQty > 0 ? ((beforeQty * beforeAvg) + (qty * px.price)) / newQty : px.price;
    state.qty_base = newQty;
    state.avg_entry_price = newAvg;
    state.margin_used_eur = beforeMargin + marginPosted;
    qtySigned = qty;
  } else {
    const closeQty = Math.min(qty, Math.max(0, beforeQty));
    if (closeQty <= 0) {
      baseOrder.status = "SKIPPED";
      baseOrder.reason = baseOrder.reason || (fxAllowShort ? "SHORT_OPEN_NOT_IMPLEMENTED" : "NO_OPEN_FX_POSITION");
      orders.push(baseOrder);
      continue;
    }
    execQty = closeQty;
    qtySigned = -closeQty;

    const pnlQuote = closeQty * (px.price - beforeAvg);
    realizedPnlEur = pnlQuote * conv.quote_to_eur;
    marginReleased = beforeQty > 0 ? (beforeMargin * (closeQty / beforeQty)) : 0;
    state.qty_base = Math.max(0, beforeQty - closeQty);
    state.margin_used_eur = Math.max(0, beforeMargin - marginReleased);
    if (state.qty_base <= 1e-12) {
      state.qty_base = 0;
      state.avg_entry_price = 0;
    }
  }

  state.last_price = px.price;
  state.quote_to_eur = conv.quote_to_eur;
  fxState.set(symbol, state);

  baseOrder.status = "FILLED";
  orders.push(baseOrder);

  const fill_id = makeId("FIL", order_id, "1");
  fills.push({
    fill_id,
    order_id,
    run_id,
    ts_fill: ts_end,
    qty: execQty,
    price: px.price,
    fees_eur,
    slippage_bps: 0,
    liquidity: "UNKNOWN",
    raw_fill_json: {
      mode: "SIM",
      decision,
      price_source: px.source,
      asset_class: "FX",
      pair6: fxMeta.pair6,
      base_ccy: fxMeta.base,
      quote_ccy: fxMeta.quote,
      quote_to_eur: conv.quote_to_eur,
      conversion_symbol: conv.source_symbol,
      notional_eur,
      margin_used_eur,
      margin_posted_eur: marginPosted,
      margin_released_eur: marginReleased,
      leverage,
      fee_bps,
      realized_pnl_eur: realizedPnlEur,
      qty_base_signed: qtySigned,
      entry_price: beforeAvg,
      exit_price: px.price,
    },
  });

  if (marginPosted > 0) {
    cash_ledger.push({
      cash_tx_id: makeId("TX", run_id, fill_id, "MARGIN_POST"),
      run_id,
      ts: ts_end,
      currency: "EUR",
      amount: -marginPosted,
      type: "MARGIN_POST",
      symbol,
      ref_id: fill_id,
      notes: `FX margin posted ${symbol}`,
      payload_json: { order_id, fill_id, pair6: fxMeta.pair6, leverage },
    });
  }

  if (marginReleased > 0) {
    cash_ledger.push({
      cash_tx_id: makeId("TX", run_id, fill_id, "MARGIN_RELEASE"),
      run_id,
      ts: ts_end,
      currency: "EUR",
      amount: marginReleased,
      type: "MARGIN_RELEASE",
      symbol,
      ref_id: fill_id,
      notes: `FX margin released ${symbol}`,
      payload_json: { order_id, fill_id, pair6: fxMeta.pair6, leverage },
    });
  }

  if (Math.abs(realizedPnlEur) > 1e-9) {
    cash_ledger.push({
      cash_tx_id: makeId("TX", run_id, fill_id, "PNL_REALIZED_FX"),
      run_id,
      ts: ts_end,
      currency: "EUR",
      amount: realizedPnlEur,
      type: "PNL_REALIZED_FX",
      symbol,
      ref_id: fill_id,
      notes: `FX realized PnL ${symbol}`,
      payload_json: { order_id, fill_id, pair6: fxMeta.pair6 },
    });
  }

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
      notes: `FX execution fee ${symbol}`,
      payload_json: { order_id, fill_id, fee_bps },
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
  const isFx = normAssetClass(a.assetClass ?? a.asset_class, a.symbol_internal || a.symbol) === "FX";
  const symbol = normSymbol((isFx ? (a.symbol_internal || a.symbol) : a.symbol));
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
  .map((p) => {
    const ac = normAssetClass(p.AssetClass ?? p.assetClass ?? p.asset_class, p.Symbol ?? p.symbol);
    const s = normSymbol(p.Symbol ?? p.symbol);
    return ac === "FX" ? fxSymbolInternal(s) : s;
  })
  .filter((s) => s && s !== "CASH_EUR" && s !== "__META__");

const symbolsAll = uniq([
  ...symbolsFromPositions,
  ...orders.map((o) => o.symbol),
  ...ai_signals.map((s) => s.symbol),
  ...market_prices.map((m) => m.symbol),
  ...backfill_queue.map((b) => b.symbol),
]);

const instruments = symbolsAll.map((symbol) => {
  const p = positions.find((x) => {
    const ac = normAssetClass(x.AssetClass ?? x.assetClass ?? x.asset_class, x.Symbol ?? x.symbol);
    const sx = ac === "FX" ? fxSymbolInternal(x.Symbol ?? x.symbol) : normSymbol(x.Symbol ?? x.symbol);
    return sx === symbol;
  }) || {};
  const inferredFx = String(symbol || "").toUpperCase().startsWith("FX:");
  return {
    symbol,
    name: clampText(p.Name ?? p.name, 240) || null,
    asset_class: (clampText(p.AssetClass ?? p.assetClass, 64) || (inferredFx ? "FX" : "EQUITY")).toUpperCase(),
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
  "/files/duckdb/ag1_v3.duckdb";

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

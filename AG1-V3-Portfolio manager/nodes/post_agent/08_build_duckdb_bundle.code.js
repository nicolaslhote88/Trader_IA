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

function roundTo(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function normalizeSymbol(v) {
  return String(v ?? "").trim();
}

function inferRiskStatus(cashPct, cashEUR) {
  if (Number.isFinite(cashEUR) && cashEUR < -0.01) return "RISK_OFF";
  if (Number.isFinite(cashPct) && cashPct >= 0.8) return "DEFENSIVE";
  if (Number.isFinite(cashPct) && cashPct <= 0.1) return "RISK_ON";
  return "BALANCED";
}

function buildSnapshotsFromPortfolio(portfolioSummary, orders, priceMap, ts, meta) {
  const positionsIn = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
  const positionsValueInput = positionsIn.reduce((sum, row) => {
    const marketValue = toNum(row?.MarketValue, null);
    if (marketValue !== null) return sum + marketValue;
    return sum + (toNum(row?.Quantity, 0) * toNum(row?.LastPrice, 0));
  }, 0);
  const summaryCash = toNum(portfolioSummary?.cashEUR, null);
  const summaryTotal = toNum(portfolioSummary?.totalPortfolioValueEUR, null);
  let startCash = summaryCash;
  if (
    summaryTotal !== null
    && Number.isFinite(positionsValueInput)
    && Math.abs(((summaryCash ?? 0) + positionsValueInput) - summaryTotal) > 0.01
  ) {
    const reconciledCash = summaryTotal - positionsValueInput;
    if (Number.isFinite(reconciledCash)) startCash = reconciledCash;
  }
  if (!Number.isFinite(startCash)) startCash = 0;
  const posMap = new Map();

  for (const row of positionsIn) {
    const symbol = normalizeSymbol(row?.Symbol);
    const qty = toNum(row?.Quantity, 0);
    if (!symbol || qty <= 0) continue;
    posMap.set(symbol, {
      symbol,
      qty,
      avgCost: toNum(row?.AvgPrice, toNum(row?.LastPrice, 0) || 0),
      lastPrice: toNum(row?.LastPrice, toNum(row?.AvgPrice, 0) || 0),
      assetClass: String(row?.AssetClass || "EQUITY").trim().toUpperCase() || "EQUITY",
      sector: clampText(row?.Sector || "UNKNOWN", 128) || "UNKNOWN",
    });
  }

  let cashEUR = startCash;
  for (const order of orders || []) {
    const symbol = normalizeSymbol(order?.symbol);
    const side = String(order?.side || "").trim().toUpperCase();
    const qty = toNum(order?.quantity, 0);
    const orderType = normalizeOrderType(order?.orderType);
    const price =
      (orderType === "LIMIT" ? toNum(order?.limitPrice, null) : null) ??
      toNum(priceMap[symbol], null);

    if (!symbol || qty <= 0 || !Number.isFinite(price) || price <= 0) continue;

    if (side === "BUY") {
      const current = posMap.get(symbol) || {
        symbol,
        qty: 0,
        avgCost: price,
        lastPrice: price,
        assetClass: String(order?.assetClass || "EQUITY").trim().toUpperCase() || "EQUITY",
        sector: "UNKNOWN",
      };
      const newQty = current.qty + qty;
      current.avgCost = newQty > 0 ? (((current.qty * current.avgCost) + (qty * price)) / newQty) : price;
      current.qty = newQty;
      current.lastPrice = toNum(priceMap[symbol], price);
      posMap.set(symbol, current);
      cashEUR -= qty * price;
      continue;
    }

    if (side === "SELL") {
      const current = posMap.get(symbol);
      if (!current) continue;
      const execQty = Math.min(qty, current.qty);
      if (execQty <= 0) continue;
      current.qty -= execQty;
      current.lastPrice = toNum(priceMap[symbol], price);
      cashEUR += execQty * price;
      if (current.qty <= 1e-9) posMap.delete(symbol);
      else posMap.set(symbol, current);
    }
  }

  const positions = [];
  const sectorTotals = {};
  let equityEUR = 0;

  for (const current of posMap.values()) {
    const lastPrice = toNum(priceMap[current.symbol], current.lastPrice ?? current.avgCost ?? 0);
    const marketValue = current.qty * lastPrice;
    const unrealizedPnL = (lastPrice - current.avgCost) * current.qty;
    equityEUR += marketValue;
    const sector = current.sector || "UNKNOWN";
    sectorTotals[sector] = (sectorTotals[sector] || 0) + marketValue;
    positions.push({
      symbol: current.symbol,
      ts,
      qty: current.qty,
      avg_cost: roundTo(current.avgCost, 8),
      last_price: roundTo(lastPrice, 8),
      market_value_eur: roundTo(marketValue, 2),
      unrealized_pnl_eur: roundTo(unrealizedPnL, 2),
      weight_pct: 0,
    });
  }

  const totalValueEUR = cashEUR + equityEUR;
  for (const position of positions) {
    position.weight_pct = totalValueEUR > 0 ? position.market_value_eur / totalValueEUR : 0;
  }

  const initialCapitalEUR = toNum(meta?.initialCapitalEUR, 50000);
  const cumFeesEUR = toNum(meta?.cumFeesEUR, 0);
  const cumAiCostEUR = toNum(meta?.cumAiCostEUR, 0);
  const totalPnLEUR = totalValueEUR - initialCapitalEUR;
  const roi = initialCapitalEUR > 0 ? (totalPnLEUR / initialCapitalEUR) : 0;
  const cashPct = totalValueEUR > 0 ? (cashEUR / totalValueEUR) : 0;
  const top1PosPct = positions.length
    ? Math.max(...positions.map((p) => Number(p.market_value_eur) || 0)) / (totalValueEUR || 1)
    : 0;
  const top1SectorPct = Object.keys(sectorTotals).length
    ? Math.max(...Object.values(sectorTotals)) / (totalValueEUR || 1)
    : 0;
  const riskStatus = inferRiskStatus(cashPct, cashEUR);

  return {
    positions,
    portfolio: {
      ts,
      cash_eur: roundTo(cashEUR, 2),
      equity_eur: roundTo(equityEUR, 2),
      total_value_eur: roundTo(totalValueEUR, 2),
      cum_fees_eur: roundTo(cumFeesEUR, 2),
      cum_ai_cost_eur: roundTo(cumAiCostEUR, 2),
      trades_this_run: orders.length,
      total_pnl_eur: roundTo(totalPnLEUR, 2),
      roi,
      drawdown_pct: 0,
      meta_json: {
        source: "node8_portfolio_summary",
        start_cash_eur: roundTo(startCash, 2),
      },
    },
    risk: {
      ts,
      cash_pct: cashPct,
      top1_pos_pct: top1PosPct,
      top1_sector_pct: top1SectorPct,
      var95_est_eur: roundTo(equityEUR * 0.015 * 1.65, 2),
      positions_count: positions.length,
      risk_status: riskStatus,
      limits_json: {
        source: "node8_portfolio_summary",
      },
    },
  };
}

const input = $json || {};
const ctx = input.ctx || {};
const runCtx = ctx.run || {};
const meta = input.meta || ctx.meta || {};
const agentDecision = input.agentDecision || {};
const ordersIn = Array.isArray(input.orders) ? input.orders : [];
const warnings = Array.isArray(input.warnings) ? input.warnings : [];
const ts_end = new Date().toISOString();
const run_id = runCtx.runId || `RUN_${Date.now()}`;
const db_path = normalizeDbPath(input.db_path || runCtx.db_path || "");

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

const snapshots = buildSnapshotsFromPortfolio(portfolioSummary, ordersIn, priceMap, ts_end, meta);

const bundle = {
  run: {
    run_id,
    ts_start: runCtx.timestampParis || ts_end,
    ts_end,
    tz: "Europe/Paris",
    model: runCtx.model || "UNKNOWN",
    db_path: db_path || null,
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
      price: (Number.isFinite(px) && px > 0) ? px : 1.0,
    };
  }),
  cash_ledger: [],
  market_prices: Object.entries(priceMap).map(([sym, px]) => ({ symbol: sym, close: px })),
  ai_signals,
  alerts,
  snapshots,
};

return [{
  json: {
    run_id,
    db_path,
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

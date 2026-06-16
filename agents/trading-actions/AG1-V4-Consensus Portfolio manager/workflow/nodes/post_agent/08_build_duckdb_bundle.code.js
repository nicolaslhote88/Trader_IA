function normalizeDbPath(v) {
  const s = String(v ?? "").trim().replace(/\\/g, "/");
  return s.replace("/local-files/", "/files/");
}

function toNum(v, dflt = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
}

function pick(row, ...keys) {
  for (const key of keys) {
    const v = row?.[key];
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return null;
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

function orderLedgerId(order, runId, index) {
  return String(order?.orderId || order?.order_id || order?.clientOrderId || `ORD_${runId}_${index}`).trim();
}

function brokerOrderId(order) {
  if (order?.brokerOrderId) return order.brokerOrderId;
  const raw = order?.ibkrResponse?.ibkr_response || order?.ibkrResponse?.details || order?.ibkrResponse;
  if (Array.isArray(raw) && raw.length > 0) {
    return raw[0]?.order_id || raw[0]?.orderId || raw[0]?.id || null;
  }
  return raw?.order_id || raw?.orderId || raw?.id || null;
}

function orderStatus(order) {
  const ibkr = String(order?.ibkrStatus || "").toLowerCase();
  if (ibkr === "submitted" || ibkr === "submitted_after_confirmation") return "SUBMITTED";
  if (ibkr === "filled" || ibkr === "executed") return "FILLED";
  if (ibkr === "dry_run") return "PLANNED";
  if (ibkr === "error" || ibkr === "not_sent") return "REJECTED";
  return "PLANNED";
}

function orderHasFillLikeEffect(order) {
  const ibkr = String(order?.ibkrStatus || "").toLowerCase();
  const broker = String(order?.broker || "").toUpperCase();
  const status = String(order?.status || order?.orderStatus || order?.executionStatus || "").toUpperCase();
  const explicitFill = Boolean(order?.ibkrFill || order?.fill || order?.execution || order?.brokerExecutionId);
  if (ibkr === "filled" || ibkr === "executed") return true;
  if (status === "FILLED" || status === "EXECUTED") return true;
  if (broker === "SIM" && ["SIM_FILLED", "SIMULATED_FILLED", "DRY_RUN_FILLED"].includes(status)) return true;
  return explicitFill && !["REJECTED", "BROKER_ERROR", "ERROR", "SUBMITTED", "PENDING", "PLANNED"].includes(status);
}

function rawFill(order) {
  return order?.ibkrFill || order?.fill || order?.execution || order?.ibkrExecution || {};
}

function pickWithKey(row, ...keys) {
  for (const key of keys) {
    const value = row?.[key];
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return { key, value };
    }
  }
  return { key: "", value: null };
}

function extractFillTimestamp(order, fallbackTs) {
  const raw = rawFill(order);
  const picked = pickWithKey(
    raw,
    "filled_at",
    "filledAt",
    "trade_time",
    "tradeTime",
    "execution_time",
    "executionTime",
    "time",
    "timestamp"
  ).value;
  if (!picked) return fallbackTs;
  const dt = new Date(picked);
  return Number.isNaN(dt.getTime()) ? fallbackTs : dt.toISOString();
}

function extractBrokerExecutionId(order) {
  const raw = rawFill(order);
  return clampText(
    pickWithKey(
      raw,
      "execution_id",
      "executionId",
      "execId",
      "trade_id",
      "tradeId",
      "id"
    ).value || order?.brokerExecutionId || order?.brokerOrderId || order?.clientOrderId || "",
    128
  ) || null;
}

function extractFillPrice(order, priceMap) {
  const raw = rawFill(order);
  const rawPrice = toNum(
    pickWithKey(raw, "price", "avgPrice", "avg_price", "fill_price", "fillPrice", "execution_price", "executionPrice").value,
    null
  );
  if (rawPrice !== null && rawPrice > 0) return rawPrice;
  const symbol = String(order?.symbol || "").trim();
  const orderType = normalizeOrderType(order?.orderType);
  if (orderType === "LIMIT") {
    const limitPrice = toNum(order?.limitPrice, null);
    if (limitPrice !== null && limitPrice > 0) return limitPrice;
  }
  const mappedPrice = toNum(priceMap[symbol], null);
  return (mappedPrice !== null && mappedPrice > 0) ? mappedPrice : 1.0;
}

function extractFillQty(order) {
  const raw = rawFill(order);
  const rawQty = toNum(
    pickWithKey(raw, "quantity", "qty", "shares", "size", "filledQuantity", "filled_quantity", "fill_size", "fillSize").value,
    null
  );
  if (rawQty !== null && rawQty > 0) return rawQty;
  return toNum(order?.quantity, 0) || 0;
}

function extractCommission(order) {
  const raw = rawFill(order);
  const picked = pickWithKey(
    raw,
    "commission",
    "ibCommission",
    "ib_commission",
    "commission_amount",
    "commissionAmount",
    "fees_eur",
    "fee"
  );
  const rawAmount = toNum(picked.value, null);
  const rawCcy = clampText(
    pickWithKey(
      raw,
      "commissionCurrency",
      "commission_currency",
      "commissionCcy",
      "ibCommissionCurrency",
      "feeCurrency",
      "currency"
    ).value || "",
    8
  ).toUpperCase();

  if (rawAmount !== null) {
    const sourceBase = picked.key ? `ibkr_${picked.key}` : "ibkr_commission";
    const source = rawCcy && rawCcy !== "EUR"
      ? `${sourceBase}_reported_${rawCcy}_assumed_eur`
      : (rawCcy ? sourceBase : `${sourceBase}_fallback_eur_no_ccy`);
    return {
      commissionAmount: Math.abs(rawAmount),
      commissionCcy: rawCcy || "EUR",
      commissionEUR: Math.abs(rawAmount),
      commissionSource: source,
    };
  }

  const expected = toNum(order?.actualFeesEUR ?? order?.expectedFeesEUR ?? order?.feesEUR ?? order?.fees_eur, 0) || 0;
  const ibkrStatus = String(order?.ibkrStatus || "").toLowerCase();
  return {
    commissionAmount: Math.abs(expected),
    commissionCcy: "EUR",
    commissionEUR: Math.abs(expected),
    commissionSource: ibkrStatus === "dry_run" ? "simulated_bps" : "ibkr_commission_missing",
  };
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
    const marketValue = toNum(pick(row, "MarketValue", "marketValue", "value"), null);
    if (marketValue !== null) return sum + marketValue;
    return sum + (toNum(pick(row, "Quantity", "quantity", "qty"), 0) * toNum(pick(row, "LastPrice", "lastPrice", "price"), 0));
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
    const symbol = normalizeSymbol(pick(row, "Symbol", "symbol"));
    const qty = toNum(pick(row, "Quantity", "quantity", "qty"), 0);
    if (!symbol || qty <= 0) continue;
    posMap.set(symbol, {
      symbol,
      qty,
      avgCost: toNum(pick(row, "AvgPrice", "avgPrice"), toNum(pick(row, "LastPrice", "lastPrice", "price"), 0) || 0),
      lastPrice: toNum(pick(row, "LastPrice", "lastPrice", "price"), toNum(pick(row, "AvgPrice", "avgPrice"), 0) || 0),
      assetClass: String(pick(row, "AssetClass", "assetClass", "asset_class") || "EQUITY").trim().toUpperCase() || "EQUITY",
      sector: clampText(pick(row, "Sector", "sector") || "UNKNOWN", 128) || "UNKNOWN",
    });
  }

  let cashEUR = startCash;
  let runFeesEUR = 0;
  for (const order of orders || []) {
    const symbol = normalizeSymbol(order?.symbol);
    const side = String(order?.side || "").trim().toUpperCase();
    const qty = toNum(order?.quantity, 0);
    const orderType = normalizeOrderType(order?.orderType);
    const price =
      (orderType === "LIMIT" ? toNum(order?.limitPrice, null) : null) ??
      toNum(priceMap[symbol], null);

    if (!symbol || qty <= 0 || !Number.isFinite(price) || price <= 0) continue;
    const feesEUR = extractCommission(order).commissionEUR;
    runFeesEUR += Math.max(0, feesEUR);

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
      cashEUR -= (qty * price) + feesEUR;
      continue;
    }

    if (side === "SELL") {
      const current = posMap.get(symbol);
      if (!current) continue;
      const execQty = Math.min(qty, current.qty);
      if (execQty <= 0) continue;
      current.qty -= execQty;
      current.lastPrice = toNum(priceMap[symbol], price);
      cashEUR += (execQty * price) - feesEUR;
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

  const initialCapitalEUR = toNum(meta?.initialCapitalEUR, 10000);
  const cumFeesEUR = toNum(meta?.cumFeesEUR, 0) + runFeesEUR;
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
const transferPack = input.transfer_pack || {};
const ctx = input.ctx || {};
const runCtx = ctx.run || input.run || transferPack.run || {};
const meta = input.meta || ctx.meta || transferPack.meta || {};
const agentDecision = input.agentDecision || {};
const ordersIn = Array.isArray(input.orders) ? input.orders : [];
const modelProposals = Array.isArray(input.modelProposals)
  ? input.modelProposals
  : (Array.isArray(agentDecision?.consensus?.modelProposals) ? agentDecision.consensus.modelProposals : []);
const consensusVotes = Array.isArray(input.consensusVotes)
  ? input.consensusVotes
  : (Array.isArray(agentDecision?.consensus?.votes) ? agentDecision.consensus.votes : []);
const consensusDecisions = Array.isArray(input.consensusDecisions)
  ? input.consensusDecisions
  : (Array.isArray(agentDecision?.consensus?.decisions) ? agentDecision.consensus.decisions : []);
const fillEffectOrders = ordersIn.filter(orderHasFillLikeEffect);
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
    const sym = String(pick(p, "Symbol", "symbol") ?? "").trim();
    const px = Number(pick(p, "LastPrice", "lastPrice", "price"));
    if (sym && Number.isFinite(px) && px > 0) priceMap[sym] = px;
  });
}

const instrumentMap = new Map();
if (Array.isArray(portfolioSummary.positions)) {
  portfolioSummary.positions.forEach((p) => {
    const symbol = normalizeSymbol(pick(p, "Symbol", "symbol"));
    if (!symbol || symbol === "CASH_EUR" || symbol === "__META__") return;
    instrumentMap.set(symbol, {
      symbol,
      name: clampText(pick(p, "Name", "name") || symbol, 256),
      asset_class: clampText(pick(p, "AssetClass", "assetClass", "asset_class") || "Equity", 64),
      sector: clampText(pick(p, "Sector", "sector") || "", 128) || null,
      industry: clampText(pick(p, "Industry", "industry") || "", 128) || null,
      isin: clampText(pick(p, "ISIN", "isin") || "", 64) || null,
    });
  });
}

// complete with limit prices from actions/orders
if (Array.isArray(agentDecision.actions)) {
  agentDecision.actions.forEach((a) => {
    const sym = String(a.symbol_internal || a.symbol || "").trim();
    const lp = Number(a.entryPlan?.limitPrice);
    if (sym && !(sym in priceMap) && Number.isFinite(lp) && lp > 0) priceMap[sym] = lp;
    if (sym && !instrumentMap.has(sym)) {
      instrumentMap.set(sym, {
        symbol: sym,
        name: sym,
        asset_class: clampText(a.assetClass || "Equity", 64),
        sector: null,
        industry: null,
        isin: null,
      });
    }
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

const fillRecords = fillEffectOrders.map((o, i) => {
  const sym = String(o.symbol || "").trim();
  const order_id = orderLedgerId(o, run_id, i);
  const fee = extractCommission(o);
  const px = extractFillPrice(o, priceMap);
  const qty = extractFillQty(o);
  const brokerExecutionId = extractBrokerExecutionId(o);
  const tsFill = extractFillTimestamp(o, ts_end);
  const fillId = brokerExecutionId
    ? `FIL_${run_id}_${brokerExecutionId}`
    : `FIL_${run_id}_${i}`;

  return {
    fill_id: fillId,
    order_id,
    symbol: sym,
    side: o.side,
    qty,
    price: (Number.isFinite(px) && px > 0) ? px : 1.0,
    ts_fill: tsFill,
    fees_eur: fee.commissionEUR,
    fee_amount: fee.commissionAmount,
    fee_ccy: fee.commissionCcy,
    fee_source: fee.commissionSource,
    broker: o.broker || (o.ibkrStatus ? "IBKR" : "SIM"),
    broker_execution_id: brokerExecutionId,
    slippage_bps: toNum(o.slippageBps ?? o.slippage_bps, null),
    liquidity: clampText(o.liquidity || "UNKNOWN", 32),
    raw_fill_json: {
      source: o.ibkrStatus === "dry_run" ? "simulated_sandbox" : "confirmed_or_imported",
      clientOrderId: o.clientOrderId || null,
      ibkrStatus: o.ibkrStatus || null,
      ibkrResponse: o.ibkrResponse || null,
      ibkrFill: rawFill(o) || null,
    },
  };
});

const fillCostRecords = fillRecords.map((f) => ({
  fill_id: f.fill_id,
  order_id: f.order_id,
  symbol: f.symbol,
  pair: f.symbol,
  broker: f.broker,
  broker_execution_id: f.broker_execution_id,
  commission_amount: f.fee_amount,
  commission_ccy: f.fee_ccy,
  commission_eur: f.fees_eur,
  commission_source: f.fee_source,
  raw_json: f.raw_fill_json,
  recorded_at: f.ts_fill,
}));

const snapshots = buildSnapshotsFromPortfolio(portfolioSummary, fillEffectOrders, priceMap, ts_end, meta);

const bundle = {
  run: {
    run_id,
    ts_start: runCtx.timestampParis || ts_end,
    ts_end,
    tz: "Europe/Paris",
    strategy_version: runCtx.strategyVersion || runCtx.strategy_version || null,
    config_version: runCtx.configVersion || runCtx.config_version || null,
    prompt_version: runCtx.promptVersion || runCtx.prompt_version || null,
    model: runCtx.model || "UNKNOWN",
    n8n_execution_id: runCtx.executionId || runCtx.n8nExecutionId || runCtx.n8n_execution_id || null,
    db_path: db_path || null,
    decision_summary: input.decision || "NO_TRADE",
    data_ok_for_trading: true,
    agent_output_json: agentDecision,
    warnings_json: warnings,
  },
  orders: ordersIn.map((o, i) => ({
    order_id: orderLedgerId(o, run_id, i),
    symbol: o.symbol,
    side: o.side,
    order_type: normalizeOrderType(o.orderType),
    qty: o.quantity,
    limit_price: o.limitPrice ?? null,
    status: orderStatus(o),
    broker: o.broker || (o.ibkrStatus ? "IBKR" : "SIM"),
    broker_order_id: brokerOrderId(o),
    reason: o.ibkrError || o.reason || null,
    rationale_json: {
      action: o.action,
      assetClass: o.assetClass,
      clientOrderId: o.clientOrderId || null,
      ibkrStatus: o.ibkrStatus || null,
      ibkrResponse: o.ibkrResponse || null,
      ibkrError: o.ibkrError || null,
      expectedFeesEUR: o.expectedFeesEUR || 0,
      riskChecks: o.riskChecks || null,
    },
  })),
  fills: fillRecords,
  fill_costs: fillCostRecords,
  cash_ledger: [],
  instruments: Array.from(instrumentMap.values()),
  market_prices: Object.entries(priceMap).map(([sym, px]) => ({ symbol: sym, close: px })),
  ai_signals,
  model_proposals: modelProposals,
  consensus_votes: consensusVotes,
  consensus_decisions: consensusDecisions,
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
      model_proposals: modelProposals.length,
      consensus_votes: consensusVotes.length,
      consensus_decisions: consensusDecisions.length,
      alerts: alerts.length,
    }
  }
}];

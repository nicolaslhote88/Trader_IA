function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function envNum(name, fallback) {
  const env = typeof $env !== 'undefined' ? $env : {};
  const n = Number(env[name]);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function envBool(name, fallback) {
  const env = typeof $env !== 'undefined' ? $env : {};
  const raw = env[name];
  if (raw == null || raw === '') return fallback;
  return ['1', 'true', 'yes', 'y', 'on'].includes(String(raw).trim().toLowerCase());
}

function pairMeta(brief, pair) {
  return (brief.universe?.metadata || []).find((x) => x.pair === pair) || { pair, base_ccy: pair.slice(0, 3), quote_ccy: pair.slice(3), pip_size: pair.endsWith('JPY') ? 0.01 : 0.0001 };
}

function lastPrice(brief, pair) {
  const row = (brief.technical_signals || []).find((x) => x.pair === pair);
  return num(row?.last_close, 0);
}

function quoteToEur(brief, quote) {
  if (quote === 'EUR') return 1;
  const direct = lastPrice(brief, `${quote}EUR`);
  if (direct > 0) return direct;
  const inv = lastPrice(brief, `EUR${quote}`);
  if (inv > 0) return 1 / inv;
  const qUsd = quote === 'USD' ? 1 : lastPrice(brief, `${quote}USD`) || (lastPrice(brief, `USD${quote}`) ? 1 / lastPrice(brief, `USD${quote}`) : 0);
  const usdEur = lastPrice(brief, 'USDEUR') || (lastPrice(brief, 'EURUSD') ? 1 / lastPrice(brief, 'EURUSD') : 0);
  return qUsd > 0 && usdEur > 0 ? qUsd * usdEur : 1;
}

function currencyExposures(lots, addOrders, brief) {
  const ex = {};
  function add(ccy, v) { ex[ccy] = (ex[ccy] || 0) + v; }
  for (const lot of lots || []) {
    const pair = lot.pair;
    const meta = pairMeta(brief, pair);
    const px = lastPrice(brief, pair) || num(lot.open_price, 0);
    const notional = Math.abs(num(lot.size_lots) * 100000 * px * quoteToEur(brief, meta.quote_ccy));
    const sign = lot.side === 'short' ? -1 : 1;
    add(meta.base_ccy, sign * notional);
    add(meta.quote_ccy, -sign * notional);
  }
  for (const o of addOrders || []) {
    const meta = pairMeta(brief, o.pair);
    const sign = o.side === 'sell_base' ? -1 : 1;
    add(meta.base_ccy, sign * o.notional_eur);
    add(meta.quote_ccy, -sign * o.notional_eur);
  }
  return ex;
}

function rejectionSummary(orders) {
  const byReason = {};
  let rejected = 0;
  for (const order of orders || []) {
    if (order.status !== 'rejected') continue;
    rejected += 1;
    const reason = order.rejection_reason || 'UNKNOWN';
    byReason[reason] = (byReason[reason] || 0) + 1;
  }
  return { rejected_orders_count: rejected, rejection_reasons: byReason };
}

function openLotById(openLots, lotId) {
  const id = String(lotId || '').trim();
  if (!id) return null;
  return (openLots || []).find((lot) => String(lot.lot_id || '').trim() === id) || null;
}

function closeSideForLot(lot) {
  if (lot?.side === 'long') return 'close_long';
  if (lot?.side === 'short') return 'close_short';
  return '';
}

function signalSign(v, threshold = 0.20) {
  const n = num(v, 0);
  if (Math.abs(n) < threshold) return 0;
  return n > 0 ? 1 : -1;
}

function sideToCubeDirection(side) {
  if (side === 'buy_base') return 'BUY_BASE';
  if (side === 'sell_base') return 'SELL_BASE';
  return 'WAIT';
}

function llmDecisionProfile(ctx, pair) {
  const p = String(pair || '').toUpperCase();
  const compact = ctx.llm_brief || {};
  const rows = [
    ...(compact.market_watch || []),
    ...(compact.pair_matrix || []),
  ];
  const row = rows.find((x) => String(x?.pair || '').toUpperCase() === p);
  return row?.decision || {};
}

function applySizeCap(order, sizeCapPct, equity, px, quoteToEurRate, reason) {
  const capNotionalEur = Math.max(0, equity * sizeCapPct);
  if (capNotionalEur <= 0 || order.notional_eur <= capNotionalEur + 1e-9) {
    return;
  }
  const denom = Math.max(1, 100000 * px * quoteToEurRate);
  const cappedLots = capNotionalEur / denom;
  order.risk_size_adjustment = {
    reason,
    requested_size_lots: order.size_lots,
    requested_notional_eur: order.notional_eur,
    capped_size_lots: cappedLots,
    capped_notional_eur: capNotionalEur,
    cap_pct_equity: sizeCapPct,
  };
  order.size_lots = cappedLots;
  order.notional_quote = Math.abs(cappedLots * 100000 * px);
  order.notional_eur = Math.abs(order.notional_quote * quoteToEurRate);
  order.risk_check_notes = `${order.risk_check_notes || ''} [Risk sizing: capped to ${(sizeCapPct * 100).toFixed(1)}% equity for ${reason}.]`.trim();
}

function cashOnlyBrokerOpenAllowed(pair, side, baseCcy) {
  const p = String(pair || '').toUpperCase();
  const b = String(baseCcy || 'EUR').toUpperCase();
  if (p.length !== 6) return false;
  const base = p.slice(0, 3);
  const quote = p.slice(3, 6);
  return (side === 'sell_base' && base === b) || (side === 'buy_base' && quote === b);
}

const j = $json || {};
const brief = j.brief || {};
const cfg = brief.config || {};
const limits = brief.limits || {};
const portfolio = brief.portfolio_state || {};
const universe = new Set(brief.universe?.pairs || []);
const decisions = j.decision_json?.decisions || [];
const equity = Math.max(1, num(portfolio.equity_eur, cfg.capital_eur || 10000));
const leverageMax = Math.max(0.01, num(cfg.leverage_max, 1));
const maxPairPct = num(limits.max_pair_pct, 0.20);
const maxCurrencyPct = num(limits.max_currency_exposure_pct, 0.50);
const maxDd = num(limits.max_daily_drawdown_pct, 0.05);
const reducedSizeMaxPairPct = Math.min(maxPairPct, envNum('AG1_FX_REDUCED_SIZE_MAX_PAIR_PCT', 0.10));
const cashOnlyBaseCcyMode = envBool('AG1_FX_CASH_ONLY_BASE_CCY_MODE', true);
const prefundNonEurFx = envBool('AG1_FX_PREFUND_NON_EUR_FX', true);
const prefundBufferPct = Math.max(0, envNum('AG1_FX_PREFUND_BUFFER_PCT', 0.005));
const portfolioBaseCcy = String(cfg.portfolio_base_ccy || (typeof $env !== 'undefined' ? $env.AG1_FX_PORTFOLIO_BASE_CCY : '') || 'EUR').toUpperCase();
const openLots = portfolio.open_lots || [];
const projected = [];
const orders = [];
const alerts = [];

function ibkrCashByCurrency(ctx) {
  return ctx.ibkr_reconciliation?.cash_balances?.ibkr_cash_by_currency
    || ctx.portfolio_state?.reconciliation?.cash_balances?.ibkr_cash_by_currency
    || ctx.brief?.portfolio_state?.reconciliation?.cash_balances?.ibkr_cash_by_currency
    || {};
}

function fundingNeedForOrder(order, px) {
  const pair = String(order.pair || '').toUpperCase();
  if (pair.length !== 6) return null;
  const base = pair.slice(0, 3);
  const quote = pair.slice(3, 6);
  const unitsBase = num(order.size_lots, 0) * 100000;
  if (unitsBase <= 0 || px <= 0) return null;
  if (order.side === 'sell_base') {
    return { currency: base, units: unitsBase, reason: `sell ${base} in ${pair}` };
  }
  if (order.side === 'buy_base') {
    return { currency: quote, units: unitsBase * px, reason: `sell ${quote} in ${pair}` };
  }
  return null;
}

function fundingPairForCurrency(ccy) {
  const target = String(ccy || '').toUpperCase();
  if (!target || target === portfolioBaseCcy) return null;
  const direct = `${portfolioBaseCcy}${target}`;
  const inverse = `${target}${portfolioBaseCcy}`;
  if (universe.has(direct) && lastPrice(brief, direct) > 0) {
    return { pair: direct, side: 'sell_base', px: lastPrice(brief, direct) };
  }
  if (universe.has(inverse) && lastPrice(brief, inverse) > 0) {
    return { pair: inverse, side: 'buy_base', px: lastPrice(brief, inverse) };
  }
  return null;
}

function buildPrefundingOrder(targetOrder, need, availableUnits) {
  if (!need || need.currency === portfolioBaseCcy) return null;
  const deficitUnits = Math.max(0, need.units * (1 + prefundBufferPct) - Math.max(0, availableUnits));
  if (deficitUnits <= 1e-6) return null;
  const funding = fundingPairForCurrency(need.currency);
  if (!funding) {
    targetOrder.rejection_reason = 'PREFUNDING_PAIR_UNAVAILABLE';
    targetOrder.risk_check_notes = `${targetOrder.risk_check_notes || ''} [Prefunding unavailable: no ${portfolioBaseCcy}/${need.currency} conversion pair with price.]`.trim();
    return null;
  }
  const fundingMeta = pairMeta(brief, funding.pair);
  const fundingSizeLots = funding.side === 'buy_base'
    ? deficitUnits / 100000
    : deficitUnits / Math.max(1, 100000 * funding.px);
  const fundingQuoteToEur = quoteToEur(brief, fundingMeta.quote_ccy);
  const fundingNotionalQuote = Math.abs(fundingSizeLots * 100000 * funding.px);
  const fundingNotionalEur = Math.abs(fundingNotionalQuote * fundingQuoteToEur);
  const fundingOrder = {
    ...targetOrder,
    order_id: `${targetOrder.order_id}_FUND`,
    client_order_id: `${targetOrder.client_order_id}::FUND`,
    pair: funding.pair,
    side: funding.side,
    order_type: 'cash_conversion',
    size_lots: fundingSizeLots,
    notional_quote: fundingNotionalQuote,
    notional_eur: fundingNotionalEur,
    limit_price: null,
    stop_loss_price: null,
    take_profit_price: null,
    status: 'pending',
    rejection_reason: '',
    risk_check_passed: true,
    risk_check_notes: `Prefund ${need.currency} via ${funding.pair} before ${targetOrder.pair}: ${need.reason}.`,
    decision: 'cash_prefund',
    horizon: 'intraday',
    lot_id_to_close: '',
    is_currency_conversion: true,
    funding_for_order_id: targetOrder.order_id,
    prefund_currency: need.currency,
    prefund_required_units: need.units,
    prefund_available_units: availableUnits,
    prefund_deficit_units: deficitUnits,
  };
  targetOrder.requires_funding_order_id = fundingOrder.order_id;
  targetOrder.prefund_currency = need.currency;
  targetOrder.prefund_required_units = need.units;
  targetOrder.prefund_available_units = availableUnits;
  targetOrder.risk_check_notes = `${targetOrder.risk_check_notes || ''} [Prefunding: ${fundingOrder.order_id} buys ${need.currency} before target order.]`.trim();
  return fundingOrder;
}

let killSwitch = Boolean(cfg.kill_switch_active);
const drawdownDayFrac = num(portfolio.drawdown_day_frac, num(portfolio.drawdown_day_pct, 0));
if (drawdownDayFrac <= -maxDd) {
  killSwitch = true;
  alerts.push({ severity: 'critical', category: 'kill_switch', message: 'Daily drawdown gate breached; opens blocked' });
}
if (j.reconciliation_block_new_orders || j.ibkr_reconciliation?.block_new_orders || portfolio.reconciliation?.block_new_orders) {
  killSwitch = true;
  alerts.push({
    severity: 'critical',
    category: 'ibkr_reconciliation',
    message: 'IBKR vs DuckDB divergence or broker guard failure; all new opens blocked',
    payload: j.ibkr_reconciliation || portfolio.reconciliation || {},
  });
}

let seq = 1;
for (const d of decisions) {
  const lotToClose = openLotById(openLots, d.lot_id_to_close);
  const pair = lotToClose?.pair || d.pair;
  const action = d.decision;
  const side = action === 'open_long'
    ? 'buy_base'
    : action === 'open_short'
      ? 'sell_base'
      : (action === 'close' || action === 'partial_close')
        ? closeSideForLot(lotToClose)
        : 'hold';
  const orderId = `ORD_${j.run_id}_${String(seq).padStart(3, '0')}`;
  const base = {
    order_id: orderId,
    client_order_id: `${j.run_id}::${pair || 'UNKNOWN'}::${side}::${seq}`,
    run_id: j.run_id,
    pair,
    side,
    order_type: 'market',
    size_lots: 0,
    notional_quote: 0,
    notional_eur: 0,
    leverage_used: leverageMax,
    limit_price: null,
    stop_loss_price: d.stop_loss_price ?? null,
    take_profit_price: d.take_profit_price ?? null,
    status: 'rejected',
    rejection_reason: '',
    risk_check_passed: false,
    risk_check_notes: d.rationale || '',
    decision: action,
    conviction: d.conviction,
    horizon: d.horizon,
    lot_id_to_close: d.lot_id_to_close || '',
  };
  seq += 1;

  if (action === 'hold') continue;
  if (!universe.has(pair)) {
    base.rejection_reason = 'PAIR_NOT_IN_UNIVERSE';
    orders.push(base);
    continue;
  }
  if (killSwitch && action.startsWith('open_')) {
    base.rejection_reason = 'KILL_SWITCH_ACTIVE';
    orders.push(base);
    continue;
  }

  const px = lastPrice(brief, pair);
  if (px <= 0) {
    base.rejection_reason = 'NO_ENTRY_PRICE';
    orders.push(base);
    continue;
  }
  const meta = pairMeta(brief, pair);
  const quoteToEurRate = quoteToEur(brief, meta.quote_ccy);
  const profile = action.startsWith('open_') ? llmDecisionProfile(j, pair) : {};
  const tradePermission = String(profile.trade_permission || 'ALLOW').toUpperCase();
  const cube = profile.cube || {};
  const effectivePairPct = tradePermission === 'REDUCED_SIZE_ONLY' ? reducedSizeMaxPairPct : maxPairPct;
  if (action.startsWith('open_')) {
    base.trade_permission = tradePermission;
    base.decision_alignment = profile.decision_alignment || '';
    base.preferred_action = profile.preferred_action || '';
    base.cube_check = {
      zone: cube.cube_zone || '',
      x_technical: cube.x_technical,
      y_news_event: cube.y_news_event,
      z_three_pillars: cube.z_three_pillars,
      action_hint: cube.portfolio_action_hint,
    };
  }
  let sizeLots = num(d.size_lots, 0);
  if (sizeLots <= 0 && num(d.size_pct_equity, 0) > 0) {
    const targetEur = equity * Math.min(effectivePairPct, num(d.size_pct_equity));
    sizeLots = targetEur / Math.max(1, 100000 * px * quoteToEurRate);
  }
  base.size_lots = sizeLots;
  base.notional_quote = Math.abs(sizeLots * 100000 * px);
  base.notional_eur = Math.abs(base.notional_quote * quoteToEurRate);

  if (action.startsWith('open_')) {
    if (!base.rejection_reason && tradePermission === 'NO_NEW_POSITION') base.rejection_reason = 'TRADE_PERMISSION_NO_NEW_POSITION';
    if (!base.rejection_reason && cube.structural_data_complete === false) base.rejection_reason = 'CUBE_STRUCTURAL_DATA_INCOMPLETE';
    if (!base.rejection_reason && cube.crowded_warning) base.rejection_reason = 'CUBE_CROWDED_WARNING';
    if (!base.rejection_reason && num(cube.event_risk_score, 0) >= 0.75) base.rejection_reason = 'CUBE_EVENT_RISK_TOO_HIGH';
    if (!base.rejection_reason && !String(cube.cube_zone || '').startsWith('convergence_multi_horizon')) base.rejection_reason = 'CUBE_NOT_MULTI_HORIZON_CONVERGENCE';
    if (!base.rejection_reason && cube.cube_direction && cube.cube_direction !== sideToCubeDirection(side)) base.rejection_reason = 'CUBE_DIRECTION_MISMATCH';
    if (!base.rejection_reason && signalSign(cube.z_three_pillars) === 0) base.rejection_reason = 'CUBE_Z_TOO_WEAK';
    if (!base.rejection_reason && sizeLots <= 0) base.rejection_reason = 'INVALID_SIZE';
    if (!base.rejection_reason && tradePermission === 'REDUCED_SIZE_ONLY') {
      applySizeCap(base, reducedSizeMaxPairPct, equity, px, quoteToEurRate, 'REDUCED_SIZE_ONLY');
      sizeLots = base.size_lots;
    }
    const pairExisting = openLots.filter((l) => l.pair === pair).reduce((s, l) => s + Math.abs(num(l.size_lots) * 100000 * (lastPrice(brief, pair) || num(l.open_price)) * quoteToEur(brief, meta.quote_ccy)), 0);
    if (!base.rejection_reason && (pairExisting + base.notional_eur) / equity > maxPairPct) base.rejection_reason = 'MAX_PAIR_EXPOSURE';
    const totalNotional = openLots.reduce((s, l) => {
      const m = pairMeta(brief, l.pair);
      return s + Math.abs(num(l.size_lots) * 100000 * (lastPrice(brief, l.pair) || num(l.open_price)) * quoteToEur(brief, m.quote_ccy));
    }, 0) + projected.reduce((s, o) => s + o.notional_eur, 0) + base.notional_eur;
    if (!base.rejection_reason && totalNotional / equity > leverageMax) base.rejection_reason = 'LEVERAGE_MAX';
    if (!base.rejection_reason && base.notional_eur / leverageMax > num(portfolio.margin_free_eur, equity)) base.rejection_reason = 'INSUFFICIENT_MARGIN';
    const ex = currencyExposures(openLots, [...projected, base], brief);
    if (!base.rejection_reason && Object.values(ex).some((v) => Math.abs(v) / equity > maxCurrencyPct)) base.rejection_reason = 'MAX_CURRENCY_EXPOSURE';
    if (!base.rejection_reason && action === 'open_long' && base.stop_loss_price && base.stop_loss_price >= px) base.rejection_reason = 'STOP_LOSS_WRONG_SIDE';
    if (!base.rejection_reason && action === 'open_long' && base.take_profit_price && base.take_profit_price <= px) base.rejection_reason = 'TAKE_PROFIT_WRONG_SIDE';
    if (!base.rejection_reason && action === 'open_short' && base.stop_loss_price && base.stop_loss_price <= px) base.rejection_reason = 'STOP_LOSS_WRONG_SIDE';
    if (!base.rejection_reason && action === 'open_short' && base.take_profit_price && base.take_profit_price >= px) base.rejection_reason = 'TAKE_PROFIT_WRONG_SIDE';
    if (!base.rejection_reason) {
      let fundingOrder = null;
      if (cashOnlyBaseCcyMode && !cashOnlyBrokerOpenAllowed(pair, side, portfolioBaseCcy)) {
        if (!prefundNonEurFx) {
          base.rejection_reason = `IBKR_CASH_ONLY_${portfolioBaseCcy}_LEG_REQUIRED`;
          base.risk_check_notes = `${base.risk_check_notes || ''} [Broker guard: live IBKR paper account rejects new FX orders that borrow non-${portfolioBaseCcy} currency.]`.trim();
        } else {
          const need = fundingNeedForOrder(base, px);
          const available = num(ibkrCashByCurrency(j)[need?.currency], 0);
          fundingOrder = buildPrefundingOrder(base, need, available);
        }
      }
      if (base.rejection_reason) {
        orders.push(base);
        continue;
      }
      if (fundingOrder) {
        orders.push(fundingOrder);
      }
      base.status = 'pending';
      base.risk_check_passed = true;
      projected.push(base);
    }
  } else {
    if (!d.lot_id_to_close) {
      base.rejection_reason = 'MISSING_LOT_ID_TO_CLOSE';
      orders.push(base);
      continue;
    }
    if (!lotToClose) {
      base.rejection_reason = 'LOT_TO_CLOSE_NOT_FOUND';
      orders.push(base);
      continue;
    }
    if (d.pair && d.pair !== lotToClose.pair) {
      base.rejection_reason = 'LOT_PAIR_MISMATCH';
      orders.push(base);
      continue;
    }
    if (!side) {
      base.rejection_reason = 'LOT_SIDE_INVALID';
      orders.push(base);
      continue;
    }
    const lotSize = num(lotToClose.size_lots, 0);
    if (lotSize <= 0) {
      base.rejection_reason = 'LOT_SIZE_INVALID';
      orders.push(base);
      continue;
    }
    if (action === 'close') {
      sizeLots = lotSize;
    } else if (action === 'partial_close') {
      if (sizeLots <= 0) {
        base.rejection_reason = 'INVALID_PARTIAL_CLOSE_SIZE';
        orders.push(base);
        continue;
      }
      if (sizeLots > lotSize + 1e-9) {
        base.rejection_reason = 'CLOSE_SIZE_EXCEEDS_LOT';
        orders.push(base);
        continue;
      }
    }
    base.size_lots = sizeLots;
    base.notional_quote = Math.abs(sizeLots * 100000 * px);
    base.notional_eur = Math.abs(base.notional_quote * quoteToEur(brief, meta.quote_ccy));
    base.status = 'pending';
    base.risk_check_passed = true;
  }
  orders.push(base);
}

const safetySummary = rejectionSummary(orders);
return [{ json: { ...j, kill_switch_active_effective: killSwitch, executable_orders: orders, risk_alerts: alerts, safety_summary: safetySummary } }];

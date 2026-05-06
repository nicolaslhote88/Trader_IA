function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
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
const openLots = portfolio.open_lots || [];
const projected = [];
const orders = [];
const alerts = [];

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
  let sizeLots = num(d.size_lots, 0);
  if (sizeLots <= 0 && num(d.size_pct_equity, 0) > 0) {
    const targetEur = equity * Math.min(maxPairPct, num(d.size_pct_equity));
    sizeLots = targetEur / Math.max(1, 100000 * px * quoteToEur(brief, meta.quote_ccy));
  }
  base.size_lots = sizeLots;
  base.notional_quote = Math.abs(sizeLots * 100000 * px);
  base.notional_eur = Math.abs(base.notional_quote * quoteToEur(brief, meta.quote_ccy));

  if (action.startsWith('open_')) {
    if (sizeLots <= 0) base.rejection_reason = 'INVALID_SIZE';
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

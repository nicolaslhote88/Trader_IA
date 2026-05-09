/**
 * Node 11b — Size computation + portfolio risk checks (AG1-FX-V2)
 *
 * Receives validated_orders from node 11 (pillar/crowded filter).
 * Computes notional, applies leverage/margin/currency-exposure limits,
 * and outputs executable_orders in the format expected by nodes 12–17.
 *
 * Maps V2 intents to V1-compatible sides:
 *   OPEN  buy_base   → buy_base (long)
 *   OPEN  sell_base  → sell_base (short)
 *   CLOSE close      → close_long or close_short (based on open lots)
 *   INCREASE/DECREASE → same side as original action
 */

function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function pairMeta(brief, pair) {
  return (brief.universe?.metadata || []).find((x) => x.pair === pair)
    || { pair, base_ccy: pair.slice(0, 3), quote_ccy: pair.slice(3), pip_size: pair.endsWith('JPY') ? 0.01 : 0.0001 };
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
  const qUsd = quote === 'USD' ? 1
    : lastPrice(brief, `${quote}USD`) || (lastPrice(brief, `USD${quote}`) ? 1 / lastPrice(brief, `USD${quote}`) : 0);
  const usdEur = lastPrice(brief, 'USDEUR') || (lastPrice(brief, 'EURUSD') ? 1 / lastPrice(brief, 'EURUSD') : 0);
  return qUsd > 0 && usdEur > 0 ? qUsd * usdEur : 1;
}

function currencyExposures(lots, addOrders, brief) {
  const ex = {};
  function add(ccy, v) { ex[ccy] = (ex[ccy] || 0) + v; }
  for (const lot of lots || []) {
    const meta = pairMeta(brief, lot.pair);
    const px = lastPrice(brief, lot.pair) || num(lot.open_price, 0);
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

const j = $json || {};
const brief = j.brief || {};
const cfg = j.config || brief.config || {};
const portfolio = j.portfolio_state || brief.portfolio_state || {};
const universe = new Set((j.universe_fx || []).map((u) => u.pair));
const validatedOrders = j.validated_orders || [];
const openLots = portfolio.open_lots || [];

const equity = Math.max(1, num(portfolio.equity_eur, num(cfg.initial_capital_eur, 10000)));
const leverageMax = Math.max(0.01, num(cfg.leverage_max, 2.0));
const maxPairPct = num(cfg.max_pair_pct, 0.20);
const maxCurrencyPct = num(cfg.max_currency_exposure_pct, 0.50);
const maxDd = num(cfg.max_daily_drawdown_pct, 0.05);

let killSwitch = Boolean(cfg.kill_switch_active);
const alerts = [];
if (num(portfolio.drawdown_day_pct, 0) <= -maxDd) {
  killSwitch = true;
  alerts.push({ severity: 'critical', category: 'kill_switch', message: 'Daily drawdown gate breached; opens blocked' });
}

const projected = [];
const orders = [];
let seq = 1;

for (const action of validatedOrders) {
  const intent = String(action.intent || 'OPEN').toUpperCase();
  const pair = String(action.pair || '').toUpperCase();
  const side = String(action.side || 'buy_base').toLowerCase();
  const orderId = `ORD_${j.run_id}_${String(seq).padStart(3, '0')}`;
  const rationale = action.rationale || {};
  const conviction = num(rationale.conviction, 0);
  seq += 1;

  const base = {
    order_id: orderId,
    client_order_id: `${j.run_id}::${pair}::${side}::${intent}::${seq}`,
    run_id: j.run_id,
    pair,
    side,
    intent,
    order_type: 'market',
    size_lots: 0,
    notional_quote: 0,
    notional_eur: 0,
    leverage_used: leverageMax,
    limit_price: null,
    stop_loss_price: null,
    take_profit_price: null,
    three_pillars_check: action.three_pillars_check || 'passed',
    status: 'rejected',
    rejection_reason: '',
    risk_check_passed: false,
    risk_check_notes: rationale.thesis || '',
    conviction,
  };

  // CLOSE and DECREASE always pass risk checks
  if (intent === 'CLOSE' || intent === 'DECREASE') {
    const existingLot = openLots.find((l) => l.pair === pair);
    const closeSide = existingLot
      ? (existingLot.side === 'long' ? 'close_long' : 'close_short')
      : 'close_long';
    base.side = closeSide;
    const sizeLots = num(action.size_lots, 0) > 0 ? num(action.size_lots, 0) : 999999;
    base.size_lots = sizeLots;
    base.status = 'pending';
    base.risk_check_passed = true;
    orders.push(base);
    continue;
  }

  // OPEN / INCREASE
  if (!universe.has(pair)) {
    base.rejection_reason = 'PAIR_NOT_IN_UNIVERSE';
    orders.push(base);
    continue;
  }
  if (killSwitch && intent === 'OPEN') {
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
  let sizeLots = num(action.size_lots, 0);
  if (sizeLots <= 0) {
    // Default: 5% of equity
    const targetEur = equity * 0.05;
    sizeLots = targetEur / Math.max(1, 100000 * px * quoteToEur(brief, meta.quote_ccy));
  }
  base.size_lots = sizeLots;
  base.notional_quote = Math.abs(sizeLots * 100000 * px);
  base.notional_eur = Math.abs(base.notional_quote * quoteToEur(brief, meta.quote_ccy));

  if (sizeLots <= 0) { base.rejection_reason = 'INVALID_SIZE'; }

  if (!base.rejection_reason) {
    const pairExisting = openLots
      .filter((l) => l.pair === pair)
      .reduce((s, l) => s + Math.abs(num(l.size_lots) * 100000 * (lastPrice(brief, pair) || num(l.open_price)) * quoteToEur(brief, meta.quote_ccy)), 0);
    if ((pairExisting + base.notional_eur) / equity > maxPairPct) {
      base.rejection_reason = 'MAX_PAIR_EXPOSURE';
    }
  }

  if (!base.rejection_reason) {
    const totalNotional = openLots.reduce((s, l) => {
      const m = pairMeta(brief, l.pair);
      return s + Math.abs(num(l.size_lots) * 100000 * (lastPrice(brief, l.pair) || num(l.open_price)) * quoteToEur(brief, m.quote_ccy));
    }, 0) + projected.reduce((s, o) => s + o.notional_eur, 0) + base.notional_eur;
    if (totalNotional / equity > leverageMax) {
      base.rejection_reason = 'LEVERAGE_MAX';
    }
  }

  if (!base.rejection_reason) {
    if (base.notional_eur / leverageMax > num(portfolio.margin_free_eur, equity)) {
      base.rejection_reason = 'INSUFFICIENT_MARGIN';
    }
  }

  if (!base.rejection_reason) {
    const ex = currencyExposures(openLots, [...projected, base], brief);
    if (Object.values(ex).some((v) => Math.abs(v) / equity > maxCurrencyPct)) {
      base.rejection_reason = 'MAX_CURRENCY_EXPOSURE';
    }
  }

  if (!base.rejection_reason) {
    base.status = 'pending';
    base.risk_check_passed = true;
    projected.push(base);
  }

  orders.push(base);
}

const rejectedCount = orders.filter((o) => o.status === 'rejected').length;
const safetyRejected = j.rejected_orders || [];
const safetyWarnings = j.warnings || [];

return [{
  json: {
    ...j,
    kill_switch_active_effective: killSwitch,
    executable_orders: orders,
    risk_alerts: alerts,
    safety_summary: {
      total_actions: (j.stats || {}).total || validatedOrders.length,
      pillar_rejected: (j.stats || {}).rejected || safetyRejected.length,
      crowded_forced_close: (j.stats || {}).forced_close_crowded || 0,
      risk_rejected: rejectedCount,
      safety_warnings: safetyWarnings,
      rejected_orders_count: rejectedCount + safetyRejected.length,
      rejection_reasons: orders
        .filter((o) => o.status === 'rejected' && o.rejection_reason)
        .reduce((acc, o) => { acc[o.rejection_reason] = (acc[o.rejection_reason] || 0) + 1; return acc; }, {}),
    },
  },
}];

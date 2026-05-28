const j = $json;
const cfg = j.config || {};

function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function rounded(v, decimals = 4, d = 0) {
  const n = num(v, d);
  const f = 10 ** decimals;
  return Math.round(n * f) / f;
}

function pct(v, decimals = 2) {
  return rounded(num(v, 0) * 100, decimals);
}

function clamp(v, lo = -1, hi = 1) {
  return Math.max(lo, Math.min(hi, num(v, 0)));
}

function signalSign(v, threshold = 0.20) {
  const n = num(v, 0);
  if (Math.abs(n) < threshold) return 0;
  return n > 0 ? 1 : -1;
}

const CONFIDENCE_RANK = { missing: 0, low: 1, medium: 2, high: 3 };
function confidenceFloor(values) {
  const ranks = (values || []).map((v) => CONFIDENCE_RANK[String(v || 'missing').toLowerCase()] ?? 0);
  const minRank = ranks.length ? Math.min(...ranks) : 0;
  return Object.entries(CONFIDENCE_RANK).find(([, rank]) => rank === minRank)?.[0] || 'missing';
}

function truncate(v, max = 160) {
  const s = String(v || '').replace(/\s+/g, ' ').trim();
  return s.length > max ? `${s.slice(0, max - 1).trim()}...` : s;
}

function unique(values) {
  return [...new Set((values || []).filter(Boolean))];
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

const MAX_LLM_NEWS = envNum('AG1_FX_LLM_TOP_NEWS_MAX', 6);
const MAX_LLM_WATCH = envNum('AG1_FX_LLM_MARKET_WATCH_MAX', 14);
const MAX_LLM_DRIVERS = envNum('AG1_FX_LLM_PAIR_DRIVERS_MAX', 2);
const CASH_ONLY_BASE_CCY_MODE = envBool('AG1_FX_CASH_ONLY_BASE_CCY_MODE', true);
const PREFUND_NON_EUR_FX = envBool('AG1_FX_PREFUND_NON_EUR_FX', true);
const PORTFOLIO_BASE_CCY = String((typeof $env !== 'undefined' ? $env.AG1_FX_PORTFOLIO_BASE_CCY : '') || 'EUR').toUpperCase();

const universeRows = j.universe_fx || [];
const technicalRows = j.technical_signals || [];
const macroNews = j.macro_news || { top_news: [], pair_focus: {}, macro_regime: {} };
const fundamentalFx = j.fundamental_fx || {};
const threePillars = j.three_pillars || {};
const byPillarCcy = threePillars.by_currency || {};
const pairFocus = macroNews.pair_focus || {};
const openLots = (j.portfolio_state || {}).open_lots || [];
const openPairs = new Set(openLots.map((x) => x.pair).filter(Boolean));

const brief = {
  run: {
    run_id: j.run_id,
    as_of: j.as_of,
    llm_model: j.llm_model,
  },
  config: {
    capital_eur: Number(cfg.initial_capital_eur || 10000),
    leverage_max: Number(cfg.leverage_max || 1),
    kill_switch_active: Boolean(cfg.kill_switch_active),
  },
  portfolio_state: j.portfolio_state || {},
  universe: {
    pairs: universeRows.map((x) => x.pair),
    metadata: universeRows,
  },
  technical_signals: technicalRows,
  macro_news: macroNews,
  fundamental_fx: fundamentalFx,
  three_pillars: {
    data_available: Boolean(threePillars.data_available),
    threshold: num(threePillars.threshold, 0.20),
    by_currency: byPillarCcy,
    opportunities: threePillars.opportunities || [],
    crowded_alerts: threePillars.crowded_alerts || [],
  },
  limits: {
    max_pair_pct: Number(cfg.max_pair_pct || cfg.max_pos_pct || 0.20),
    max_currency_exposure_pct: Number(cfg.max_currency_exposure_pct || 0.50),
    max_daily_drawdown_pct: Number(cfg.max_daily_drawdown_pct || 0.05),
    max_pair_frac: Number(cfg.max_pair_pct || cfg.max_pos_pct || 0.20),
    max_currency_exposure_frac: Number(cfg.max_currency_exposure_pct || 0.50),
    max_daily_drawdown_frac: Number(cfg.max_daily_drawdown_pct || 0.05),
    max_daily_drawdown_pct_display: pct(cfg.max_daily_drawdown_pct || 0.05, 2),
  },
};

const metaByPair = Object.fromEntries(universeRows.map((x) => [x.pair, x]));
const techByPair = Object.fromEntries(technicalRows.map((x) => [x.pair, x]));

function pairMeta(pair) {
  return metaByPair[pair] || {
    pair,
    base_ccy: String(pair || '').slice(0, 3),
    quote_ccy: String(pair || '').slice(3),
    pip_size: String(pair || '').endsWith('JPY') ? 0.01 : 0.0001,
    price_decimals: String(pair || '').endsWith('JPY') ? 3 : 5,
    liquidity_tier: 'unknown',
  };
}

function compactLot(lot) {
  const pair = String(lot.pair || '').toUpperCase();
  const meta = pairMeta(pair);
  const tech = techByPair[pair] || {};
  const pip = num(meta.pip_size, pair.endsWith('JPY') ? 0.01 : 0.0001);
  const px = num(tech.last_close, lot.open_price);
  const dir = lot.side === 'short' ? -1 : 1;
  const pnlPips = pip > 0 ? (px - num(lot.open_price, px)) * dir / pip : 0;
  const sl = lot.stop_loss_price == null ? null : num(lot.stop_loss_price, null);
  const tp = lot.take_profit_price == null ? null : num(lot.take_profit_price, null);
  const q2e = num(lot.quote_to_eur, 1);
  const sizeLots = num(lot.size_lots, 0);
  const slDistancePips = sl == null || pip <= 0 ? null : Math.abs(sl - px) / pip;
  const tpDistancePips = tp == null || pip <= 0 ? null : Math.abs(tp - px) / pip;
  const slRiskEur = sl == null ? null : Math.max(0, sizeLots * 100000 * (sl - px) * -dir * q2e);
  return {
    lot_id: lot.lot_id,
    pair,
    side: lot.side,
    size_lots: rounded(sizeLots, 4),
    open_price: rounded(lot.open_price, num(meta.price_decimals, 5)),
    current_price: rounded(px, num(meta.price_decimals, 5)),
    pnl_pips: rounded(pnlPips, 1),
    pnl_eur: rounded(lot.unrealized_pnl_eur, 2),
    notional_eur: rounded(lot.notional_eur, 2),
    sl: sl == null ? null : rounded(sl, num(meta.price_decimals, 5)),
    tp: tp == null ? null : rounded(tp, num(meta.price_decimals, 5)),
    sl_distance_pips: slDistancePips == null ? null : rounded(slDistancePips, 1),
    tp_distance_pips: tpDistancePips == null ? null : rounded(tpDistancePips, 1),
    sl_risk_eur: slRiskEur == null ? null : rounded(slRiskEur, 2),
  };
}

function compactTech(row, detail = false) {
  const pair = String(row.pair || '').toUpperCase();
  const meta = pairMeta(pair);
  const pip = num(row.pip_size, num(meta.pip_size, pair.endsWith('JPY') ? 0.01 : 0.0001));
  const decimals = num(meta.price_decimals, pair.endsWith('JPY') ? 3 : 5);
  const out = {
    pair,
    px: rounded(row.last_close, decimals),
    signal: row.signal_label || 'neutral',
    score: rounded(row.signal_score, 2),
    regime: row.regime || '',
    rsi14: rounded(row.rsi14, 1),
    ret1d_pct: pct(row.ret_1d, 2),
    ret5d_pct: pct(row.ret_5d, 2),
    ret20d_pct: pct(row.ret_20d, 2),
    atr_pips: pip > 0 ? rounded(num(row.atr14) / pip, 1) : 0,
  };
  if (row.ibkr_mid != null || row.ibkr_market_data_source) {
    out.market_px = {
      source: row.ibkr_market_data_source || 'unknown',
      bid: row.ibkr_bid == null ? null : rounded(row.ibkr_bid, decimals),
      ask: row.ibkr_ask == null ? null : rounded(row.ibkr_ask, decimals),
      mid: row.ibkr_mid == null ? null : rounded(row.ibkr_mid, decimals),
      spread_bps: row.ibkr_spread_pct == null ? null : rounded(num(row.ibkr_spread_pct) * 10000, 2),
    };
  }
  if (detail) {
    out.sma20_gap_pct = num(row.sma20) ? pct((num(row.last_close) / num(row.sma20)) - 1, 2) : 0;
    out.sma50_gap_pct = num(row.sma50) ? pct((num(row.last_close) / num(row.sma50)) - 1, 2) : 0;
    out.macd_hist = rounded(row.macd_hist, 5);
    out.pivot = rounded(row.pivot, decimals);
    out.s1 = rounded(row.s1, decimals);
    out.r1 = rounded(row.r1, decimals);
    out.bb_width_pct = pct(row.bb_width, 2);
  }
  return out;
}

function compactPairFocus(pair, detail = false) {
  const f = pairFocus[pair] || {};
  const out = {
    bias: f.bias_news || 'unknown',
    bias_score: rounded(f.bias_news_score, 2),
    confidence: rounded(f.confidence, 2),
    news_24h: Number(f.news_count_24h || 0),
    urgent_4h: Boolean(f.urgent_event_within_4h),
  };
  if (detail) {
    out.drivers = (f.top_drivers || []).slice(0, MAX_LLM_DRIVERS).map((x) => truncate(x, 110));
  }
  return out;
}

function newsEventAxis(pair) {
  const f = pairFocus[pair] || {};
  const rawScore = Number.isFinite(Number(f.bias_news_score)) ? Number(f.bias_news_score) : 0;
  const direction = signedDirectionFromNews(pair);
  let score = rawScore;
  if (direction === 'BUY_BASE') score = Math.abs(rawScore || 1);
  else if (direction === 'SELL_BASE') score = -Math.abs(rawScore || 1);
  else if (Math.abs(rawScore) > 1) score = 0;
  const eventRisk = f.urgent_event_within_4h ? 0.85 : Math.max(num(f.event_risk_score, 0), num(f.urgency_score, 0));
  return {
    score: clamp(score),
    direction,
    event_risk_score: Math.max(0, Math.min(1, eventRisk > 1 ? eventRisk / 100 : eventRisk)),
  };
}

function cubeZone(xTechnical, yNews, zPillars, hasStructuralData) {
  if (!hasStructuralData) return 'structural_data_incomplete';
  const sx = signalSign(xTechnical);
  const sy = signalSign(yNews);
  const sz = signalSign(zPillars);
  if (sx && sy && sz && sx === sy && sy === sz) {
    return sz > 0 ? 'convergence_multi_horizon_long_base' : 'convergence_multi_horizon_short_base';
  }
  const shortTerm = signalSign((xTechnical + yNews) / 2);
  if (sz && shortTerm && shortTerm === -sz) return 'pullback_against_structural_z';
  if (sx && sy && sx === sy && sz && sx === -sz) return 'short_term_hype_against_pillars';
  if (sz && (!sx || !sy)) return 'structural_valid_short_term_neutral';
  return 'neutral_or_mixed';
}

function sideFromDirection(direction) {
  return direction > 0 ? 'BUY_BASE' : direction < 0 ? 'SELL_BASE' : 'WAIT';
}

function pairCubeSignal(pair) {
  const p = String(pair || '').toUpperCase();
  const base = p.slice(0, 3);
  const quote = p.slice(3, 6);
  const tech = techByPair[p] || {};
  const news = newsEventAxis(p);
  const basePillar = byPillarCcy[base] || {};
  const quotePillar = byPillarCcy[quote] || {};
  const baseComplete = basePillar.score_status !== 'data_incomplete' && basePillar.composite_score !== null && basePillar.composite_score !== undefined;
  const quoteComplete = quotePillar.score_status !== 'data_incomplete' && quotePillar.composite_score !== null && quotePillar.composite_score !== undefined;
  const structuralComplete = Boolean(baseComplete && quoteComplete);
  const structuralConfidenceFloor = structuralComplete
    ? confidenceFloor([basePillar.confidence_floor || 'missing', quotePillar.confidence_floor || 'missing'])
    : 'missing';
  const structuralProxyUsed = structuralComplete && (
    basePillar.score_status === 'scored_proxy' ||
    quotePillar.score_status === 'scored_proxy' ||
    basePillar.data_completeness === 'proxy_complete' ||
    quotePillar.data_completeness === 'proxy_complete' ||
    structuralConfidenceFloor === 'low'
  );
  const macroSignal = num(basePillar.macro_score, 0) - num(quotePillar.macro_score, 0);
  const valuationSignal = num(basePillar.valuation_score, 0) - num(quotePillar.valuation_score, 0);
  const positioningSignal = num(basePillar.positioning_score, 0) - num(quotePillar.positioning_score, 0);
  const z = structuralComplete ? clamp((macroSignal + valuationSignal + positioningSignal) / 3) : null;
  const x = clamp(tech.signal_score || 0);
  const y = news.score;
  const zone = cubeZone(x, y, z, structuralComplete);
  const zSign = signalSign(z);
  const direction = sideFromDirection(zSign);
  const crowdedWarning = Boolean(basePillar.crowded_flag && (
    (basePillar.crowded_direction === 'long' && zSign > 0) ||
    (basePillar.crowded_direction === 'short' && zSign < 0)
  ));
  let actionHint = 'WATCH';
  if (!structuralComplete) actionHint = 'NO_STRUCTURAL_TRADE';
  else if (crowdedWarning) actionHint = 'AVOID_CROWDED';
  else if (news.event_risk_score >= 0.75) actionHint = openPairs.has(p) ? 'REDUCE_OR_WAIT_EVENT_RISK' : 'AVOID_EVENT_RISK';
  else if (zone.startsWith('convergence_multi_horizon')) actionHint = openPairs.has(p) ? 'HOLD_OR_INCREASE' : 'OPEN_CANDIDATE';
  else if (zone === 'pullback_against_structural_z') actionHint = openPairs.has(p) ? 'INCREASE_CANDIDATE' : 'WATCH_PULLBACK';
  else if (zone === 'short_term_hype_against_pillars') actionHint = openPairs.has(p) ? 'REDUCE_CANDIDATE' : 'AVOID_CHASING';
  else if (zone === 'structural_valid_short_term_neutral') actionHint = openPairs.has(p) ? 'HOLD' : 'WATCH';

  return {
    pair: p,
    x_technical: rounded(x, 3),
    y_news_event: rounded(y, 3),
    z_three_pillars: z == null ? null : rounded(z, 3),
    cube_alignment_score: z == null ? null : rounded((x + y + z) / 3, 3),
    cube_zone: zone,
    cube_direction: direction,
    macro_signal: rounded(macroSignal, 2),
    valuation_signal: rounded(valuationSignal, 2),
    positioning_signal: rounded(positioningSignal, 2),
    structural_data_complete: structuralComplete,
    structural_data_quality: structuralProxyUsed ? 'proxy_usable' : structuralComplete ? 'official_or_medium' : 'incomplete',
    structural_confidence_floor: structuralConfidenceFloor,
    structural_proxy_used: structuralProxyUsed,
    structural_missing: structuralComplete ? [] : [base, quote].filter((ccy) => {
      const row = byPillarCcy[ccy] || {};
      return row.score_status === 'data_incomplete' || row.composite_score === null || row.composite_score === undefined;
    }),
    event_risk_score: rounded(news.event_risk_score, 3),
    crowded_warning: crowdedWarning,
    portfolio_action_hint: structuralProxyUsed && actionHint === 'OPEN_CANDIDATE' ? 'OPEN_CANDIDATE_REDUCED_CONFIDENCE' : actionHint,
  };
}

function candidateScore(row) {
  const f = pairFocus[row.pair] || {};
  return (
    (openPairs.has(row.pair) ? 100 : 0) +
    Math.abs(num(row.signal_score)) * 25 +
    Math.min(10, Number(f.news_count_24h || 0)) +
    num(f.confidence) * 8 +
    (f.urgent_event_within_4h ? 5 : 0)
  );
}

const selectedPairs = unique([
  ...openPairs,
  ...technicalRows
    .slice()
    .sort((a, b) => candidateScore(b) - candidateScore(a))
    .slice(0, MAX_LLM_WATCH)
    .map((x) => x.pair),
]).slice(0, MAX_LLM_WATCH);

const pairMatrix = technicalRows.map((row) => ({
  ...compactTech(row, false),
  news: compactPairFocus(row.pair, false),
  cube: pairCubeSignal(row.pair),
}));

const marketWatch = selectedPairs.map((pair) => ({
  ...compactTech(techByPair[pair] || { pair }, true),
  meta: {
    base: pairMeta(pair).base_ccy,
    quote: pairMeta(pair).quote_ccy,
    pip: pairMeta(pair).pip_size,
    tier: pairMeta(pair).liquidity_tier,
  },
  news: compactPairFocus(pair, true),
  cube: pairCubeSignal(pair),
}));

const rawTopNews = macroNews.top_news || [];
const fxUsableTopNews = rawTopNews.filter((n) => (
  truncate(n.fx_directional_hint, 260).length > 0 &&
  (n.impact_fx_pairs || []).length > 0
));
const topNewsSource = fxUsableTopNews.length > 0
  ? fxUsableTopNews
  : rawTopNews.filter((n) => (n.impact_fx_pairs || []).length > 0);
const topNews = topNewsSource
  .slice(0, MAX_LLM_NEWS)
  .map((n) => ({
    published_at: n.published_at,
    impact: n.impact_magnitude,
    title: truncate(n.title, 140),
    pairs: (n.impact_fx_pairs || []).slice(0, 8),
    bullish: n.currencies_bullish || [],
    bearish: n.currencies_bearish || [],
    hint: truncate(n.fx_directional_hint, 220),
  }));

function compactFundamental(pair) {
  const f = fundamentalFx[pair] || {};
  const decimals = num(pairMeta(pair).price_decimals, String(pair || '').endsWith('JPY') ? 3 : 5);
  return {
    bias: f?.fundamental?.directional_bias || 'unknown',
    score: rounded(f?.fundamental?.score, 2),
    confidence: rounded(f?.fundamental?.confidence, 2),
    equilibrium_mid: rounded(f?.equilibrium?.target_mid, decimals),
    equilibrium_low: rounded(f?.equilibrium?.target_low, decimals),
    equilibrium_high: rounded(f?.equilibrium?.target_high, decimals),
    mispricing_pct: pct(f?.equilibrium?.mispricing_pct, 2),
    horizon_days: f?.equilibrium?.target_horizon_days || null,
    drivers: (f?.drivers || []).slice(0, 3).map((x) => truncate(x, 120)),
    invalidators: (f?.invalidators || []).slice(0, 3).map((x) => truncate(x, 120)),
    data_quality: rounded(f?.data_quality?.score, 2),
    macro_degraded: Boolean(f?.data_quality?.macro_data_degraded),
    missing_factors: (f?.data_quality?.missing_factors || []).slice(0, 4),
    stale_factors: (f?.data_quality?.stale_factors || []).slice(0, 4),
    conflict_score: rounded(f?.fundamental?.pair_conflict_score, 2),
  };
}

function signedDirectionFromTech(row) {
  const signal = String(row?.signal_label || '').toLowerCase();
  if (signal.includes('buy')) return 'BUY_BASE';
  if (signal.includes('sell')) return 'SELL_BASE';
  return 'NEUTRAL';
}

function signedDirectionFromNews(pair) {
  const focus = pairFocus[pair] || {};
  const bias = String(focus.bias_news || '').toLowerCase();
  const meta = pairMeta(pair);
  if (!bias || bias === 'unknown' || bias === 'mixed') return 'NEUTRAL';
  if (bias === `bullish_${String(meta.base_ccy || '').toLowerCase()}`) return 'BUY_BASE';
  if (bias === `bearish_${String(meta.base_ccy || '').toLowerCase()}`) return 'SELL_BASE';
  if (bias === `bullish_${String(meta.quote_ccy || '').toLowerCase()}`) return 'SELL_BASE';
  if (bias === `bearish_${String(meta.quote_ccy || '').toLowerCase()}`) return 'BUY_BASE';
  return 'NEUTRAL';
}

function directionConflict(a, b) {
  return !['NEUTRAL', 'unknown'].includes(a) && !['NEUTRAL', 'unknown'].includes(b) && a !== b;
}

function directionAligned(a, b) {
  return !['NEUTRAL', 'unknown'].includes(a) && a === b;
}

function cashOnlyOpenAllowed(pair, preferredAction) {
  if (!CASH_ONLY_BASE_CCY_MODE) return true;
  const p = String(pair || '').toUpperCase();
  if (p.length !== 6) return false;
  const base = p.slice(0, 3);
  const quote = p.slice(3, 6);
  return (preferredAction === 'SELL_BASE' && base === PORTFOLIO_BASE_CCY) || (preferredAction === 'BUY_BASE' && quote === PORTFOLIO_BASE_CCY);
}

function exposureSummary() {
  const equity = Math.max(1, num(brief.portfolio_state.equity_eur, brief.config.capital_eur || 10000));
  const byPair = {};
  const byCurrency = {};
  let totalSlRisk = 0;
  let missingSlCount = 0;

  function addCurrency(ccy, value) {
    if (!ccy) return;
    byCurrency[ccy] = (byCurrency[ccy] || 0) + value;
  }

  for (const lot of openLots) {
    const pair = String(lot.pair || '').toUpperCase();
    const meta = pairMeta(pair);
    const notional = Math.abs(num(lot.notional_eur, 0));
    const sign = String(lot.side || '').toLowerCase() === 'short' ? -1 : 1;
    byPair[pair] = (byPair[pair] || 0) + notional;
    addCurrency(meta.base_ccy, sign * notional);
    addCurrency(meta.quote_ccy, -sign * notional);

    const compact = compactLot(lot);
    if (compact.sl_risk_eur == null) {
      missingSlCount += 1;
    } else {
      totalSlRisk += Math.max(0, num(compact.sl_risk_eur, 0));
    }
  }

  const pairRows = Object.entries(byPair)
    .map(([pair, exposure]) => ({
      pair,
      exposure_eur: rounded(exposure, 2),
      exposure_pct_equity: rounded(exposure / equity, 4),
      remaining_eur: rounded(Math.max(0, equity * brief.limits.max_pair_frac - exposure), 2),
    }))
    .sort((a, b) => Math.abs(b.exposure_eur) - Math.abs(a.exposure_eur));

  const currencyRows = Object.entries(byCurrency)
    .map(([currency, exposure]) => ({
      currency,
      net_exposure_eur: rounded(exposure, 2),
      net_exposure_pct_equity: rounded(exposure / equity, 4),
      remaining_abs_eur: rounded(Math.max(0, equity * brief.limits.max_currency_exposure_frac - Math.abs(exposure)), 2),
    }))
    .sort((a, b) => Math.abs(b.net_exposure_eur) - Math.abs(a.net_exposure_eur));

  const dailyDdFrac = num(brief.portfolio_state.drawdown_day_frac, num(brief.portfolio_state.drawdown_day_pct, 0));
  const ddLimit = num(brief.limits.max_daily_drawdown_frac, 0.05);
  const killSwitchRequired = Boolean(brief.config.kill_switch_active) || dailyDdFrac <= -ddLimit;
  const leverageCapacityEur = Math.max(0, equity * num(brief.config.leverage_max, 1) - num(brief.portfolio_state.gross_exposure_eur, 0));
  const marginCapacityEur = Math.max(0, num(brief.portfolio_state.margin_free_eur, 0));

  return {
    can_open_new_trade: !killSwitchRequired && leverageCapacityEur > 0 && marginCapacityEur > 0,
    kill_switch_required: killSwitchRequired,
    daily_drawdown_limit_breached: dailyDdFrac <= -ddLimit,
    leverage_capacity_eur: rounded(leverageCapacityEur, 2),
    margin_capacity_eur: rounded(marginCapacityEur, 2),
    total_sl_risk_eur: rounded(totalSlRisk, 2),
    total_sl_risk_pct_equity: rounded(totalSlRisk / equity, 4),
    open_lots_without_sl: missingSlCount,
    by_pair: pairRows,
    by_currency: currencyRows,
  };
}

const portfolioRisk = exposureSummary();

function pairDecisionProfile(pair) {
  const tech = techByPair[pair] || { pair };
  const fund = compactFundamental(pair);
  const techDir = signedDirectionFromTech(tech);
  const fundDir = fund.bias || 'unknown';
  const newsDir = signedDirectionFromNews(pair);
  const urgentNews = Boolean((pairFocus[pair] || {}).urgent_event_within_4h);
  const cube = pairCubeSignal(pair);
  const macroDegraded = Boolean(fund.macro_degraded);
  const conflictScore = num(fund.conflict_score, 0);
  const techMacroConflict = directionConflict(techDir, fundDir);
  const newsConflict = directionConflict(newsDir, fundDir) || directionConflict(newsDir, techDir);
  const aligned = directionAligned(techDir, fundDir) || directionAligned(newsDir, fundDir);
  let alignment = 'NEUTRAL_OR_INSUFFICIENT';
  if (techMacroConflict) alignment = 'CONFLICT_TECH_VS_MACRO';
  else if (newsConflict) alignment = 'CONFLICT_NEWS_VS_SIGNAL';
  else if (conflictScore >= 0.5) alignment = 'CONFLICT_WITHIN_MACRO_SAFE_HAVENS';
  else if (aligned) alignment = 'ALIGNED';
  else if (fundDir !== 'NEUTRAL' && fundDir !== 'unknown') alignment = 'MACRO_ONLY';
  else if (techDir !== 'NEUTRAL') alignment = 'TECH_ONLY';

  let tradePermission = 'ALLOW';
  let preferredAction = fundDir === 'SELL_BASE' ? 'SELL_BASE' : fundDir === 'BUY_BASE' ? 'BUY_BASE' : 'WAIT';
  if (!portfolioRisk.can_open_new_trade) {
    tradePermission = 'NO_NEW_POSITION';
    preferredAction = 'MANAGE_OR_REDUCE_ONLY';
  } else if (alignment.startsWith('CONFLICT') || urgentNews || macroDegraded) {
    tradePermission = alignment.startsWith('CONFLICT') || urgentNews ? 'NO_NEW_POSITION' : 'REDUCED_SIZE_ONLY';
    preferredAction = openPairs.has(pair) ? 'MANAGE_EXISTING_ONLY' : 'WAIT';
  } else if (alignment === 'TECH_ONLY') {
    tradePermission = 'REDUCED_SIZE_ONLY';
    preferredAction = techDir === 'SELL_BASE' ? 'SELL_BASE' : techDir === 'BUY_BASE' ? 'BUY_BASE' : 'WAIT';
  }
  if (!openPairs.has(pair)) {
    if (!cube.structural_data_complete) {
      tradePermission = 'NO_NEW_POSITION';
      preferredAction = 'WAIT';
    } else if (cube.crowded_warning) {
      tradePermission = 'NO_NEW_POSITION';
      preferredAction = 'WAIT';
    } else if (cube.event_risk_score >= 0.75) {
      tradePermission = 'NO_NEW_POSITION';
      preferredAction = 'WAIT';
    } else if (cube.cube_zone.startsWith('convergence_multi_horizon')) {
      preferredAction = cube.cube_direction;
      if (cube.structural_proxy_used || cube.structural_confidence_floor === 'low') {
        tradePermission = tradePermission === 'NO_NEW_POSITION' ? tradePermission : 'REDUCED_SIZE_ONLY';
      }
    } else if (cube.cube_zone === 'short_term_hype_against_pillars') {
      tradePermission = 'NO_NEW_POSITION';
      preferredAction = 'WAIT';
    } else {
      tradePermission = tradePermission === 'ALLOW' ? 'REDUCED_SIZE_ONLY' : tradePermission;
    }
  }
  const brokerConstraint = CASH_ONLY_BASE_CCY_MODE && !['WAIT', 'MANAGE_OR_REDUCE_ONLY', 'MANAGE_EXISTING_ONLY'].includes(preferredAction) && !cashOnlyOpenAllowed(pair, preferredAction);
  if (brokerConstraint && !PREFUND_NON_EUR_FX && !openPairs.has(pair)) {
    tradePermission = 'NO_NEW_POSITION';
    preferredAction = 'WAIT';
  }

  return {
    decision_alignment: alignment,
    trade_permission: tradePermission,
    preferred_action: preferredAction,
    tech_direction: techDir,
    fundamental_direction: fundDir,
    news_direction: newsDir,
    urgent_news: urgentNews,
    macro_degraded: macroDegraded,
    broker_cash_only_blocked: brokerConstraint,
    prefunding_required: brokerConstraint && PREFUND_NON_EUR_FX && !openPairs.has(pair),
    cube,
  };
}

const allCubeSignals = technicalRows.map((row) => pairCubeSignal(row.pair));
const cubeSummary = {
  best_convergences: allCubeSignals
    .filter((p) => p.cube_zone.startsWith('convergence_multi_horizon') && !p.crowded_warning && p.event_risk_score < 0.75)
    .sort((a, b) => Math.abs(num(b.cube_alignment_score, 0)) - Math.abs(num(a.cube_alignment_score, 0)))
    .slice(0, 10),
  portfolio_positions_review: openLots.map((lot) => {
    const p = pairCubeSignal(lot.pair);
    const lotSide = String(lot.side || '').toLowerCase() === 'short' ? 'SELL_BASE' : 'BUY_BASE';
    return {
      pair: lot.pair,
      side: lot.side,
      cube_zone: p.cube_zone,
      z_three_pillars: p.z_three_pillars,
      structural_data_quality: p.structural_data_quality,
      structural_confidence_floor: p.structural_confidence_floor,
      event_risk_score: p.event_risk_score,
      crowded_warning: p.crowded_warning,
      portfolio_action_hint: p.cube_direction !== 'WAIT' && p.cube_direction !== lotSide ? 'REDUCE_OR_CLOSE_Z_FLIPPED' : p.portfolio_action_hint,
    };
  }),
  pullback_reinforcement_candidates: allCubeSignals.filter((p) => p.cube_zone === 'pullback_against_structural_z' && openPairs.has(p.pair) && !p.crowded_warning).slice(0, 10),
  short_term_hype_to_avoid: allCubeSignals.filter((p) => p.cube_zone === 'short_term_hype_against_pillars').slice(0, 10),
  missing_structural_data: allCubeSignals.filter((p) => !p.structural_data_complete).map((p) => ({ pair: p.pair, missing: p.structural_missing })),
};
brief.cube_summary = cubeSummary;

const regime = macroNews.macro_regime || {};
const llmBrief = {
  run: brief.run,
  config: brief.config,
  limits: brief.limits,
  broker_execution_constraints: {
    cash_only_base_ccy_mode: CASH_ONLY_BASE_CCY_MODE,
    prefund_non_eur_fx: PREFUND_NON_EUR_FX,
    portfolio_base_ccy: PORTFOLIO_BASE_CCY,
    allowed_new_open_patterns: CASH_ONLY_BASE_CCY_MODE
      ? [`SELL_BASE when pair base is ${PORTFOLIO_BASE_CCY}`, `BUY_BASE when pair quote is ${PORTFOLIO_BASE_CCY}`]
      : ['No cash-only currency-leg restriction'],
    prefunding_rule: PREFUND_NON_EUR_FX
      ? `For a new open outside the direct ${PORTFOLIO_BASE_CCY} patterns, the execution layer must first buy the currency that the target order will sell, using ${PORTFOLIO_BASE_CCY}. SELL_BASE needs the pair base currency; BUY_BASE needs the pair quote currency.`
      : 'New opens outside the direct base-currency patterns are blocked before IBKR.',
    note: 'Closes and reductions of existing lots remain allowed. When prefunding is enabled, do not invent separate funding decisions; propose the target trade only and the validator derives the EUR funding leg.',
  },
  portfolio: {
    cash_eur: rounded(brief.portfolio_state.cash_eur, 2),
    available_cash_eur: rounded(brief.portfolio_state.available_cash_eur, 2),
    ledger_cash_eur: rounded(brief.portfolio_state.ledger_cash_eur, 2),
    equity_eur: rounded(brief.portfolio_state.equity_eur, 2),
    realized_pnl_eur: rounded(brief.portfolio_state.realized_pnl_eur, 2),
    floating_pnl_eur: rounded(brief.portfolio_state.floating_pnl_eur, 2),
    fees_eur: rounded(brief.portfolio_state.fees_eur, 2),
    gross_exposure_eur: rounded(brief.portfolio_state.gross_exposure_eur, 2),
    margin_used_eur: rounded(brief.portfolio_state.margin_used_eur, 2),
    margin_free_eur: rounded(brief.portfolio_state.margin_free_eur, 2),
    leverage_effective: rounded(brief.portfolio_state.leverage_effective, 3),
    drawdown_day_frac: rounded(num(brief.portfolio_state.drawdown_day_frac, brief.portfolio_state.drawdown_day_pct), 6),
    drawdown_total_frac: rounded(num(brief.portfolio_state.drawdown_total_frac, brief.portfolio_state.drawdown_total_pct), 6),
    drawdown_day_pct_display: rounded(num(brief.portfolio_state.drawdown_day_pct_display, pct(brief.portfolio_state.drawdown_day_pct, 3)), 3),
    drawdown_total_pct_display: rounded(num(brief.portfolio_state.drawdown_total_pct_display, pct(brief.portfolio_state.drawdown_total_pct, 3)), 3),
    valuation_source: brief.portfolio_state.valuation_source || 'unknown',
    kill_switch_auto_reset: Boolean(brief.portfolio_state.kill_switch_auto_reset),
    valuation_warnings: brief.portfolio_state.valuation_warnings || {},
    open_lots: openLots.map(compactLot),
  },
  portfolio_risk: portfolioRisk,
  universe_pairs: brief.universe.pairs,
  macro: {
    market_regime: regime.market_regime || 'Unknown',
    confidence: rounded(regime.confidence, 2),
    currency_biases: regime.biases || {},
    drivers: truncate(regime.drivers, 420),
    as_of: regime.as_of,
  },
  top_news: topNews,
  pair_matrix: pairMatrix.map((row) => ({ ...row, decision: pairDecisionProfile(row.pair) })),
  market_watch: marketWatch.map((row) => ({ ...row, decision: pairDecisionProfile(row.pair) })),
  cube_summary: cubeSummary,
  briefing_notes: [
    'pair_matrix is a compact scan of all eligible pairs.',
    'market_watch contains the highest-priority/open pairs with extra technical and news context.',
    'drawdown_*_frac fields are fractions; drawdown_*_pct_display fields are human-readable percentages.',
    'portfolio_risk is precomputed. Do not open new positions if can_open_new_trade=false or trade_permission=NO_NEW_POSITION.',
    'broker_execution_constraints are hard live-execution constraints. If prefund_non_eur_fx=true, non-EUR target opens may be proposed only when their setup is strong; the validator will derive the required EUR funding leg.',
    'Use only universe_pairs. Prefer no trade when fundamental/macro and technicals conflict.',
    'Cube 3 axes is mandatory for new opens: X=technical, Y=news/event, Z=3 Pillars structural. Open only in convergence_multi_horizon_* and cite cube_zone in rationale.',
    'When cube structural_data_quality=proxy_usable or structural_confidence_floor=low, only reduced-size opens are allowed and the proxy limitation must be cited.',
    'If cube zone is structural_data_incomplete, keep the pair on watchlist only. If short-term X/Y conflicts with Z, explain why the trade is ignored or reduced.',
  ],
};

if (Object.keys(fundamentalFx).length > 0) {
  llmBrief.fundamental = {
    by_pair: Object.fromEntries(selectedPairs.map((pair) => [pair, compactFundamental(pair)])),
  };
}

const fullBriefChars = JSON.stringify(brief, null, 2).length;
const llmBriefJson = JSON.stringify(llmBrief);

return [{
  json: {
    ...j,
    brief,
    llm_brief: llmBrief,
    brief_stats: {
      full_brief_chars: fullBriefChars,
      llm_brief_chars: llmBriefJson.length,
      compression_ratio: fullBriefChars > 0 ? rounded(llmBriefJson.length / fullBriefChars, 3) : 1,
      market_watch_pairs: selectedPairs.length,
      top_news_items: topNews.length,
      top_news_filtered_out: Math.max(0, rawTopNews.length - topNewsSource.length),
    },
    system_prompt_vars: {
      llm_model: j.llm_model,
      leverage_max: brief.config.leverage_max,
    },
    user_prompt: `Use this compact AG1-FX-V1 briefing JSON. The full raw data remains available to downstream risk checks, so base your response on this compact decision pack only:\n\n\`\`\`json\n${llmBriefJson}\n\`\`\``,
  },
}];

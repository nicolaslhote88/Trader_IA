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

const MAX_LLM_NEWS = envNum('AG1_FX_LLM_TOP_NEWS_MAX', 6);
const MAX_LLM_WATCH = envNum('AG1_FX_LLM_MARKET_WATCH_MAX', 14);
const MAX_LLM_DRIVERS = envNum('AG1_FX_LLM_PAIR_DRIVERS_MAX', 2);

const universeRows = j.universe_fx || [];
const technicalRows = j.technical_signals || [];
const macroNews = j.macro_news || { top_news: [], pair_focus: {}, macro_regime: {} };
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
  limits: {
    max_pair_pct: Number(cfg.max_pair_pct || cfg.max_pos_pct || 0.20),
    max_currency_exposure_pct: Number(cfg.max_currency_exposure_pct || 0.50),
    max_daily_drawdown_pct: Number(cfg.max_daily_drawdown_pct || 0.05),
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
  return {
    lot_id: lot.lot_id,
    pair,
    side: lot.side,
    size_lots: rounded(lot.size_lots, 4),
    open_price: rounded(lot.open_price, num(meta.price_decimals, 5)),
    current_price: rounded(px, num(meta.price_decimals, 5)),
    pnl_pips: rounded(pnlPips, 1),
    pnl_eur: rounded(lot.unrealized_pnl_eur, 2),
    notional_eur: rounded(lot.notional_eur, 2),
    sl: lot.stop_loss_price == null ? null : rounded(lot.stop_loss_price, num(meta.price_decimals, 5)),
    tp: lot.take_profit_price == null ? null : rounded(lot.take_profit_price, num(meta.price_decimals, 5)),
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

const regime = macroNews.macro_regime || {};
const llmBrief = {
  run: brief.run,
  config: brief.config,
  limits: brief.limits,
  portfolio: {
    cash_eur: rounded(brief.portfolio_state.cash_eur, 2),
    equity_eur: rounded(brief.portfolio_state.equity_eur, 2),
    realized_pnl_eur: rounded(brief.portfolio_state.realized_pnl_eur, 2),
    floating_pnl_eur: rounded(brief.portfolio_state.floating_pnl_eur, 2),
    fees_eur: rounded(brief.portfolio_state.fees_eur, 2),
    leverage_effective: rounded(brief.portfolio_state.leverage_effective, 3),
    drawdown_day_pct: pct(brief.portfolio_state.drawdown_day_pct, 3),
    drawdown_total_pct: pct(brief.portfolio_state.drawdown_total_pct, 3),
    valuation_source: brief.portfolio_state.valuation_source || 'unknown',
    valuation_warnings: brief.portfolio_state.valuation_warnings || {},
    open_lots: openLots.map(compactLot),
  },
  universe_pairs: brief.universe.pairs,
  macro: {
    market_regime: regime.market_regime || 'Unknown',
    confidence: rounded(regime.confidence, 2),
    currency_biases: regime.biases || {},
    drivers: truncate(regime.drivers, 420),
    as_of: regime.as_of,
  },
  top_news: topNews,
  pair_matrix: pairMatrix,
  market_watch: marketWatch,
  briefing_notes: [
    'pair_matrix is a compact scan of all eligible pairs.',
    'market_watch contains the highest-priority/open pairs with extra technical and news context.',
    'Use only universe_pairs. Prefer no trade when macro and technicals conflict.',
  ],
};

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

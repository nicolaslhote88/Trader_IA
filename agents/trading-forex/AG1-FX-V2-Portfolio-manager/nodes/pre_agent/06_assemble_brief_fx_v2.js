// AG1-FX-V2 — Assemblage du Brief LLM (Framework 3 Piliers)
// Version enrichie du brief avec les scores macro, valorisation, positionnement + courbe des taux.
const j = $json;
const cfg = j.config || {};

function num(v, d = 0) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function rounded(v, d = 4) { const n = num(v, 0); return Math.round(n * 10**d) / 10**d; }
function pct(v) { return rounded(num(v, 0) * 100, 2); }
function truncate(v, max = 140) {
  const s = String(v || '').replace(/\s+/g, ' ').trim();
  return s.length > max ? `${s.slice(0, max - 1).trim()}...` : s;
}

const MAX_LLM_NEWS = Number($env.AG1_FX_LLM_TOP_NEWS_MAX || 6);
const MAX_LLM_WATCH = Number($env.AG1_FX_LLM_MARKET_WATCH_MAX || 12);

const universeRows = j.universe_fx || [];
const technicalRows = j.technical_signals || [];
const macroNews = j.macro_news || { top_news: [], pair_focus: {}, macro_regime: {} };
const openLots = (j.portfolio_state || {}).open_lots || [];
const openPairs = new Set(openLots.map(x => x.pair).filter(Boolean));
const threePillars = j.three_pillars || {};
const yieldCurves = j.yield_curves || {};

const metaByPair = Object.fromEntries(universeRows.map(x => [x.pair, x]));
const techByPair = Object.fromEntries(technicalRows.map(x => [x.pair, x]));
const byPillarCcy = threePillars.by_currency || {};

// Construire le signal composite par paire FX (3 piliers + technique)
function buildPairSignal(pair) {
  const base = pair.slice(0, 3);
  const quote = pair.slice(3);
  const tech = techByPair[pair] || {};

  const basePillar = byPillarCcy[base] || {};
  const quotePillar = byPillarCcy[quote] || {};

  // Signal pilier par paire = pilier_base - pilier_quote (direction relative)
  const macro_signal = (basePillar.macro_score || 0) - (quotePillar.macro_score || 0);
  const val_signal = (basePillar.valuation_score || 0) - (quotePillar.valuation_score || 0);
  const pos_signal = (basePillar.positioning_score || 0) - (quotePillar.positioning_score || 0);

  const THRESH = 0.20;
  const macro_aligned = Math.abs(macro_signal) >= THRESH;
  const val_aligned = Math.abs(val_signal) >= THRESH;
  const pos_aligned = Math.abs(pos_signal) >= THRESH;
  const all_aligned = macro_aligned && val_aligned && pos_aligned &&
    (macro_signal > 0) === (val_signal > 0) && (val_signal > 0) === (pos_signal > 0);

  // Crowded warning : si base crowded dans même direction que signaux → danger
  const base_crowded = basePillar.crowded_flag && (
    (basePillar.crowded_direction === 'long' && macro_signal > 0) ||
    (basePillar.crowded_direction === 'short' && macro_signal < 0)
  );

  return {
    pair,
    technical_score: rounded(tech.signal_score || 0),
    macro_signal: rounded(macro_signal, 2),
    valuation_signal: rounded(val_signal, 2),
    positioning_signal: rounded(pos_signal, 2),
    all_three_pillars_aligned: all_aligned,
    crowded_warning: base_crowded,
    tech_label: tech.signal_label || 'neutral',
    regime: tech.regime || 'unknown',
  };
}

const pairSignals = universeRows.map(x => buildPairSignal(x.pair));
const opportunitiesAligned = pairSignals.filter(p => p.all_three_pillars_aligned)
  .sort((a, b) => Math.abs(b.macro_signal) - Math.abs(a.macro_signal));

// Brief enrichi pour le LLM
const brief = {
  run: {
    run_id: j.run_id,
    as_of: j.as_of,
    llm_model: j.llm_model,
    framework: 'three_pillars_v2',
  },
  config: {
    capital_eur: num(cfg.initial_capital_eur || 10000),
    leverage_max: num(cfg.leverage_max || 2),
    volatility_target_pct: num(j.volatility_target_pct || 0.12),
    three_pillars_threshold: num(j.three_pillars_threshold || 0.20),
    kill_switch_active: Boolean(cfg.kill_switch_active),
  },
  portfolio_state: j.portfolio_state || {},
  universe: { pairs: universeRows.map(x => x.pair) },

  // Signaux techniques (AG2-FX, existant)
  technical_signals: technicalRows.slice(0, MAX_LLM_WATCH),

  // Macro news (AG4-FX, existant)
  macro_news: {
    regime: macroNews.macro_regime || {},
    top_news: (macroNews.top_news || []).slice(0, MAX_LLM_NEWS),
  },

  // *** NOUVEAU : Scores des 3 piliers par devise ***
  three_pillars: {
    as_of: threePillars.as_of || '',
    data_available: threePillars.data_available || false,
    by_currency: byPillarCcy,
    by_pair: Object.fromEntries(pairSignals.map(p => [p.pair, p])),
    opportunities: (threePillars.opportunities || []).slice(0, 5),
    crowded_alerts: threePillars.crowded_alerts || [],
  },

  // *** NOUVEAU : Courbe des taux ***
  yield_curves: {
    us: yieldCurves['USD'] || null,
    g10: yieldCurves,
    steepening_opportunities: Object.entries(yieldCurves)
      .filter(([_, v]) => v.rates_signal === 'steepener')
      .map(([ccy, v]) => ({ currency: ccy, slope: v.slope_10y2y, signal: 'steepener' })),
  },

  // *** NOUVEAU : Opportunités alignées (3 piliers) ***
  aligned_opportunities: opportunitiesAligned.map(p => ({
    pair: p.pair,
    direction: p.macro_signal > 0 ? 'buy_base' : 'sell_base',
    macro_signal: p.macro_signal,
    valuation_signal: p.valuation_signal,
    positioning_signal: p.positioning_signal,
  })),

  // Règles du framework (rappel pour le LLM)
  framework_rules: {
    open: 'OPEN uniquement si les 3 piliers (macro, valorisation, positionnement) pointent dans la même direction avec |score| > 0.20',
    hold: 'HOLD tant que les 3 piliers restent alignés — ignorer le bruit à court terme',
    exit: 'CLOSE si (1) l\'histoire macro change fondamentalement OU (2) le positionnement vire crowded (|COT z-score| > 1.5 dans la direction adverse)',
    reinforce: 'INCREASE si les prix baissent MAIS les 3 piliers restent intacts — approche contrarian (période de soldes)',
    avoid: 'NE PAS OUVRIR si crowded_warning = true pour cette paire',
    priority_thesis: 'USD faible vs. devises excédentaires (JPY, EUR, KRW) sur thèse fin de l\'exceptionnalisme américain',
  },
};

// LLM compact brief (pour limiter les tokens)
const llm_brief_compact = {
  ...brief,
  technical_signals: brief.technical_signals.map(t => ({
    pair: t.pair, score: t.signal_score, label: t.signal_label, rsi: t.rsi14,
  })),
};

return [{ json: { ...j, brief, llm_brief: llm_brief_compact, pair_signals: pairSignals } }];

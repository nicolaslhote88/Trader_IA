// AG1-FX-V2 — Assemblage du Brief LLM (Framework 3 Piliers)
// Version enrichie du brief avec les scores macro, valorisation, positionnement + courbe des taux.
const j = $json;
const cfg = j.config || {};

function num(v, d = 0) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function rounded(v, d = 4) { const n = num(v, 0); return Math.round(n * 10**d) / 10**d; }
function pct(v) { return rounded(num(v, 0) * 100, 2); }
function clamp(v, lo = -1, hi = 1) { return Math.max(lo, Math.min(hi, num(v, 0))); }
function sign(v, threshold = 0.20) {
  const n = num(v, 0);
  if (Math.abs(n) < threshold) return 0;
  return n > 0 ? 1 : -1;
}
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

function pairFocusFor(pair) {
  const focus = macroNews.pair_focus || {};
  return focus[pair] || focus[pair.slice(0, 3) + '/' + pair.slice(3)] || {};
}

function textBiasScore(value) {
  const s = String(value || '').toLowerCase();
  if (['bullish', 'buy_base', 'base_positive', 'positive', 'long'].some(x => s.includes(x))) return 0.45;
  if (['bearish', 'sell_base', 'base_negative', 'negative', 'short'].some(x => s.includes(x))) return -0.45;
  return 0;
}

function newsDirectionFromBias(pair, focus) {
  const p = String(pair || '').toUpperCase();
  const base = p.slice(0, 3).toLowerCase();
  const quote = p.slice(3, 6).toLowerCase();
  const bias = String(focus.bias_news || focus.bias || focus.direction || focus.sentiment || focus.action || '').toLowerCase();
  if (!bias || bias === 'unknown' || bias === 'mixed' || bias === 'neutral') return 'NEUTRAL';
  if (bias.includes('buy_base') || bias.includes('bullish_base') || bias.includes(`bullish_${base}`) || bias.includes(`bearish_${quote}`)) return 'BUY_BASE';
  if (bias.includes('sell_base') || bias.includes('bearish_base') || bias.includes(`bearish_${base}`) || bias.includes(`bullish_${quote}`)) return 'SELL_BASE';
  return 'NEUTRAL';
}

function newsEventAxis(pair) {
  const focus = pairFocusFor(pair);
  const candidates = [
    focus.news_score, focus.bias_score, focus.directional_score, focus.pair_score,
    focus.score, focus.impact_score,
  ];
  const rawScore = candidates.map(x => Number(x)).find(Number.isFinite);
  const direction = newsDirectionFromBias(pair, focus);
  let score = rawScore;
  if (direction === 'BUY_BASE') score = Math.abs(num(rawScore, 1));
  else if (direction === 'SELL_BASE') score = -Math.abs(num(rawScore, 1));
  if (!Number.isFinite(score)) {
    score = textBiasScore(focus.bias || focus.direction || focus.sentiment || focus.action || '');
  } else if (direction === 'NEUTRAL' && Math.abs(score) > 1) {
    score = 0;
  }
  const eventRisk = Math.max(
    num(focus.event_risk, 0),
    num(focus.event_risk_score, 0),
    num(focus.risk_score, 0),
    num(focus.urgency, 0)
  );
  return {
    score: clamp(score),
    direction,
    event_risk_score: Math.max(0, Math.min(1, eventRisk > 1 ? eventRisk / 100 : eventRisk)),
    summary: truncate(focus.summary || focus.reason || focus.rationale || focus.headline || '', 180),
  };
}

function cubeZone(xTechnical, yNews, zPillars, hasStructuralData) {
  if (!hasStructuralData) return 'structural_data_incomplete';
  const sx = sign(xTechnical);
  const sy = sign(yNews);
  const sz = sign(zPillars);
  if (sx && sy && sz && sx === sy && sy === sz) {
    return sz > 0 ? 'convergence_multi_horizon_long_base' : 'convergence_multi_horizon_short_base';
  }
  const shortTerm = sign((xTechnical + yNews) / 2);
  if (sz && shortTerm && shortTerm === -sz) return 'pullback_against_structural_z';
  if (sx && sy && sx === sy && sz && sx === -sz) return 'short_term_hype_against_pillars';
  if (sz && (!sx || !sy)) return 'structural_valid_short_term_neutral';
  return 'neutral_or_mixed';
}

function sideFromDirection(direction) {
  return direction > 0 ? 'buy_base' : direction < 0 ? 'sell_base' : 'watch';
}

function actionHint(pairSignal, hasOpenPosition) {
  if (pairSignal.crowded_warning) return 'AVOID_CROWDED';
  if (pairSignal.event_risk_score >= 0.75) return hasOpenPosition ? 'REDUCE_OR_WAIT_EVENT_RISK' : 'AVOID_EVENT_RISK';
  if (pairSignal.cube_zone.startsWith('convergence_multi_horizon')) return hasOpenPosition ? 'HOLD_OR_INCREASE' : 'OPEN_CANDIDATE';
  if (pairSignal.cube_zone === 'pullback_against_structural_z') return hasOpenPosition ? 'INCREASE_CANDIDATE' : 'WATCH_PULLBACK';
  if (pairSignal.cube_zone === 'short_term_hype_against_pillars') return hasOpenPosition ? 'REDUCE_CANDIDATE' : 'AVOID_CHASING';
  if (pairSignal.cube_zone === 'structural_valid_short_term_neutral') return hasOpenPosition ? 'HOLD' : 'WATCH';
  if (pairSignal.cube_zone === 'structural_data_incomplete') return 'NO_STRUCTURAL_TRADE';
  return 'WATCH';
}

// Construire le signal composite par paire FX (3 piliers + technique + news/event = cube)
function buildPairSignal(pair) {
  const base = pair.slice(0, 3);
  const quote = pair.slice(3);
  const tech = techByPair[pair] || {};
  const newsAxis = newsEventAxis(pair);
  const hasOpenPosition = openPairs.has(pair);

  const basePillar = byPillarCcy[base] || {};
  const quotePillar = byPillarCcy[quote] || {};
  const baseComplete = basePillar.score_status !== 'data_incomplete' && basePillar.composite_score !== null && basePillar.composite_score !== undefined;
  const quoteComplete = quotePillar.score_status !== 'data_incomplete' && quotePillar.composite_score !== null && quotePillar.composite_score !== undefined;
  const structuralComplete = Boolean(baseComplete && quoteComplete);

  // Signal pilier par paire = pilier_base - pilier_quote (direction relative)
  const macro_signal = (basePillar.macro_score || 0) - (quotePillar.macro_score || 0);
  const val_signal = (basePillar.valuation_score || 0) - (quotePillar.valuation_score || 0);
  const pos_signal = (basePillar.positioning_score || 0) - (quotePillar.positioning_score || 0);
  const z_three_pillars = structuralComplete ? (macro_signal + val_signal + pos_signal) / 3 : null;

  const THRESH = 0.20;
  const macro_aligned = Math.abs(macro_signal) >= THRESH;
  const val_aligned = Math.abs(val_signal) >= THRESH;
  const pos_aligned = Math.abs(pos_signal) >= THRESH;
  const all_aligned = structuralComplete && macro_aligned && val_aligned && pos_aligned &&
    (macro_signal > 0) === (val_signal > 0) && (val_signal > 0) === (pos_signal > 0);

  // Crowded warning : si base crowded dans même direction que signaux → danger
  const base_crowded = basePillar.crowded_flag && (
    (basePillar.crowded_direction === 'long' && macro_signal > 0) ||
    (basePillar.crowded_direction === 'short' && macro_signal < 0)
  );
  const xTechnical = clamp(tech.signal_score || 0);
  const yNews = newsAxis.score;
  const zPillars = z_three_pillars === null ? null : clamp(z_three_pillars);
  const zone = cubeZone(xTechnical, yNews, zPillars, structuralComplete);
  const direction = sideFromDirection(sign(zPillars === null ? (xTechnical + yNews) / 2 : zPillars));
  const cubeAlignment = structuralComplete ? (xTechnical + yNews + zPillars) / 3 : null;

  const out = {
    pair,
    technical_score: rounded(tech.signal_score || 0),
    x_technical: rounded(xTechnical, 3),
    y_news_event: rounded(yNews, 3),
    z_three_pillars: zPillars === null ? null : rounded(zPillars, 3),
    cube_alignment_score: cubeAlignment === null ? null : rounded(cubeAlignment, 3),
    cube_zone: zone,
    cube_direction: direction,
    event_risk_score: rounded(newsAxis.event_risk_score, 3),
    news_event_summary: newsAxis.summary,
    macro_signal: rounded(macro_signal, 2),
    valuation_signal: rounded(val_signal, 2),
    positioning_signal: rounded(pos_signal, 2),
    all_three_pillars_aligned: all_aligned,
    structural_data_complete: structuralComplete,
    structural_missing: structuralComplete ? [] : [base, quote].filter(ccy => {
      const p = byPillarCcy[ccy] || {};
      return p.score_status === 'data_incomplete' || p.composite_score === null || p.composite_score === undefined;
    }),
    crowded_warning: base_crowded,
    tech_label: tech.signal_label || 'neutral',
    regime: tech.regime || 'unknown',
  };
  out.portfolio_action_hint = actionHint(out, hasOpenPosition);
  return out;
}

const pairSignals = universeRows.map(x => buildPairSignal(x.pair));
const pairSignalByPair = Object.fromEntries(pairSignals.map(p => [p.pair, p]));
const opportunitiesAligned = pairSignals.filter(p => p.all_three_pillars_aligned)
  .sort((a, b) => Math.abs(b.macro_signal) - Math.abs(a.macro_signal));

const cubeSummary = {
  best_convergences: pairSignals
    .filter(p => p.cube_zone.startsWith('convergence_multi_horizon') && !p.crowded_warning && p.event_risk_score < 0.75)
    .sort((a, b) => Math.abs(b.cube_alignment_score || 0) - Math.abs(a.cube_alignment_score || 0))
    .slice(0, 10),
  portfolio_positions_review: openLots.map(lot => {
    const p = pairSignalByPair[lot.pair] || buildPairSignal(lot.pair || '');
    const sideSign = lot.side === 'sell_base' ? -1 : 1;
    const zSign = sign(p.z_three_pillars);
    return {
      pair: lot.pair,
      side: lot.side,
      cube_zone: p.cube_zone,
      z_three_pillars: p.z_three_pillars,
      event_risk_score: p.event_risk_score,
      crowded_warning: p.crowded_warning,
      portfolio_action_hint: zSign && zSign !== sideSign ? 'REDUCE_OR_CLOSE_Z_FLIPPED' : p.portfolio_action_hint,
    };
  }),
  pullback_reinforcement_candidates: pairSignals
    .filter(p => p.cube_zone === 'pullback_against_structural_z' && openPairs.has(p.pair) && !p.crowded_warning)
    .slice(0, 10),
  short_term_hype_to_avoid: pairSignals
    .filter(p => p.cube_zone === 'short_term_hype_against_pillars')
    .slice(0, 10),
  missing_structural_data: pairSignals
    .filter(p => !p.structural_data_complete)
    .map(p => ({ pair: p.pair, missing: p.structural_missing })),
};

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

  cube_summary: cubeSummary,

  // Règles du framework (rappel pour le LLM)
  framework_rules: {
    open: 'OPEN uniquement si cube_zone = convergence_multi_horizon_* dans le sens de l ordre, event risk acceptable, pas de crowded_warning',
    hold: 'HOLD tant que Z trois piliers reste dans le sens de la position, même si X technique ou Y news deviennent neutres',
    exit: 'CLOSE/DECREASE si Z trois piliers se retourne, si le positionnement devient crowded adverse, ou si event risk invalide le scénario',
    reinforce: 'INCREASE uniquement si Z trois piliers reste aligné avec la position et que X/Y offrent un pullback exploitable',
    avoid: 'NE PAS OUVRIR si crowded_warning = true pour cette paire',
    cube_required: 'Chaque décision doit citer cube_zone, X technique, Y news/event et Z trois piliers. Les paires structural_data_incomplete sont watchlist seulement.',
    priority_thesis: 'USD faible vs. devises excédentaires (JPY, EUR, KRW) sur thèse fin de l\'exceptionnalisme américain',
  },
};

// LLM compact brief (pour limiter les tokens)
const llm_brief_compact = {
  ...brief,
  technical_signals: brief.technical_signals.map(t => ({
    pair: t.pair, score: t.signal_score, label: t.signal_label, rsi: t.rsi14,
  })),
  cube_summary: {
    ...brief.cube_summary,
    best_convergences: brief.cube_summary.best_convergences.map(p => ({
      pair: p.pair, zone: p.cube_zone, direction: p.cube_direction,
      x: p.x_technical, y: p.y_news_event, z: p.z_three_pillars,
      event_risk: p.event_risk_score, action_hint: p.portfolio_action_hint,
    })),
  },
};

return [{ json: { ...j, brief, llm_brief: llm_brief_compact, pair_signals: pairSignals, cube_summary: cubeSummary } }];

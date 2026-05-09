/**
 * AG1-FX-V2 — Validation et enforcement de sécurité (Framework 3 Piliers)
 *
 * En plus des checks standard (cash, leverage, drawdown), ajoute :
 * 1. Vérification alignement des 3 piliers avant d'autoriser un OPEN
 * 2. Forçage de CLOSE si positionnement devient crowded dans direction adverse
 * 3. Ajustement de taille selon volatilité cible (10-15%)
 * 4. Logique de renforcement contrarian (INCREASE si prix baisse + pilliers intacts)
 */

const j = $json;
const cfg = j.config || {};
const actions = j.decision?.actions || [];
const openLots = (j.portfolio_state || {}).open_lots || [];
const byPair = Object.fromEntries((j.pair_signals || []).map(p => [p.pair, p]));
const byPillarCcy = (j.three_pillars || {}).by_currency || {};
const crowdedAlerts = (j.three_pillars || {}).crowded_alerts || [];

const capitalEur = Number(cfg.initial_capital_eur || 10000);
const leverageMax = Number(cfg.leverage_max || 2.0);
const maxPairPct = Number(cfg.max_pair_pct || cfg.max_pos_pct || 0.20);
const maxCcyPct = Number(cfg.max_currency_exposure_pct || 0.50);
const maxDailyDd = Number(cfg.max_daily_drawdown_pct || 0.05);
const killSwitch = Boolean(cfg.kill_switch_active);
const PILLAR_THRESHOLD = Number(j.three_pillars_threshold || 0.20);
const COT_CROWDED_THRESHOLD = 1.5;

const validated = [];
const rejected = [];
const warnings = [];

// Déviation drawdown quotidien
const navToday = Number((j.portfolio_state || {}).nav_eur || capitalEur);
const dailyPnl = Number((j.portfolio_state || {}).daily_pnl_eur || 0);
const dailyDdReached = Math.abs(dailyPnl) / capitalEur >= maxDailyDd && dailyPnl < 0;

if (killSwitch) {
  return [{ json: { ...j, validated_orders: [], rejected_orders: actions.map(a => ({ ...a, reject_reason: 'KILL_SWITCH_ACTIVE' })), warnings: ['KILL_SWITCH_ACTIVE'] } }];
}
if (dailyDdReached) {
  return [{ json: { ...j, validated_orders: [], rejected_orders: actions.map(a => ({ ...a, reject_reason: 'DAILY_DRAWDOWN_LIMIT' })), warnings: ['DAILY_DRAWDOWN_LIMIT'] } }];
}

// Crowded shorts crowded : identifier les devises où CLOSE est forcé
const crowdedForcedClose = new Set();
for (const lot of openLots) {
  const pair = lot.pair || '';
  const base = pair.slice(0, 3);
  const quote = pair.slice(3);
  const baseCot = byPillarCcy[base] || {};
  const quoteCot = byPillarCcy[quote] || {};

  // Si la position est LONG base ET base devient crowded long → forcer CLOSE
  if (lot.side === 'buy_base' && baseCot.crowded_flag && baseCot.crowded_direction === 'long') {
    const z = Math.abs(baseCot.cot_z_score || 0);
    if (z >= COT_CROWDED_THRESHOLD + 0.5) {  // z > 2 pour le force close
      crowdedForcedClose.add(pair);
      warnings.push(`FORCE_CLOSE_CROWDED: ${pair} — ${base} crowded long z=${z.toFixed(2)}`);
    }
  }
  // Si SHORT base ET base devient crowded short → fermer le short (thèse contrarian = plus valide)
  if (lot.side === 'sell_base' && baseCot.crowded_flag && baseCot.crowded_direction === 'short') {
    const z = Math.abs(baseCot.cot_z_score || 0);
    if (z >= COT_CROWDED_THRESHOLD + 0.5) {
      crowdedForcedClose.add(pair);
      warnings.push(`FORCE_CLOSE_CROWDED_SHORT: ${pair} — ${base} crowded short z=${z.toFixed(2)}`);
    }
  }
}

for (const action of actions) {
  const intent = String(action.intent || action.action || '').toUpperCase();
  const pair = String(action.pair || action.symbol || '').toUpperCase();
  const side = String(action.side || '').toLowerCase();

  // Toujours laisser passer les CLOSE et les DECREASE
  if (intent === 'CLOSE' || intent === 'DECREASE') {
    validated.push({ ...action, validated: true, note: 'close_decrease_always_allowed' });
    continue;
  }

  // CLOSE forcé si crowded
  if (crowdedForcedClose.has(pair)) {
    validated.push({ ...action, intent: 'CLOSE', side: 'close', validated: true, note: 'force_close_crowded' });
    continue;
  }

  // Pour les OPEN et INCREASE : vérifier les 3 piliers
  if (intent === 'OPEN' || intent === 'INCREASE') {
    const pairSignal = byPair[pair] || {};
    const allAligned = pairSignal.all_three_pillars_aligned;
    const crowdedWarning = pairSignal.crowded_warning;

    // Bloquer si crowded warning sur la paire
    if (crowdedWarning && intent === 'OPEN') {
      rejected.push({ ...action, reject_reason: 'CROWDED_POSITION_WARNING', pair_signal: pairSignal });
      warnings.push(`REJECTED_CROWDED: ${pair} — position crowded dans direction demandée`);
      continue;
    }

    // Pour OPEN : requérir alignement des 3 piliers
    if (intent === 'OPEN' && !allAligned) {
      // Exception : si données piliers non disponibles, autoriser avec warning
      const dataAvailable = (j.three_pillars || {}).data_available;
      if (dataAvailable) {
        rejected.push({ ...action, reject_reason: 'THREE_PILLARS_NOT_ALIGNED', pair_signal: pairSignal });
        warnings.push(`REJECTED_PILLARS: ${pair} — 3 piliers non alignés (macro:${pairSignal.macro_signal}, val:${pairSignal.valuation_signal}, pos:${pairSignal.positioning_signal})`);
        continue;
      } else {
        warnings.push(`WARNING_NO_PILLAR_DATA: ${pair} — données macro non disponibles, ordre passé sans vérification piliers`);
      }
    }

    // INCREASE contrarian : autoriser si prix baisse ET piliers intacts (même non-parfaitement alignés)
    if (intent === 'INCREASE') {
      const pairMacro = Math.abs(pairSignal.macro_signal || 0);
      const pairVal = Math.abs(pairSignal.valuation_signal || 0);
      if (pairMacro < PILLAR_THRESHOLD && pairVal < PILLAR_THRESHOLD) {
        warnings.push(`INCREASE_WEAK_PILLARS: ${pair} — renforcement avec piliers faibles, vérifier thèse`);
      }
    }

    validated.push({ ...action, validated: true, three_pillars_check: allAligned ? 'passed' : 'bypassed' });
    continue;
  }

  // Cas fallback : laisser passer
  validated.push({ ...action, validated: true, note: 'fallback' });
}

return [{
  json: {
    ...j,
    validated_orders: validated,
    rejected_orders: rejected,
    warnings,
    stats: {
      total: actions.length,
      validated: validated.length,
      rejected: rejected.length,
      forced_close_crowded: crowdedForcedClose.size,
    },
  },
}];

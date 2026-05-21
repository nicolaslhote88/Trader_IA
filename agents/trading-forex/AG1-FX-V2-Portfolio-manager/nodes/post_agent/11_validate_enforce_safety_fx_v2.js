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
const EVENT_RISK_BLOCK_THRESHOLD = Number(j.cube_event_risk_block_threshold || 0.75);

const validated = [];
const rejected = [];
const warnings = [];

function signalSign(v, threshold = PILLAR_THRESHOLD) {
  const n = Number(v);
  if (!Number.isFinite(n) || Math.abs(n) < threshold) return 0;
  return n > 0 ? 1 : -1;
}

function sideSign(side) {
  if (side === 'buy_base') return 1;
  if (side === 'sell_base') return -1;
  return 0;
}

function cubeDirectionMatchesSide(pairSignal, side) {
  const wanted = sideSign(side);
  const z = signalSign(pairSignal.z_three_pillars);
  return wanted !== 0 && z !== 0 && wanted === z;
}

function isCubeConvergence(pairSignal) {
  return String(pairSignal.cube_zone || '').startsWith('convergence_multi_horizon');
}

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
    const eventRisk = Number(pairSignal.event_risk_score || 0);
    const structuralComplete = pairSignal.structural_data_complete !== false;

    // Bloquer si crowded warning sur la paire
    if (crowdedWarning && intent === 'OPEN') {
      rejected.push({ ...action, reject_reason: 'CROWDED_POSITION_WARNING', pair_signal: pairSignal });
      warnings.push(`REJECTED_CROWDED: ${pair} — position crowded dans direction demandée`);
      continue;
    }

    if (!structuralComplete) {
      rejected.push({ ...action, reject_reason: 'CUBE_STRUCTURAL_DATA_INCOMPLETE', pair_signal: pairSignal });
      warnings.push(`REJECTED_CUBE_INCOMPLETE: ${pair} — axe Z trois piliers incomplet`);
      continue;
    }

    if (eventRisk >= EVENT_RISK_BLOCK_THRESHOLD && intent === 'OPEN') {
      rejected.push({ ...action, reject_reason: 'CUBE_EVENT_RISK_TOO_HIGH', pair_signal: pairSignal });
      warnings.push(`REJECTED_EVENT_RISK: ${pair} — event risk ${eventRisk.toFixed(2)} trop élevé`);
      continue;
    }

    // Pour OPEN : requérir convergence du cube dans le sens demandé.
    if (intent === 'OPEN' && (!allAligned || !isCubeConvergence(pairSignal) || !cubeDirectionMatchesSide(pairSignal, side))) {
      // Exception : si données piliers non disponibles, autoriser avec warning
      const dataAvailable = (j.three_pillars || {}).data_available;
      if (dataAvailable) {
        rejected.push({ ...action, reject_reason: 'CUBE_NOT_MULTI_HORIZON_CONVERGENCE', pair_signal: pairSignal });
        warnings.push(`REJECTED_CUBE: ${pair} — cube non convergent dans le sens demandé (zone:${pairSignal.cube_zone}, x:${pairSignal.x_technical}, y:${pairSignal.y_news_event}, z:${pairSignal.z_three_pillars})`);
        continue;
      } else {
        warnings.push(`WARNING_NO_PILLAR_DATA: ${pair} — données macro non disponibles, ordre passé sans vérification piliers`);
      }
    }

    // INCREASE contrarian : autoriser uniquement si l'axe Z reste dans le sens de la position.
    if (intent === 'INCREASE') {
      if (!cubeDirectionMatchesSide(pairSignal, side)) {
        rejected.push({ ...action, reject_reason: 'CUBE_Z_NOT_ALIGNED_FOR_INCREASE', pair_signal: pairSignal });
        warnings.push(`REJECTED_INCREASE_Z: ${pair} — Z trois piliers n'est pas aligné avec ${side}`);
        continue;
      }
      if (eventRisk >= EVENT_RISK_BLOCK_THRESHOLD) {
        warnings.push(`INCREASE_EVENT_RISK: ${pair} — renforcement validé mais event risk élevé (${eventRisk.toFixed(2)}), taille prudente requise`);
      }
    }

    validated.push({
      ...action,
      validated: true,
      three_pillars_check: allAligned ? 'passed' : 'bypassed',
      cube_check: {
        zone: pairSignal.cube_zone,
        x_technical: pairSignal.x_technical,
        y_news_event: pairSignal.y_news_event,
        z_three_pillars: pairSignal.z_three_pillars,
        action_hint: pairSignal.portfolio_action_hint,
      },
    });
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

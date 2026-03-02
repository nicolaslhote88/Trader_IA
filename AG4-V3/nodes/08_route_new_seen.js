// 20G2 — Route new vs seen + stale re-analysis + event clustering (V2)
function toNum(x, def = 0) {
  const n = Number(x);
  return Number.isFinite(n) ? n : def;
}

function hoursBetween(a, b) {
  const da = new Date(a), db = new Date(b);
  if (isNaN(da.getTime()) || isNaN(db.getTime())) return 9999;
  return Math.abs((da.getTime() - db.getTime()) / 3600000);
}

const n = $json || {};
const nowIso = new Date().toISOString();

let historyIndex = {};
let historyEventIndex = {};
try {
  const idxNode = $items('20G1B - Build History Index')[0]?.json || {};
  historyIndex = idxNode.historyIndex || {};
  historyEventIndex = idxNode.historyEventIndex || {};
} catch {
  historyIndex = {};
  historyEventIndex = {};
}

const dKey = n.dedupeKey;
const eKey = n.eventKey;
const old = (dKey && historyIndex[dKey]) ? historyIndex[dKey] : null;
const oldEvent = (!old && eKey && historyEventIndex[eKey]) ? historyEventIndex[eKey] : null;

const REANALYZE_HOURS = 12;

if (old) {
  const age = hoursBetween(nowIso, old.firstSeenAt || old.publishedAt || old.analyzedAt || nowIso);
  const preImpact = toNum(n.preImpactScore, 0);

  if (preImpact >= 7 && age >= REANALYZE_HOURS) {
    return [{ json: { ...n, _action: 'analyze', _reason: 'stale_high_impact_refresh', seenNowAt: nowIso } }];
  }

  return [{
    json: {
      ...n,
      _action: 'skip',
      _reason: 'duplicate_known',
      seenNowAt: nowIso,
      historyRowNumber: old.row_number ?? old.rowNumber ?? old.row ?? null,

      // restore previously analyzed strategic fields
      ImpactScore: old.ImpactScore ?? old.impactScore ?? 0,
      confidence: old.confidence ?? 0,
      urgency: old.urgency ?? 'low',
      notes: old.notes ?? '',
      Snippet: old.Snippet ?? old.snippet ?? n.snippet ?? '',
      symbols: old.symbols ?? (Array.isArray(n.symbols) ? n.symbols.join(', ') : ''),
      type: old.type ?? n.type ?? 'macro',
      Regime: old.Regime ?? old.market_regime ?? 'Neutral',
      Theme: old.Theme ?? old.macro_theme ?? 'Résultats/Micro',
      sectors_bullish: old.sectors_bullish ?? old.Winners ?? '',
      sectors_bearish: old.sectors_bearish ?? old.Losers ?? '',
      currencies_bullish: old.currencies_bullish ?? '',
      currencies_bearish: old.currencies_bearish ?? '',
      Winners: old.Winners ?? old.sectors_bullish ?? '',
      Losers: old.Losers ?? old.sectors_bearish ?? '',
      Strategy: old.Strategy ?? old.strategic_summary ?? '',
      firstSeenAt: old.firstSeenAt ?? old.analyzedAt ?? old.publishedAt ?? n.publishedAt,
      analyzedAt: old.analyzedAt ?? old.firstSeenAt ?? nowIso,
    }
  }];
}

if (oldEvent && !n.preAnalyzeHint) {
  return [{
    json: {
      ...n,
      _action: 'skip',
      _reason: 'event_cluster_duplicate',
      seenNowAt: nowIso,
      ImpactScore: oldEvent.ImpactScore ?? oldEvent.impactScore ?? 0,
      confidence: oldEvent.confidence ?? 0,
      urgency: oldEvent.urgency ?? 'low',
      notes: `Cluster duplicate of prior eventKey ${eKey}`,
      Snippet: n.snippet || oldEvent.Snippet || '',
      symbols: Array.isArray(n.symbols) ? n.symbols.join(', ') : (n.symbols || ''),
      type: n.type || oldEvent.type || 'macro',
      Regime: oldEvent.Regime ?? 'Neutral',
      Theme: oldEvent.Theme ?? 'Résultats/Micro',
      sectors_bullish: oldEvent.sectors_bullish ?? oldEvent.Winners ?? '',
      sectors_bearish: oldEvent.sectors_bearish ?? oldEvent.Losers ?? '',
      currencies_bullish: oldEvent.currencies_bullish ?? '',
      currencies_bearish: oldEvent.currencies_bearish ?? '',
      Winners: oldEvent.Winners ?? oldEvent.sectors_bullish ?? '',
      Losers: oldEvent.Losers ?? oldEvent.sectors_bearish ?? '',
      Strategy: oldEvent.Strategy ?? '',
      firstSeenAt: oldEvent.firstSeenAt ?? oldEvent.analyzedAt ?? nowIso,
      analyzedAt: oldEvent.analyzedAt ?? nowIso,
    }
  }];
}

return [{
  json: {
    ...n,
    _action: 'analyze',
    _reason: 'new_or_material',
    seenNowAt: nowIso,
  }
}];

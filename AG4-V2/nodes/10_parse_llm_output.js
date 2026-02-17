// 20H2 - Parse model output + flatten final row (V2)
function safeJsonParse(str) {
  try {
    if (typeof str === 'object' && str !== null) return str;
    return JSON.parse(str);
  } catch {
    return {};
  }
}

function clamp01(n, d = 0.5) {
  const v = Number(n);
  if (!Number.isFinite(v)) return d;
  return Math.max(0, Math.min(1, v));
}

function clamp10(n, d = 0) {
  const v = Number(n);
  if (!Number.isFinite(v)) return d;
  return Math.max(0, Math.min(10, Math.round(v)));
}

function joinList(arr) {
  if (Array.isArray(arr)) return arr.join(', ');
  return arr || '';
}

function toBool(v, d = true) {
  if (typeof v === 'boolean') return v;
  if (typeof v === 'number') return v !== 0;
  if (typeof v === 'string') {
    const s = v.trim().toLowerCase();
    if (['true', '1', 'yes', 'y'].includes(s)) return true;
    if (['false', '0', 'no', 'n'].includes(s)) return false;
  }
  return d;
}

function normalizeSectorList(v) {
  const raw = Array.isArray(v) ? v : (typeof v === 'string' ? v.split(',') : []);
  return raw
    .map((x) => String(x || '').trim())
    .filter(Boolean)
    .slice(0, 5);
}

const j = $json || {};
const llmRaw = j.output?.[0]?.content?.[0]?.text || j.content || '{}';
const ai = safeJsonParse(llmRaw);
const now = new Date().toISOString();

const symbols = Array.isArray(j.symbols) ? j.symbols.join(', ') : (j.symbols || '');
const isActionable = toBool(ai.isActionable, true);
const winners = normalizeSectorList(ai.sectors_bullish);
const losers = normalizeSectorList(ai.sectors_bearish);
const notes = isActionable ? (ai.notes || '') : 'Noise';

return [{
  json: {
    run_id: j.run_id || '',
    sourceTier: j.sourceTier ?? null,
    sourceId: j.sourceId || '',
    dedupeKey: j.dedupeKey,
    eventKey: j.eventKey || '',
    canonicalUrl: j.canonicalUrlNormalized || j.canonicalUrl || j.url || 'unknown',
    publishedAt: j.publishedAtNormalized || j.publishedAt || now,
    title: j.title || 'unknown',
    source: j.source || 'unknown',
    feedUrl: j.feedUrl || '',
    symbols,
    type: j.type || (symbols ? 'symbol' : 'macro'),
    notes,
    isActionable,
    ImpactScore: isActionable ? clamp10(ai.impact_score, j.preImpactScore ?? 0) : 0,
    confidence: clamp01(ai.confidence, 0.5),
    urgency: isActionable ? (ai.urgency || j.preUrgency || 'low') : 'low',
    Snippet: j.snippet || '',
    firstSeenAt: j.seenNowAt || now,
    Strategy: ai.strategic_summary || '',
    Losers: isActionable ? joinList(losers) : '',
    Winners: isActionable ? joinList(winners) : '',
    Theme: isActionable ? (ai.macro_theme || 'Resultats/Micro') : 'Resultats/Micro',
    Regime: isActionable ? (ai.market_regime || 'Neutral') : 'Neutral',
    analyzedAt: now,
    _action: 'analyze',
    _reason: j._reason || '',
  }
}];

// 20H2 - Parse model output + flatten final row (V2)
function safeJsonParse(str) {
  try {
    if (typeof str === 'object' && str !== null) return str;
    return JSON.parse(str);
  } catch {
    return {};
  }
}

function toArray(v) {
  if (Array.isArray(v)) return v;
  if (typeof v === 'string') {
    return v.split(',').map((x) => String(x || '').trim()).filter(Boolean);
  }
  return [];
}

function normalizeKey(s) {
  return String(s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
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

function buildAllowedSectors(rawList) {
  const out = [];
  const seen = new Set();
  for (const v of toArray(rawList)) {
    const label = String(v || '').trim();
    if (!label) continue;
    const key = normalizeKey(label);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({ key, label });
  }
  return out;
}

function normalizeSectorList(raw, allowed) {
  const out = [];
  const seen = new Set();

  for (const value of toArray(raw)) {
    const key = normalizeKey(value);
    if (!key) continue;

    let match = allowed.find((s) => s.key === key);
    if (!match) {
      match = allowed.find((s) => key.includes(s.key) || s.key.includes(key));
    }

    if (!match) continue;
    if (seen.has(match.key)) continue;
    seen.add(match.key);
    out.push(match.label);

    if (out.length >= 5) break;
  }

  return out;
}

const j = $json || {};
const llmRaw = j.output?.[0]?.content?.[0]?.text || j.content || '{}';
const ai = safeJsonParse(llmRaw);
const now = new Date().toISOString();

const allowedSectors = buildAllowedSectors(j.universeSectors);
const winners = normalizeSectorList(ai.sectors_bullish, allowedSectors);
const losers = normalizeSectorList(ai.sectors_bearish, allowedSectors);
const winnersText = winners.join(', ');
const losersText = losers.join(', ');

const modelActionable = toBool(ai.isActionable, true);
const hasSectorImpact = winners.length > 0 || losers.length > 0;
const isActionable = modelActionable && hasSectorImpact;
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
    symbols: '',
    type: 'macro',
    notes,
    isActionable,
    ImpactScore: isActionable ? clamp10(ai.impact_score, j.preImpactScore ?? 0) : 0,
    confidence: clamp01(ai.confidence, 0.5),
    urgency: isActionable ? (ai.urgency || j.preUrgency || 'low') : 'low',
    Snippet: j.snippet || '',
    firstSeenAt: j.seenNowAt || now,
    Strategy: ai.strategic_summary || '',
    sectors_bearish: isActionable ? losersText : '',
    sectors_bullish: isActionable ? winnersText : '',
    Losers: isActionable ? losersText : '',
    Winners: isActionable ? winnersText : '',
    Theme: isActionable ? (ai.macro_theme || 'Resultats/Micro') : 'Resultats/Micro',
    Regime: isActionable ? (ai.market_regime || 'Neutral') : 'Neutral',
    analyzedAt: now,
    _action: 'analyze',
    _reason: j._reason || '',
  },
}];

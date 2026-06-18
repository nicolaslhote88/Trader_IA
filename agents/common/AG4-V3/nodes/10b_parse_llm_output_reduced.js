// 20H2R - Parse Grok output (branche REDUITE actions) + flatten final row
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
    if (!match) match = allowed.find((s) => key.includes(s.key) || s.key.includes(key));
    if (!match) continue;
    if (seen.has(match.key)) continue;
    seen.add(match.key);
    out.push(match.label);
    if (out.length >= 5) break;
  }
  return out;
}

const ALLOWED_MAG = new Set(['Low', 'Medium', 'High']);

function sanitizeMagnitude(raw) {
  const mag = String(raw || '').trim();
  return ALLOWED_MAG.has(mag) ? mag : 'Low';
}

// --- P0 calibration : borne le score par magnitude + confiance ---
function calibrateImpact(score, magnitude, confidence) {
  let cap = 10;
  if (magnitude === 'Low') cap = 3;
  else if (magnitude === 'Medium') cap = 6;
  let s = Math.min(clamp10(score, 0), cap);
  if (confidence < 0.5 && s > 6) s = 6;
  return s;
}

function deriveUrgency(score, magnitude) {
  if (magnitude === 'High' || score >= 8) return 'immediate';
  if (score >= 6) return 'today';
  if (score >= 4) return 'this_week';
  return 'low';
}

function deriveSource(existing, url) {
  const cur = String(existing || '').trim();
  if (cur && cur.toLowerCase() !== 'unknown') return cur;
  try {
    const h = new URL(String(url || '')).hostname.toLowerCase().replace(/^www\./, '');
    if (!h) return 'unknown';
    if (h.includes('boursorama')) return 'Boursorama';
    if (h.includes('investir') || h.includes('lesechos')) return 'Investir/Les Echos';
    if (h.includes('reuters')) return 'Reuters';
    if (h.includes('bloomberg')) return 'Bloomberg';
    if (h.includes('zonebourse')) return 'Zonebourse';
    if (h.includes('amf-france')) return 'AMF France';
    return h;
  } catch {
    return cur || 'unknown';
  }
}

const j = $json || {};
// Reponse Grok (chat.completions OpenAI-compatible) ou contenu deja extrait.
const llmRaw = j.choices?.[0]?.message?.content
  || j.output?.[0]?.content?.[0]?.text
  || j.content
  || '{}';
const ai = safeJsonParse(llmRaw);
const now = new Date().toISOString();

const allowedSectors = buildAllowedSectors(j.universeSectors);
const winners = normalizeSectorList(ai.sectors_bullish, allowedSectors);
const losers = normalizeSectorList(ai.sectors_bearish, allowedSectors);
const winnersText = winners.join(', ');
const losersText = losers.join(', ');

const confidence = clamp01(ai.confidence, 0.5);
const impactMagnitude = sanitizeMagnitude(ai.impact_magnitude);

const modelActionable = toBool(ai.isActionable, true);
const hasSectorImpact = winners.length > 0 || losers.length > 0;
const isActionable = modelActionable && hasSectorImpact;

const rawScore = calibrateImpact(ai.impact_score, impactMagnitude, confidence);
const impactScore = isActionable ? rawScore : 0;
const notes = isActionable ? (ai.notes || '') : 'Noise';
const urgency = isActionable ? deriveUrgency(impactScore, impactMagnitude) : 'low';
const canonicalUrl = j.canonicalUrlNormalized || j.canonicalUrl || j.url || 'unknown';

return [{
  json: {
    run_id: j.run_id || '',
    sourceTier: j.sourceTier ?? null,
    sourceId: j.sourceId || '',
    dedupeKey: j.dedupeKey,
    eventKey: j.eventKey || '',
    canonicalUrl,
    publishedAt: j.publishedAtNormalized || j.publishedAt || now,
    title: j.title || 'unknown',
    source: deriveSource(j.source, canonicalUrl),
    feedUrl: j.feedUrl || '',
    symbols: '',
    type: 'macro',
    notes,
    isActionable,
    ImpactScore: impactScore,
    confidence,
    urgency,
    Snippet: j.snippet || '',
    firstSeenAt: j.seenNowAt || now,
    Strategy: '',
    sectors_bearish: isActionable ? losersText : '',
    sectors_bullish: isActionable ? winnersText : '',
    currencies_bearish: '',
    currencies_bullish: '',
    Losers: isActionable ? losersText : '',
    Winners: isActionable ? winnersText : '',
    Theme: isActionable ? (ai.macro_theme || 'Resultats/Micro') : 'Resultats/Micro',
    Regime: isActionable ? (ai.market_regime || 'Neutral') : 'Neutral',
    impact_region: '',
    impact_asset_class: isActionable ? 'Equity' : 'None',
    impact_magnitude: impactMagnitude,
    impact_fx_pairs: '',
    tagger_version: 'reduced_grok_v1',
    _taxonomyViolations: [],
    analyzedAt: now,
    _action: 'analyze',
    _reason: j._reason || '',
    analysisMode: 'reduced',
  },
}];

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

const ALLOWED_CURRENCIES = new Set(['USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD', 'CHF', 'NZD']);
const ALLOWED_REGIONS = new Set(['Global', 'US', 'EU', 'France', 'UK', 'APAC', 'Emerging', 'Other']);
const ALLOWED_CLASSES = new Set(['Equity', 'FX', 'Commodity', 'Bond', 'Crypto', 'Mixed', 'None']);
const ALLOWED_MAG = new Set(['Low', 'Medium', 'High']);
const ALLOWED_PAIRS = new Set([
  'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'NZDUSD', 'USDCAD',
  'EURGBP', 'EURJPY', 'EURCHF', 'EURAUD', 'EURCAD', 'EURNZD',
  'GBPJPY', 'GBPCHF', 'GBPAUD', 'GBPCAD',
  'AUDJPY', 'AUDNZD', 'AUDCAD',
  'NZDJPY', 'NZDCAD',
  'CADJPY', 'CHFJPY', 'CADCHF',
  'CHFCAD', 'JPYNZD',
]);

function normalizeCurrencyList(raw) {
  const out = [];
  const seen = new Set();
  for (const value of toArray(raw)) {
    const ccy = String(value || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 3);
    if (ccy.length !== 3) continue;
    if (!ALLOWED_CURRENCIES.has(ccy)) continue;
    if (seen.has(ccy)) continue;
    seen.add(ccy);
    out.push(ccy);
    if (out.length >= 5) break;
  }
  return out;
}

function sanitizeCsv(raw, allowed, fallback, violations, fieldName) {
  const parts = toArray(raw).map((x) => String(x || '').trim()).filter(Boolean);
  if (parts.length === 0) return fallback;

  const kept = [];
  const seen = new Set();
  for (const part of parts) {
    if (!allowed.has(part)) {
      violations.push(`${fieldName}:${part}`);
      continue;
    }
    if (seen.has(part)) continue;
    seen.add(part);
    kept.push(part);
  }
  if (kept.length === 0) return fallback;
  return kept.join(', ');
}

function sanitizeMagnitude(raw, violations) {
  const mag = String(raw || '').trim();
  if (ALLOWED_MAG.has(mag)) return mag;
  if (mag) violations.push(`impact_magnitude:${mag}`);
  return 'Low';
}

function ensureFxPairs(assetClass, rawPairs, ccyBullish, ccyBearish, violations) {
  const classes = toArray(assetClass);
  const needsFxPairs = classes.includes('FX') || classes.includes('Mixed');
  const pairs = sanitizeCsv(rawPairs, ALLOWED_PAIRS, '', violations, 'impact_fx_pairs');
  if (!needsFxPairs) {
    if (pairs) violations.push('impact_fx_pairs:present_without_fx_or_mixed');
    return '';
  }
  if (pairs) return pairs;

  const currencies = [...new Set([...(ccyBullish || []), ...(ccyBearish || [])])];
  const derived = [];
  const add = (p) => {
    if (ALLOWED_PAIRS.has(p) && !derived.includes(p)) derived.push(p);
  };
  for (const ccy of currencies) {
    if (ccy === 'USD') { add('EURUSD'); add('USDJPY'); add('USDCHF'); }
    if (ccy === 'EUR') { add('EURUSD'); add('EURGBP'); add('EURJPY'); }
    if (ccy === 'GBP') { add('GBPUSD'); add('EURGBP'); add('GBPJPY'); }
    if (ccy === 'JPY') { add('USDJPY'); add('EURJPY'); add('GBPJPY'); }
    if (ccy === 'CHF') { add('USDCHF'); add('EURCHF'); add('CHFJPY'); }
    if (ccy === 'AUD') { add('AUDUSD'); add('AUDJPY'); add('EURAUD'); }
    if (ccy === 'CAD') { add('USDCAD'); add('CADJPY'); add('EURCAD'); }
    if (ccy === 'NZD') { add('NZDUSD'); add('NZDJPY'); add('EURNZD'); }
  }
  if (derived.length === 0) add('EURUSD');
  violations.push('impact_fx_pairs:missing_derived');
  return derived.slice(0, 5).join(', ');
}

function normalizeUrgencyForMagnitude(urgency, magnitude) {
  const u = String(urgency || 'low').trim() || 'low';
  if (magnitude !== 'High') return u;
  return ['immediate', 'today'].includes(u) ? u : 'today';
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
const currenciesBullish = normalizeCurrencyList(ai.currencies_bullish);
const currenciesBearish = normalizeCurrencyList(ai.currencies_bearish);
const currenciesBullishText = currenciesBullish.join(', ');
const currenciesBearishText = currenciesBearish.join(', ');
const taxonomyViolations = [];
const impactRegion = sanitizeCsv(ai.impact_region, ALLOWED_REGIONS, 'Other', taxonomyViolations, 'impact_region');
const impactAssetClass = sanitizeCsv(ai.impact_asset_class, ALLOWED_CLASSES, 'None', taxonomyViolations, 'impact_asset_class');
const impactMagnitude = sanitizeMagnitude(ai.impact_magnitude, taxonomyViolations);
const impactFxPairs = ensureFxPairs(impactAssetClass, ai.impact_fx_pairs, currenciesBullish, currenciesBearish, taxonomyViolations);

const modelActionable = toBool(ai.isActionable, true);
const hasSectorImpact = winners.length > 0 || losers.length > 0;
const hasCurrencyImpact = currenciesBullish.length > 0 || currenciesBearish.length > 0;
const isActionable = modelActionable && (hasSectorImpact || hasCurrencyImpact);
const notes = isActionable ? (ai.notes || '') : 'Noise';
const urgency = normalizeUrgencyForMagnitude(isActionable ? (ai.urgency || j.preUrgency || 'low') : 'low', impactMagnitude);

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
    urgency,
    Snippet: j.snippet || '',
    firstSeenAt: j.seenNowAt || now,
    Strategy: ai.strategic_summary || '',
    sectors_bearish: isActionable ? losersText : '',
    sectors_bullish: isActionable ? winnersText : '',
    currencies_bearish: isActionable ? currenciesBearishText : '',
    currencies_bullish: isActionable ? currenciesBullishText : '',
    Losers: isActionable ? losersText : '',
    Winners: isActionable ? winnersText : '',
    Theme: isActionable ? (ai.macro_theme || 'Resultats/Micro') : 'Resultats/Micro',
    Regime: isActionable ? (ai.market_regime || 'Neutral') : 'Neutral',
    impact_region: impactRegion,
    impact_asset_class: impactAssetClass,
    impact_magnitude: impactMagnitude,
    impact_fx_pairs: impactFxPairs,
    tagger_version: 'geo_v1',
    _taxonomyViolations: taxonomyViolations,
    analyzedAt: now,
    _action: 'analyze',
    _reason: j._reason || '',
  },
}];

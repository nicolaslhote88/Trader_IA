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
  if (typeof v === 'string') return v.split(',').map((x) => String(x || '').trim()).filter(Boolean);
  return [];
}

const ALLOWED_PAIRS = new Set([
  'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'NZDUSD', 'USDCAD',
  'EURGBP', 'EURJPY', 'EURCHF', 'EURAUD', 'EURCAD', 'EURNZD',
  'GBPJPY', 'GBPCHF', 'GBPAUD', 'GBPCAD',
  'AUDJPY', 'AUDNZD', 'AUDCAD', 'NZDJPY', 'NZDCAD', 'CADJPY', 'CHFJPY', 'CADCHF', 'CHFCAD', 'JPYNZD',
]);

function keepPairs(raw) {
  const out = [];
  for (const pair of toArray(raw)) {
    if (ALLOWED_PAIRS.has(pair) && !out.includes(pair)) out.push(pair);
  }
  return out.join(', ');
}

function derivePairs(currencies) {
  const out = [];
  const add = (pair) => {
    if (ALLOWED_PAIRS.has(pair) && !out.includes(pair)) out.push(pair);
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
  return out.slice(0, 5).join(', ');
}

function clamp01(v, d = 0.5) {
  const n = Number(v);
  if (!Number.isFinite(n)) return d;
  return Math.max(0, Math.min(1, n));
}

function clamp10(v, d = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return d;
  return Math.max(0, Math.min(10, Math.round(n)));
}

const j = $json || {};
const ai = safeJsonParse(j.output?.[0]?.content?.[0]?.text || j.content || '{}');
const now = new Date().toISOString();
const assetClass = String(ai.impact_asset_class || 'FX').trim();
const currenciesBullish = toArray(ai.currencies_bullish).map((x) => String(x || '').toUpperCase().slice(0, 3));
const currenciesBearish = toArray(ai.currencies_bearish).map((x) => String(x || '').toUpperCase().slice(0, 3));
const fxPairs = keepPairs(ai.impact_fx_pairs) || derivePairs([...new Set([...currenciesBullish, ...currenciesBearish])]);

return [{
  json: {
    run_id: j.run_id || '',
    sourceId: j.sourceId || '',
    sourceTier: j.sourceTier || 'A',
    dedupeKey: j.dedupeKey,
    eventKey: j.eventKey || '',
    canonicalUrl: j.canonicalUrlNormalized || j.canonicalUrl || j.url || 'unknown',
    publishedAt: j.publishedAt || now,
    title: j.title || 'unknown',
    source: j.source || j.sourceId || 'fx_channel',
    feedUrl: j.feedUrl || '',
    Snippet: j.snippet || '',
    impact_region: String(ai.impact_region || 'Global').trim(),
    impact_asset_class: assetClass,
    impact_magnitude: String(ai.impact_magnitude || 'Low').trim(),
    impact_fx_pairs: assetClass.includes('FX') || assetClass.includes('Mixed') ? fxPairs : '',
    currencies_bullish: currenciesBullish.join(', '),
    currencies_bearish: currenciesBearish.join(', '),
    Regime: ai.market_regime || 'Neutral',
    Theme: ai.macro_theme || 'Banques Centrales',
    urgency: ai.urgency || 'low',
    confidence: clamp01(ai.confidence, 0.5),
    ImpactScore: clamp10(ai.impact_score, 0),
    Strategy: ai.strategic_summary || '',
    fx_directional_hint: ai.fx_directional_hint || '',
    tagger_version: 'geo_v1',
    firstSeenAt: j.fetchedAt || now,
    seenNowAt: now,
    analyzedAt: now,
  },
}];

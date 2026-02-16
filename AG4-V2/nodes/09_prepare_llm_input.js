// 20H0 — Prepare LLM input (V2)
function stripHtml(s) {
  if (!s) return '';
  return String(s).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function toIso(x) {
  if (!x || x === 'unknown') return 'unknown';
  const d = new Date(x);
  return isNaN(d.getTime()) ? 'unknown' : d.toISOString();
}

let symbolDirectory = [];
try {
  symbolDirectory = $items('20A2 - Build Symbol Directory')[0]?.json?.symbolDirectory || [];
} catch {
  symbolDirectory = [];
}

const j = $json || {};
const symbols = Array.isArray(j.symbols) ? j.symbols : [];

const payload = {
  id: j.id || 'unknown',
  dedupeKey: j.dedupeKey,
  eventKey: j.eventKey,
  title: stripHtml(j.title || 'unknown'),
  snippet: stripHtml(j.snippet || ''),
  url: j.canonicalUrlNormalized || j.canonicalUrl || j.url || 'unknown',
  publishedAt: toIso(j.publishedAtNormalized || j.publishedAt || 'unknown'),
  source: j.source || 'unknown',
  symbols,
  type: symbols.length ? 'symbol' : (j.type || 'macro'),
  preImpactScore: j.preImpactScore ?? 0,
  preUrgency: j.preUrgency || 'low',
  universeSymbolDirectory: symbolDirectory,
};

return {
  ...j,
  llmInput: JSON.stringify(payload),
};

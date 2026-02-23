// 20H0 - Prepare LLM input (V2)
function stripHtml(s) {
  if (!s) return '';
  return String(s).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function toIso(x) {
  if (!x || x === 'unknown') return 'unknown';
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? 'unknown' : d.toISOString();
}

function toArray(v) {
  if (Array.isArray(v)) return v;
  if (typeof v === 'string') {
    try {
      const parsed = JSON.parse(v);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

let sectorDictionary = [];
try {
  const raw = $items('20A2 - Build Sector Dictionary')[0]?.json?.sectorDictionary;
  sectorDictionary = toArray(raw).map((x) => String(x || '').trim()).filter(Boolean);
} catch {
  sectorDictionary = [];
}

const j = $json || {};
const candidateSectors = toArray(j.candidateSectors).map((x) => String(x || '').trim()).filter(Boolean).slice(0, 5);

const payload = {
  id: j.id || 'unknown',
  dedupeKey: j.dedupeKey,
  eventKey: j.eventKey,
  title: stripHtml(j.title || 'unknown'),
  snippet: stripHtml(j.snippet || ''),
  url: j.canonicalUrlNormalized || j.canonicalUrl || j.url || 'unknown',
  publishedAt: toIso(j.publishedAtNormalized || j.publishedAt || 'unknown'),
  source: j.source || 'unknown',
  type: 'macro',
  preImpactScore: j.preImpactScore ?? 0,
  preUrgency: j.preUrgency || 'low',
  candidateSectors,
  universeSectors: sectorDictionary,
};

return {
  ...j,
  symbols: [],
  type: 'macro',
  universeSectors: sectorDictionary,
  candidateSectors,
  llmInput: JSON.stringify(payload),
};

function stripHtml(s) {
  if (!s) return '';
  return String(s).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

const j = $json || {};
const payload = {
  dedupeKey: j.dedupeKey,
  eventKey: j.eventKey,
  origin: 'fx_channel',
  source: j.source || 'fx_channel',
  sourceTier: j.sourceTier || 'A',
  title: stripHtml(j.title || 'unknown'),
  snippet: stripHtml(j.snippet || ''),
  url: j.canonicalUrlNormalized || j.canonicalUrl || j.url || 'unknown',
  publishedAt: j.publishedAt || 'unknown',
};

return {
  ...j,
  llmInput: JSON.stringify(payload),
};


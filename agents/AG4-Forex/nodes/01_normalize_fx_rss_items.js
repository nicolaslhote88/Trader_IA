function toIsoDate(v) {
  if (!v) return 'unknown';
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? 'unknown' : d.toISOString();
}

function stripHtml(s) {
  if (!s) return '';
  return String(s).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

let feedCtx = {};
try {
  feedCtx = $('20D - Split FX Feeds').item.json || {};
} catch {
  feedCtx = {};
}

const fetchedAt = new Date().toISOString();
return $input.all().map((item) => {
  const r = item.json || {};
  const canonicalUrl = String(r.link || r.url || r.guid || '').trim() || 'unknown';
  return {
    json: {
      run_id: feedCtx.run_id || '',
      sourceId: feedCtx.sourceId || '',
      sourceTier: feedCtx.sourceTier || 'A',
      feedUrl: feedCtx.url || '',
      db_path: feedCtx.db_path || '',
      origin: 'fx_channel',
      fetchedAt,
      publishedAt: toIsoDate(r.isoDate || r.pubDate || r.published || r.date || r.updated || fetchedAt),
      canonicalUrl,
      url: canonicalUrl,
      title: stripHtml(r.title || 'unknown'),
      snippet: stripHtml(r.contentSnippet || r.content || r.summary || ''),
      source: feedCtx.sourceId || 'fx_channel',
    },
  };
});


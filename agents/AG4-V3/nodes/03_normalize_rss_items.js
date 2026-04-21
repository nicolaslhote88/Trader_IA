// 20F - Normalize RSS Items (V2)
function toIsoDate(v) {
  if (!v) return 'unknown';
  const d = new Date(v);
  return isNaN(d.getTime()) ? 'unknown' : d.toISOString();
}

function stripHtml(s) {
  if (!s) return '';
  return String(s).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function safeUrl(u) {
  if (!u || typeof u !== 'string') return 'unknown';
  return u.trim();
}

function inferSource(u) {
  try {
    const h = new URL(u).hostname.toLowerCase();
    if (h.includes('amf-france')) return 'AMF France';
    if (h.includes('boursorama')) return 'Boursorama';
    if (h.includes('investir') || h.includes('lesechos')) return 'Investir/Les Echos';
    if (h.includes('reuters')) return 'Reuters';
    if (h.includes('bloomberg')) return 'Bloomberg';
    if (h.includes('zonebourse')) return 'Zonebourse';
    if (h.includes('tradingview')) return 'TradingView';
    if (h.includes('euronext')) return 'Euronext';
    return h.replace(/^www\./, '');
  } catch {
    return 'unknown';
  }
}

let feedCtx = {};
try {
  feedCtx = $('20D - Split Feeds').item.json || {};
} catch {
  feedCtx = {};
}

const fetchedAt = new Date().toISOString();
const rows = $input.all().map(i => i.json || {});

return rows.map(r => {
  const canonicalUrl = safeUrl(r.link || r.url || r.guid);
  const title = (typeof r.title === 'string' && r.title.trim()) ? stripHtml(r.title) : 'unknown';
  const snippet = stripHtml(r.contentSnippet || r.content || r.summary || '');
  const publishedAt = toIsoDate(r.isoDate || r.pubDate || r.published || r.date || r.updated || fetchedAt);

  return {
    json: {
      run_id: feedCtx.run_id || '',
      fetchedAt,
      publishedAt,
      source: inferSource(canonicalUrl),
      url: canonicalUrl,
      canonicalUrl,
      title,
      snippet,
      symbols: [],
      type: 'macro',
      feedUrl: feedCtx.url || '',
      sourceTier: feedCtx.sourceTier || 2,
      sourceId: feedCtx.sourceId || '',
      _itemsLoopReset: true,
    }
  };
});

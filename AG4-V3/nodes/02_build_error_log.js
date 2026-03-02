// 20E_ERR - Build Error Log Items (V2)
const crypto = require('crypto');

function pick(obj, paths) {
  for (const p of paths) {
    const v = p.split('.').reduce((acc, k) => (acc && acc[k] !== undefined ? acc[k] : undefined), obj);
    if (v !== undefined && v !== null && v !== '') return v;
  }
  return undefined;
}

return $input.all().map((i, idx) => {
  const e = i.json || {};
  let feedCtx = {};
  try {
    feedCtx = $items('20D - Split Feeds', 0, idx)?.[0]?.json || {};
  } catch {
    feedCtx = {};
  }

  const feedUrl = pick(e, ['url', 'rssUrl', 'feedUrl', 'request.url', 'context.url']) || feedCtx.url || 'unknown';
  const httpCode = pick(e, ['httpCode', 'error.httpCode', 'statusCode']) || null;
  const errorMessage = pick(e, ['errorMessage', 'message', 'error.message', 'rawErrorMessage.0']) || 'unknown';
  const occurredAt = new Date().toISOString();

  const sig = `${feedUrl}|${httpCode || 'NA'}|${String(errorMessage).slice(0, 120)}|${occurredAt.slice(0, 16)}`;
  const dedupeKey = 'ERR_' + crypto.createHash('sha256').update(sig).digest('hex');

  return {
    json: {
      run_id: feedCtx.run_id || '',
      sourceTier: feedCtx.sourceTier ?? null,
      sourceId: feedCtx.sourceId || '',
      dedupeKey,
      canonicalUrl: feedUrl,
      publishedAt: occurredAt,
      title: `RSS_ERROR ${httpCode || ''}`.trim(),
      source: 'rss_feed',
      feedUrl,
      symbols: '',
      type: 'rss_error',
      notes: errorMessage,
      ImpactScore: 0,
      confidence: 1,
      urgency: 'immediate',
      Snippet: `RSS fetch error on ${feedUrl}`,
      firstSeenAt: occurredAt,
      Strategy: '',
      sectors_bearish: '',
      sectors_bullish: '',
      currencies_bearish: '',
      currencies_bullish: '',
      Losers: '',
      Winners: '',
      Theme: 'Pipeline/Error',
      Regime: 'Neutral',
      eventKey: '',
      analyzedAt: occurredAt,
      rawError: e,
    }
  };
});

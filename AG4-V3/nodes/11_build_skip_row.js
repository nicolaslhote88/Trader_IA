// 20S1 - Build final row for skip path (V2)
const j = $json || {};
const now = new Date().toISOString();

const symbols = Array.isArray(j.symbols) ? j.symbols.join(', ') : (j.symbols || '');
const sectorsBullish = j.sectors_bullish || j.Winners || '';
const sectorsBearish = j.sectors_bearish || j.Losers || '';

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
    symbols,
    type: j.type || (symbols ? 'symbol' : 'macro'),
    notes: j.notes || `Skipped: ${j._reason || 'duplicate'}`,
    ImpactScore: j.ImpactScore ?? 0,
    confidence: j.confidence ?? 0,
    urgency: j.urgency || 'low',
    Snippet: j.Snippet || j.snippet || '',
    firstSeenAt: j.firstSeenAt || j.seenNowAt || now,
    Strategy: j.Strategy || '',
    sectors_bearish: sectorsBearish,
    sectors_bullish: sectorsBullish,
    Losers: j.Losers || sectorsBearish,
    Winners: j.Winners || sectorsBullish,
    Theme: j.Theme || 'Resultats/Micro',
    Regime: j.Regime || 'Neutral',
    analyzedAt: j.analyzedAt || now,
    _action: 'skip',
    _reason: j._reason || 'duplicate',
  }
}];

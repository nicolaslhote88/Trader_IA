// 20G1B — Build History Index (V2)
// Optimized dictionary for fast lookups, filtered to recent horizon.

const LOOKBACK_DAYS = 120;
const now = new Date();
const cutoff = new Date(now.getTime() - LOOKBACK_DAYS * 24 * 3600 * 1000);

function parseDt(v) {
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

const rows = $input.all().map(i => i.json || {});

const byDedupe = {};
const byEvent = {};
let kept = 0;

for (const row of rows) {
  const typ = String(row.type || '').toLowerCase();
  if (typ === 'rss_error') continue;

  const dt = parseDt(row.firstSeenAt || row.publishedAt || row.analyzedAt);
  if (dt && dt < cutoff) continue;

  const dKey = row.dedupeKey ? String(row.dedupeKey) : '';
  const eKey = row.eventKey ? String(row.eventKey) : '';

  if (dKey) byDedupe[dKey] = row;
  if (eKey && !byEvent[eKey]) byEvent[eKey] = row;
  kept += 1;
}

return [{
  json: {
    historyIndex: byDedupe,
    historyEventIndex: byEvent,
    historyStats: {
      loadedRows: rows.length,
      keptRows: kept,
      lookbackDays: LOOKBACK_DAYS,
      indexedAt: now.toISOString(),
    }
  }
}];

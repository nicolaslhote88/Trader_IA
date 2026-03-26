// 20G0 — Add deterministic dedupeKey + eventKey (V2)
const crypto = require('crypto');

const LOOKBACK_DAYS = 120;

function hash(s) {
  return crypto.createHash('sha256').update(String(s)).digest('hex');
}

function normalizeTitle(s) {
  return String(s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeUrl(u) {
  if (!u || typeof u !== 'string') return '';
  try {
    const url = new URL(u);
    const drop = ['utm_', 'fbclid', 'gclid', 'ref', 'src'];
    const remove = [];
    url.searchParams.forEach((_, k) => {
      if (drop.some(p => k.toLowerCase().startsWith(p))) remove.push(k);
    });
    remove.forEach(k => url.searchParams.delete(k));
    return url.toString().replace(/\/$/, '');
  } catch {
    return String(u).trim();
  }
}

function toIso(v) {
  if (!v || v === 'unknown') return 'unknown';
  const d = new Date(v);
  return isNaN(d.getTime()) ? 'unknown' : d.toISOString();
}

return $input.all().map((item) => {
  const n = { ...(item.json || {}) };

  const cleanUrl = normalizeUrl(n.canonicalUrl || n.url || n.link || '');
  const cleanDate = toIso(n.publishedAt || n.fetchedAt);
  const dayBucket = cleanDate !== 'unknown' ? cleanDate.slice(0, 10) : 'unknown-day';
  const titleNorm = normalizeTitle(n.title || '');

  const dedupeSignature = cleanUrl
    ? `URL|${cleanUrl}`
    : `TXT|${titleNorm}|${String(n.source || 'unknown').toLowerCase()}|${dayBucket}`;

  // Event key: same story clustering across many URLs in same day
  const eventSignature = `${titleNorm.slice(0, 180)}|${dayBucket}`;

  n.canonicalUrlNormalized = cleanUrl;
  n.publishedAtNormalized = cleanDate;
  n.dedupeKey = hash(dedupeSignature);
  n.eventKey = hash(eventSignature);

  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - LOOKBACK_DAYS);
  const dt = cleanDate === 'unknown' ? null : new Date(cleanDate);
  const tooOld = !dt || isNaN(dt.getTime()) ? false : dt < cutoff;

  n._isValid = !!titleNorm && !tooOld;
  n._kind = 'news_item';
  n._itemsLoopReset = false;

  return { json: n, pairedItem: item.pairedItem };
});

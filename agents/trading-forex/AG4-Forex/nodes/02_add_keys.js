const crypto = require('crypto');

function hash(s) {
  return crypto.createHash('sha1').update(String(s)).digest('hex');
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
    const remove = [];
    url.searchParams.forEach((_, k) => {
      if (k.toLowerCase().startsWith('utm_') || ['fbclid', 'gclid', 'ref', 'src'].includes(k.toLowerCase())) remove.push(k);
    });
    remove.forEach((k) => url.searchParams.delete(k));
    return url.toString().replace(/\/$/, '');
  } catch {
    return String(u).trim();
  }
}

return $input.all().map((item) => {
  const j = { ...(item.json || {}) };
  const cleanUrl = normalizeUrl(j.canonicalUrl || j.url || '');
  const titleNorm = normalizeTitle(j.title || '');
  const day = String(j.publishedAt || '').slice(0, 10) || 'unknown-day';
  j.canonicalUrlNormalized = cleanUrl;
  j.dedupeKey = hash(cleanUrl || `${titleNorm}|${day}`);
  j.eventKey = hash(`${titleNorm.slice(0, 180)}|${day}`);
  return { json: j, pairedItem: item.pairedItem };
});


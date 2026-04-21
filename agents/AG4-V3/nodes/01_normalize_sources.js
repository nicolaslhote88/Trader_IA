// 20B — Normalize RSS Sources (V2)
function toIntInterest(v) {
  if (v === null || v === undefined) return 0;
  const s = String(v).trim();
  const m = s.match(/(\d+(?:[.,]\d+)?)/);
  if (!m) return 0;
  return Math.round(parseFloat(m[1].replace(',', '.')));
}

function slugify(s) {
  return String(s || '')
    .trim()
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

const rows = $input.all().map(i => i.json || {});
const out = [];

for (const row of rows) {
  const family = row['Famille'] ?? 'unknown';
  const source = row['Source'] ?? 'unknown';
  const feedName = row['Nom du Flux'] ?? 'unknown';
  const url = row['URL Exacte'] ?? '';

  if (!url || !String(url).startsWith('http')) continue;

  const interest = toIntInterest(row['Note Intérêt Agent (1-5)']);
  const tier = interest >= 4 ? 1 : (interest >= 2 ? 2 : 3);

  out.push({
    json: {
      enabled: interest >= 2,
      family,
      source,
      feedName,
      url,
      interest,
      sourceTier: tier,
      sourceId: slugify(`${family}_${source}_${feedName}`),
      fetchedAt: new Date().toISOString(),
    }
  });
}

return out;

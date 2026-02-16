// 20F1 — Tag Symbols from Universe (V2)
function norm(s) {
  return String(s || '').toLowerCase();
}

function unique(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function safeParse(v) {
  if (Array.isArray(v)) return v;
  if (typeof v === 'string') {
    try { const j = JSON.parse(v); return Array.isArray(j) ? j : []; } catch { return []; }
  }
  return [];
}

let symbolDirectory = [];
try {
  const d = $items('20A2 - Build Symbol Directory')[0]?.json?.symbolDirectory;
  symbolDirectory = safeParse(d).slice(0, 2000);
} catch {
  symbolDirectory = [];
}

const bySymbol = new Set(symbolDirectory.map(x => String(x.symbol || '').trim()).filter(Boolean));
const byName = symbolDirectory
  .map(x => ({ symbol: String(x.symbol || '').trim(), name: norm(x.name || '') }))
  .filter(x => x.symbol && x.name.length >= 4);

return $input.all().map(item => {
  const j = item.json || {};
  const text = `${j.title || ''} ${j.snippet || ''}`;
  const textNorm = norm(text);

  const found = [];

  // Pattern tickers like MC.PA / AIR.PA / ABC.DE
  const tickerMatches = text.match(/\b[A-Z]{1,6}\.(PA|AS|BR|MI|DE|L|SW|MC)\b/g) || [];
  for (const t of tickerMatches) {
    if (bySymbol.has(t)) found.push(t);
  }

  // Company name contains
  for (const row of byName) {
    if (textNorm.includes(row.name)) found.push(row.symbol);
  }

  const symbols = unique(found).slice(0, 8);

  return {
    json: {
      ...j,
      symbols,
      type: symbols.length ? 'symbol' : 'macro',
    }
  };
});

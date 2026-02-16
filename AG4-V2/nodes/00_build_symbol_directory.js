// 20A2 — Build Symbol Directory from Universe sheet (V2)
function normName(s) {
  return String(s || '')
    .trim()
    .replace(/\s+/g, ' ');
}

const rows = $input.all().map(i => i.json || {});
const out = [];

for (const r of rows) {
  const symbol = String(r.Symbol || r.symbol || '').trim();
  if (!symbol) continue;
  const name = normName(r.Name || r.name || '');
  out.push({ symbol, name });
}

return [{
  json: {
    symbolDirectory: out,
    count: out.length,
    generatedAt: new Date().toISOString(),
  }
}];

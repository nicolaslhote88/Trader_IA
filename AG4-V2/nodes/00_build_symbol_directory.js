// 20A2 - Build Sector Dictionary from Universe sheet (V2)
function cleanText(s) {
  return String(s || '')
    .trim()
    .replace(/\s+/g, ' ');
}

function pickSector(row) {
  return (
    row.Sector ??
    row.sector ??
    row['GICS Sector'] ??
    row.gics_sector ??
    row['Sector Name'] ??
    row['sector_name'] ??
    ''
  );
}

const rows = $input.all().map((i) => i.json || {});
const uniq = new Map();

for (const row of rows) {
  const sector = cleanText(pickSector(row));
  if (!sector) continue;
  const key = sector.toLowerCase();
  if (!uniq.has(key)) uniq.set(key, sector);
}

const sectorDictionary = Array.from(uniq.values()).sort((a, b) => a.localeCompare(b));

return [{
  json: {
    sectorDictionary,
    count: sectorDictionary.length,
    generatedAt: new Date().toISOString(),
  },
}];

// 20F1 - Attach sector dictionary + detect sector hints (V2)
function normalizeText(s) {
  return String(s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function unique(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function safeParseArray(v) {
  if (Array.isArray(v)) return v;
  if (typeof v === 'string') {
    try {
      const parsed = JSON.parse(v);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

let sectorDictionary = [];
try {
  const raw = $items('20A2 - Build Sector Dictionary')[0]?.json?.sectorDictionary;
  sectorDictionary = safeParseArray(raw).map((x) => String(x || '').trim()).filter(Boolean);
} catch {
  sectorDictionary = [];
}

const sectorMatchers = sectorDictionary.map((sector) => ({
  label: sector,
  norm: normalizeText(sector),
})).filter((x) => x.norm.length >= 3);

return $input.all().map((item) => {
  const j = item.json || {};
  const textNorm = normalizeText(`${j.title || ''} ${j.snippet || ''}`);

  const candidateSectors = [];
  for (const m of sectorMatchers) {
    if (textNorm.includes(m.norm)) candidateSectors.push(m.label);
  }

  return {
    json: {
      ...j,
      symbols: [],
      type: 'macro',
      universeSectors: sectorDictionary,
      candidateSectors: unique(candidateSectors).slice(0, 5),
    },
    pairedItem: item.pairedItem,
  };
});

// AG1.00 - Assemble Input Packs (clean + matrix pack support)
// Mode: Run Once for All Items

const incoming = $input.all();

const out = {
  run: null,
  portfolioBrief: null,
  sector_brief: "",
  opportunity_brief: "",
  opportunity_pack: null,
  opportunity_stats: null,
  matrix_thresholds: null,
};

function pickText(obj, keys) {
  for (const k of keys) {
    if (obj && typeof obj[k] === "string" && obj[k].trim()) return obj[k].trim();
  }
  return "";
}

function pickObject(obj, keys) {
  for (const k of keys) {
    if (obj && typeof obj[k] === "object" && obj[k] !== null) return obj[k];
  }
  return null;
}

for (const it of incoming) {
  const j = it.json || {};

  if (!out.run) out.run = pickObject(j, ["run", "Run", "decisionMeta", "meta"]);
  if (!out.portfolioBrief) out.portfolioBrief = pickObject(j, ["portfolioBrief", "PortfolioBrief"]);

  if (!out.sector_brief) {
    out.sector_brief = pickText(j, ["sector_brief", "sectorBrief", "sector", "sector_momentum"]);
  }
  if (!out.opportunity_brief) {
    out.opportunity_brief = pickText(j, ["opportunity_brief", "opportunityBrief", "opportunity", "matrix"]);
  }

  if (!out.opportunity_pack && j && typeof j.opportunity_pack === "object") {
    out.opportunity_pack = j.opportunity_pack;
  }
  if (!out.opportunity_stats && j && typeof j.opportunity_stats === "object") {
    out.opportunity_stats = j.opportunity_stats;
  }
  if (!out.matrix_thresholds && j && typeof j.matrix_thresholds === "object") {
    out.matrix_thresholds = j.matrix_thresholds;
  }

  if (!out.sector_brief) {
    out.sector_brief = pickText(j, ["text", "brief", "output"]);
  }
  if (!out.opportunity_brief) {
    out.opportunity_brief = pickText(j, ["text", "brief", "output"]);
  }
}

if (!out.run) out.run = {};
if (!out.portfolioBrief) out.portfolioBrief = {};
if (!out.sector_brief) out.sector_brief = "";
if (!out.opportunity_brief) out.opportunity_brief = "";
if (!out.opportunity_pack && out.opportunity_brief) {
  out.opportunity_pack = {
    generatedAt: new Date().toISOString(),
    rows: [],
    stats: out.opportunity_stats || {},
    thresholds: out.matrix_thresholds || {},
  };
}

out.__debug = {
  incomingItems: incoming.length,
  has_run: !!Object.keys(out.run).length,
  has_portfolioBrief: !!Object.keys(out.portfolioBrief).length,
  has_sector_brief: !!out.sector_brief,
  has_opportunity_brief: !!out.opportunity_brief,
  has_opportunity_pack: !!out.opportunity_pack,
};

return [{ json: out }];

// AG1.00 - Assemble Input Packs (V3, multi-asset aware)
// Mode: Run Once for All Items

const incoming = $input.all();

const out = {
  run: null,
  config: null,
  portfolioBrief: null,
  sector_brief: "",
  opportunity_brief: "",
  opportunity_pack: null,
  opportunity_stats: null,
  matrix_thresholds: null,
  fx_pairs: [],
  fx_macro: null,
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

function toBool(v, dflt = false) {
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return dflt;
  if (["1", "true", "yes", "y", "on", "enabled"].includes(s)) return true;
  if (["0", "false", "no", "n", "off", "disabled"].includes(s)) return false;
  return dflt;
}

function isFxSymbol(s) {
  const t = String(s || "").toUpperCase();
  return t.startsWith("FX:") || t.endsWith("=X");
}

for (const it of incoming) {
  const j = it.json || {};

  if (!out.run) out.run = pickObject(j, ["run", "Run", "decisionMeta", "meta"]);
  if (!out.portfolioBrief) out.portfolioBrief = pickObject(j, ["portfolioBrief", "PortfolioBrief"]);
  if (!out.config) out.config = pickObject(j, ["config", "cfg", "settings"]);

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

  if (!out.fx_macro && j && typeof j.fx_macro === "object") {
    out.fx_macro = j.fx_macro;
  }
  if (Array.isArray(j.fx_pairs) && j.fx_pairs.length) {
    out.fx_pairs.push(...j.fx_pairs);
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

if (!out.config) {
  out.config = {
    strategyVersion: out.run.strategyVersion || "strategy_v3",
    configVersion: out.run.configVersion || "config_v3",
    promptVersion: out.run.promptVersion || "prompt_v3",
    enable_fx: !!out.run.enable_fx,
  };
}

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

// Extract FX rows from matrix pack when no explicit fx_pairs were passed.
if ((!out.fx_pairs || out.fx_pairs.length === 0) && Array.isArray(out.opportunity_pack?.rows)) {
  for (const r of out.opportunity_pack.rows) {
    const symbol = String(r?.symbol || "").trim();
    if (!isFxSymbol(symbol)) continue;
    out.fx_pairs.push({
      symbol_internal: symbol.startsWith("FX:") ? symbol : `FX:${symbol.replace("=X", "").replace(/[^A-Za-z]/g, "").slice(0, 6).toUpperCase()}`,
      directional_bias: String(r?.decision || "").toUpperCase().includes("REDUIRE") ? "SELL_BASE" : "NEUTRAL",
      confidence: Number(r?.p_win_pct || r?.confidence || 0),
      rationale: String(r?.action_reason || ""),
      asOf: out.opportunity_pack.generatedAt || new Date().toISOString(),
      urgent_event_window: false,
    });
  }
}

const enableFx = toBool(out.run.enable_fx ?? out.config.enable_fx, false);
const universeScope = Array.isArray(out.run.universe_scope)
  ? out.run.universe_scope
  : (enableFx ? ["EQUITY", "CRYPTO", "FX"] : ["EQUITY", "CRYPTO"]);

const inputSnapshot = {
  portfolioUpdatedAt: out.portfolioBrief?.generatedAt || out.portfolioBrief?.portfolioSummary?.positions?.[0]?.UpdatedAt || null,
  technicalUpdatedAt: out.opportunity_pack?.generatedAt || null,
  researchUpdatedAt: out.opportunity_pack?.generatedAt || null,
  newsGeneratedAt: out.opportunity_pack?.generatedAt || null,
  universe_scope: universeScope,
  fxUniverseCount: enableFx ? out.fx_pairs.length : 0,
  fxSignalsFreshness: {
    max_age_h1: null,
    max_age_d1: null,
  },
  fxMacroAsOf: out.fx_macro?.as_of || out.fx_macro?.asOf || null,
  enable_fx: enableFx,
};

out.run = {
  ...out.run,
  strategyVersion: out.run.strategyVersion || out.config.strategyVersion || "strategy_v3",
  configVersion: out.run.configVersion || out.config.configVersion || "config_v3",
  promptVersion: out.run.promptVersion || out.config.promptVersion || "prompt_v3",
  enable_fx: enableFx,
  universe_scope: universeScope,
  inputSnapshot,
};

out.__debug = {
  incomingItems: incoming.length,
  has_run: !!Object.keys(out.run).length,
  has_portfolioBrief: !!Object.keys(out.portfolioBrief).length,
  has_sector_brief: !!out.sector_brief,
  has_opportunity_brief: !!out.opportunity_brief,
  has_opportunity_pack: !!out.opportunity_pack,
  fx_pairs_count: out.fx_pairs.length,
  enable_fx: enableFx,
};

return [{ json: out }];

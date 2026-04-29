// FX.00 - Prepare FX Brief Context
// Code node (typeVersion 2)
// Output: normalized FX context scaffolding for downstream merge/assembly.

const cfg = $json ?? {};
const run = (cfg.run && typeof cfg.run === "object") ? cfg.run : {};
const nowIso = new Date().toISOString();

const toNum = (v, dflt = null) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
};

const toBool = (v, dflt = false) => {
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return dflt;
  if (["1", "true", "yes", "y", "on", "enabled"].includes(s)) return true;
  if (["0", "false", "no", "n", "off", "disabled"].includes(s)) return false;
  return dflt;
};

const enableFx = toBool(cfg.enable_fx ?? run.enable_fx, true);
const existingContext = (cfg.fx_context && typeof cfg.fx_context === "object") ? cfg.fx_context : {};
const existingMacro = (cfg.fx_macro && typeof cfg.fx_macro === "object") ? cfg.fx_macro : {};
const existingRates = (cfg.fx_rates && typeof cfg.fx_rates === "object") ? cfg.fx_rates : {};

const fxContext = {
  as_of: String(existingContext.as_of || existingContext.asOf || existingMacro.as_of || existingMacro.asOf || nowIso),
  macro_regime: String(existingContext.macro_regime || existingMacro.market_regime || "Neutral"),
  macro_confidence: toNum(existingContext.macro_confidence ?? existingMacro.confidence, 0),
  signals_freshness: {
    max_age_h1_hours: toNum(existingContext?.signals_freshness?.max_age_h1_hours, null),
    max_age_d1_hours: toNum(existingContext?.signals_freshness?.max_age_d1_hours, null),
  },
  fx_universe_count: toNum(existingContext.fx_universe_count, 0),
  fx_sleeve: {
    target_pct_min: toNum(existingContext?.fx_sleeve?.target_pct_min, 5),
    target_pct_max: toNum(existingContext?.fx_sleeve?.target_pct_max, 10),
    per_pair_pct_max: toNum(existingContext?.fx_sleeve?.per_pair_pct_max, 3),
    default_pair_pct: toNum(existingContext?.fx_sleeve?.default_pair_pct, 1.5),
  },
};

const fxRates = { EUR: 1.0 };
for (const [k, v] of Object.entries(existingRates)) {
  const n = toNum(v, null);
  if (n > 0) fxRates[String(k).toUpperCase()] = n;
}

return [
  {
    json: {
      enable_fx: enableFx,
      fx_context: fxContext,
      fx_rates: fxRates,
      fx_pairs: Array.isArray(cfg.fx_pairs) ? cfg.fx_pairs : [],
      fx_candidates: Array.isArray(cfg.fx_candidates) ? cfg.fx_candidates : [],
    },
  },
];

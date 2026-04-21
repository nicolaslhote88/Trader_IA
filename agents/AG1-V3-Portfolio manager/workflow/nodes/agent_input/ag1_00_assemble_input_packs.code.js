// AG1.00 - Assemble Input Packs (V3, FX actionable)
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
  fx_candidates: [],
  fx_macro: null,
  fx_context: null,
  fx_rates: null,
  fx_candidates_summary: "",
};

const decisionMemoryMerged = {};
const executionMemoryMerged = {};
const recentIdeasMerged = [];

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

function toNum(v, dflt = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function isObj(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normSymbol(v) {
  return String(v ?? "").trim().toUpperCase();
}

function dedupeIdeas(ideas) {
  const sorted = (Array.isArray(ideas) ? ideas : []).filter(isObj).sort((a, b) => {
    const ta = Date.parse(String(a?.ts || "")) || 0;
    const tb = Date.parse(String(b?.ts || "")) || 0;
    return tb - ta;
  });
  const outIdeas = [];
  const seen = new Set();
  for (const it of sorted) {
    const sym = normSymbol(it.symbol);
    if (!sym || seen.has(sym)) continue;
    seen.add(sym);
    outIdeas.push(it);
  }
  return outIdeas;
}

function mergePortfolioBrief(cur, incomingBrief) {
  const base = isObj(cur) ? { ...cur } : {};
  const inc = isObj(incomingBrief) ? incomingBrief : {};

  if (isObj(inc.summary)) base.summary = { ...(isObj(base.summary) ? base.summary : {}), ...inc.summary };
  if (Array.isArray(inc.positions) && inc.positions.length) base.positions = inc.positions;
  if (Array.isArray(inc.recentUnexecutedIdeas) && inc.recentUnexecutedIdeas.length) {
    base.recentUnexecutedIdeas = dedupeIdeas([...(Array.isArray(base.recentUnexecutedIdeas) ? base.recentUnexecutedIdeas : []), ...inc.recentUnexecutedIdeas]);
  }
  if (Array.isArray(inc.executionNotes) && inc.executionNotes.length) base.executionNotes = inc.executionNotes;
  if (typeof inc.agentBriefingText === "string" && inc.agentBriefingText.trim()) base.agentBriefingText = inc.agentBriefingText;

  const scalarKeys = ["generatedAt", "totalValue", "cash", "exposurePct", "positionsCount", "marketValue", "source"];
  for (const k of scalarKeys) {
    if (inc[k] !== undefined && inc[k] !== null && String(inc[k]).trim() !== "") base[k] = inc[k];
  }

  for (const [k, v] of Object.entries(inc)) {
    if (!(k in base)) base[k] = v;
  }

  return base;
}

function enrichPortfolioBriefWithMemory(pb, decisionMemory, executionMemory, recentIdeas) {
  const outPb = isObj(pb) ? { ...pb } : {};
  const decMem = isObj(decisionMemory) ? decisionMemory : {};
  const exeMem = isObj(executionMemory) ? executionMemory : {};

  if (Array.isArray(outPb.positions)) {
    outPb.positions = outPb.positions.map((pos) => {
      if (!isObj(pos)) return pos;
      const sym = normSymbol(pos.symbol || pos.Symbol);
      const next = { ...pos };
      if (!next.lastDecision && sym && isObj(decMem[sym])) next.lastDecision = decMem[sym];
      if (!next.executionMemory && sym && isObj(exeMem[sym])) next.executionMemory = exeMem[sym];
      return next;
    });
  }

  if ((!Array.isArray(outPb.recentUnexecutedIdeas) || !outPb.recentUnexecutedIdeas.length) && Array.isArray(recentIdeas) && recentIdeas.length) {
    outPb.recentUnexecutedIdeas = dedupeIdeas(recentIdeas);
  } else if (Array.isArray(outPb.recentUnexecutedIdeas) && Array.isArray(recentIdeas) && recentIdeas.length) {
    outPb.recentUnexecutedIdeas = dedupeIdeas([...outPb.recentUnexecutedIdeas, ...recentIdeas]);
  }

  if ((!outPb.agentBriefingText || !String(outPb.agentBriefingText).trim()) && Array.isArray(outPb.positions)) {
    const lines = ["ETAT DU PORTEFEUILLE:", `- Positions: ${outPb.positions.length}`];
    for (const p of outPb.positions.slice(0, 15)) {
      const d = p?.lastDecision || {};
      const e = p?.executionMemory || {};
      lines.push(
        `- ${p.symbol || p.Symbol}: qty=${p.quantity ?? p.qty ?? "n/a"} last=${p.lastPrice ?? p.price ?? "n/a"} `
        + `| lastAction=${d.action || "n/a"} | exec=${e.lastExecutionStatus || "NO_ORDER"}`
      );
    }
    outPb.agentBriefingText = lines.join("\n");
  }

  return outPb;
}

function getPortfolioUpdatedAt(pb) {
  if (!isObj(pb)) return null;

  const candidates = [];
  const direct = String(pb.portfolioUpdatedAt || pb.updatedAt || "").trim();
  if (direct) candidates.push(direct);

  if (Array.isArray(pb.positions)) {
    for (const pos of pb.positions) {
      const ts = String(pos?.updatedAt ?? pos?.UpdatedAt ?? "").trim();
      if (ts) candidates.push(ts);
    }
  }

  if (isObj(pb.summary)) {
    const ts = String(pb.summary.ts || pb.summary.updatedAt || "").trim();
    if (ts) candidates.push(ts);
  }

  if (!candidates.length) {
    const generatedAt = String(pb.generatedAt || "").trim();
    return generatedAt || null;
  }

  candidates.sort((a, b) => (Date.parse(b) || 0) - (Date.parse(a) || 0));
  return candidates[0] || null;
}

function isFxSymbol(s) {
  const t = String(s || "").toUpperCase();
  return t.startsWith("FX:") || t.endsWith("=X") || /^[A-Z]{6}$/.test(t);
}

function parseFxMeta(anySymbol) {
  const raw = String(anySymbol || "").toUpperCase().trim();
  if (!raw) return null;
  let pair = raw;
  if (pair.startsWith("FX:")) pair = pair.slice(3);
  if (pair.endsWith("=X")) pair = pair.slice(0, -2);
  pair = pair.replace(/[^A-Z]/g, "").slice(0, 6);
  if (pair.length !== 6) return null;
  return {
    pair,
    base: pair.slice(0, 3),
    quote: pair.slice(3, 6),
    symbol_internal: `FX:${pair}`,
    symbol_yahoo: `${pair}=X`,
  };
}

function inversePair(pair6) {
  const p = String(pair6 || "").toUpperCase().replace(/[^A-Z]/g, "").slice(0, 6);
  if (p.length !== 6) return "";
  return `${p.slice(3, 6)}${p.slice(0, 3)}`;
}

function normalizeBias(v) {
  const b = String(v || "").toUpperCase().trim();
  if (b === "BUY_BASE" || b === "SELL_BASE" || b === "NEUTRAL") return b;
  return "NEUTRAL";
}

function parseGates(v) {
  const s = String(v || "OK").toUpperCase();
  const parts = s.split("|").map((x) => x.trim()).filter(Boolean);
  return parts.length ? parts : ["OK"];
}

function mergeFxContext(cur, incomingContext, incomingMacro, fallbackAsOf) {
  const next = isObj(cur) ? { ...cur } : {};
  const ctx = isObj(incomingContext) ? incomingContext : {};
  const macro = isObj(incomingMacro) ? incomingMacro : {};

  if (!next.as_of) next.as_of = ctx.as_of || ctx.asOf || macro.as_of || macro.asOf || fallbackAsOf || null;
  if (!next.macro_regime) next.macro_regime = ctx.macro_regime || macro.market_regime || "Neutral";
  next.macro_confidence = toNum(
    ctx.macro_confidence ?? macro.confidence ?? next.macro_confidence,
    toNum(next.macro_confidence, 0),
  );

  const sfCur = isObj(next.signals_freshness) ? next.signals_freshness : {};
  const sfIn = isObj(ctx.signals_freshness) ? ctx.signals_freshness : {};
  next.signals_freshness = {
    max_age_h1_hours: toNum(sfIn.max_age_h1_hours ?? sfCur.max_age_h1_hours, null),
    max_age_d1_hours: toNum(sfIn.max_age_d1_hours ?? sfCur.max_age_d1_hours, null),
  };

  next.fx_universe_count = toNum(ctx.fx_universe_count ?? next.fx_universe_count, 0);

  const sleeve = isObj(next.fx_sleeve) ? { ...next.fx_sleeve } : {};
  const sleeveIn = isObj(ctx.fx_sleeve) ? ctx.fx_sleeve : {};
  next.fx_sleeve = {
    target_pct_min: toNum(sleeveIn.target_pct_min ?? sleeve.target_pct_min, 5),
    target_pct_max: toNum(sleeveIn.target_pct_max ?? sleeve.target_pct_max, 10),
    per_pair_pct_max: toNum(sleeveIn.per_pair_pct_max ?? sleeve.per_pair_pct_max, 3),
    default_pair_pct: toNum(sleeveIn.default_pair_pct ?? sleeve.default_pair_pct, 1.5),
  };
  return next;
}

function adjustConfidenceForMacro(conf, bias, base, quote, macroRegime) {
  let outConf = clamp(Math.round(toNum(conf, 0)), 0, 100);
  const regime = String(macroRegime || "").toUpperCase();
  if (!regime) return outConf;

  const safeHaven = new Set(["JPY", "CHF", "USD"]);
  const beta = new Set(["AUD", "NZD", "CAD"]);
  const baseSafe = safeHaven.has(base);
  const quoteSafe = safeHaven.has(quote);
  const baseBeta = beta.has(base);
  const quoteBeta = beta.has(quote);

  if (regime.includes("RISK-OFF")) {
    if ((bias === "BUY_BASE" && baseSafe && !quoteSafe) || (bias === "SELL_BASE" && quoteSafe && !baseSafe)) outConf += 8;
    if ((bias === "BUY_BASE" && baseBeta && !quoteBeta) || (bias === "SELL_BASE" && quoteBeta && !baseBeta)) outConf -= 8;
  } else if (regime.includes("RISK-ON")) {
    if ((bias === "BUY_BASE" && baseBeta && !quoteBeta) || (bias === "SELL_BASE" && quoteBeta && !baseBeta)) outConf += 8;
    if ((bias === "BUY_BASE" && baseSafe && !quoteSafe) || (bias === "SELL_BASE" && quoteSafe && !baseSafe)) outConf -= 8;
  }

  return clamp(outConf, 0, 100);
}

function normalizeFxPairObj(obj, fallbackMacroRegime) {
  const meta = parseFxMeta(obj?.symbol_internal || obj?.symbol || obj?.symbol_yahoo || obj?.pair);
  if (!meta) return null;

  const entry = toNum(obj?.entry ?? obj?.entry_price, null);
  const lastClose = toNum(obj?.last_close ?? obj?.lastClose ?? obj?.last_price ?? entry, null);
  const stop = toNum(obj?.stop ?? obj?.stop_price, null);
  const tp = toNum(obj?.tp ?? obj?.tp_price, null);
  const pWinPct = toNum(obj?.p_win_pct ?? obj?.pWinPct, null);
  const risk = toNum(obj?.risk ?? obj?.risk_score, null);
  const reward = toNum(obj?.reward ?? obj?.reward_score, null);
  const evR = toNum(obj?.ev_r ?? obj?.ev, null);
  const dataQuality = toNum(obj?.data_quality ?? obj?.data_quality_score, null);

  let bias = normalizeBias(obj?.directional_bias ?? obj?.fx_directional_bias);
  if (bias === "NEUTRAL" && entry != null && tp != null) {
    if (tp > entry) bias = "BUY_BASE";
    else if (tp < entry) bias = "SELL_BASE";
  }

  let confidence = toNum(obj?.confidence ?? obj?.fx_bias_confidence, null);
  if (!(confidence > 0)) {
    confidence = pWinPct != null ? pWinPct : 0;
  }
  const macroRegime = String(obj?.fx_macro_regime || fallbackMacroRegime || "Neutral");
  confidence = adjustConfidenceForMacro(confidence, bias, meta.base, meta.quote, macroRegime);

  return {
    pair: meta.pair,
    symbol_internal: meta.symbol_internal,
    symbol_yahoo: meta.symbol_yahoo,
    base_ccy: meta.base,
    quote_ccy: meta.quote,
    last_close: lastClose,
    quote_to_eur: toNum(obj?.quote_to_eur, null),
    eur_per_base: toNum(obj?.eur_per_base, null),
    directional_bias: bias,
    confidence,
    urgent_event_window: toBool(obj?.urgent_event_window ?? obj?.fx_urgent_event_window, false),
    entry,
    stop,
    tp,
    p_win_pct: pWinPct,
    risk,
    reward,
    ev_r: evR,
    data_quality: dataQuality,
    gates: String(obj?.gates ?? obj?.gate_summary ?? "OK"),
    rationale: String(obj?.rationale ?? obj?.fx_rationale ?? obj?.action_reason ?? "").trim(),
    as_of: String(obj?.as_of ?? obj?.asOf ?? "").trim() || null,
    fx_macro_regime: macroRegime,
    fx_macro_confidence: toNum(obj?.fx_macro_confidence, null),
  };
}

function pairQualityScore(p) {
  const biasScore = p.directional_bias !== "NEUTRAL" ? 1 : 0;
  const conf = toNum(p.confidence, 0);
  const ev = toNum(p.ev_r, 0);
  const dq = toNum(p.data_quality, 0);
  const hasPrice = toNum(p.last_close ?? p.entry, null) != null ? 1 : 0;
  return biasScore * 1e8 + conf * 1e6 + ev * 1e4 + dq * 1e2 + hasPrice;
}

function buildFxRates(pairs) {
  const edges = [];
  for (const p of pairs) {
    const px = toNum(p.last_close ?? p.entry, null);
    if (!(px > 0)) continue;
    if (!p.base_ccy || !p.quote_ccy) continue;
    edges.push({ base: p.base_ccy, quote: p.quote_ccy, px });
  }

  const eurPer = { EUR: 1.0 };
  let changed = true;
  let guard = 0;
  while (changed && guard < 30) {
    changed = false;
    guard += 1;
    for (const e of edges) {
      if (eurPer[e.base] && !eurPer[e.quote]) {
        eurPer[e.quote] = eurPer[e.base] / e.px;
        changed = true;
      }
      if (eurPer[e.quote] && !eurPer[e.base]) {
        eurPer[e.base] = eurPer[e.quote] * e.px;
        changed = true;
      }
    }
  }
  return eurPer;
}

function enrichConversions(pairs, fxRates) {
  for (const p of pairs) {
    const q2e = toNum(p.quote_to_eur, null) ?? toNum(fxRates[p.quote_ccy], null);
    p.quote_to_eur = q2e;
    const px = toNum(p.last_close ?? p.entry, null);
    p.eur_per_base = (q2e != null && px != null) ? px * q2e : null;
  }
  return pairs;
}

for (const it of incoming) {
  const j = it.json || {};

  if (!out.run) out.run = pickObject(j, ["run", "Run", "decisionMeta", "meta"]);
  const pb = pickObject(j, ["portfolioBrief", "PortfolioBrief"]);
  if (pb) out.portfolioBrief = mergePortfolioBrief(out.portfolioBrief, pb);
  if (!out.config) out.config = pickObject(j, ["config", "cfg", "settings"]);

  if (isObj(j.portfolioDecisionMemory)) {
    for (const [sym, d] of Object.entries(j.portfolioDecisionMemory)) {
      if (sym && isObj(d)) decisionMemoryMerged[normSymbol(sym)] = d;
    }
  }
  if (isObj(j.portfolioExecutionMemory)) {
    for (const [sym, d] of Object.entries(j.portfolioExecutionMemory)) {
      if (sym && isObj(d)) executionMemoryMerged[normSymbol(sym)] = d;
    }
  }
  if (Array.isArray(j.recentUnexecutedIdeas) && j.recentUnexecutedIdeas.length) {
    recentIdeasMerged.push(...j.recentUnexecutedIdeas.filter((x) => isObj(x)));
  }

  if (!out.sector_brief) out.sector_brief = pickText(j, ["sector_brief", "sectorBrief", "sector", "sector_momentum"]);
  if (!out.opportunity_brief) out.opportunity_brief = pickText(j, ["opportunity_brief", "opportunityBrief", "opportunity", "matrix"]);

  if (!out.opportunity_pack && isObj(j.opportunity_pack)) out.opportunity_pack = j.opportunity_pack;
  if (!out.opportunity_stats && isObj(j.opportunity_stats)) out.opportunity_stats = j.opportunity_stats;
  if (!out.matrix_thresholds && isObj(j.matrix_thresholds)) out.matrix_thresholds = j.matrix_thresholds;

  if (!out.fx_macro && isObj(j.fx_macro)) out.fx_macro = j.fx_macro;
  if (isObj(j.fx_context)) out.fx_context = mergeFxContext(out.fx_context, j.fx_context, j.fx_macro, j?.opportunity_pack?.generatedAt);
  if (isObj(j.fx_rates)) out.fx_rates = { ...(out.fx_rates || {}), ...j.fx_rates };

  if (Array.isArray(j.fx_pairs) && j.fx_pairs.length) out.fx_pairs.push(...j.fx_pairs);
  if (Array.isArray(j.fx_candidates) && j.fx_candidates.length) out.fx_candidates.push(...j.fx_candidates);

  if (!out.sector_brief) out.sector_brief = pickText(j, ["text", "brief", "output"]);
  if (!out.opportunity_brief) out.opportunity_brief = pickText(j, ["text", "brief", "output"]);
}

if (!out.run) out.run = {};
if (!out.portfolioBrief) out.portfolioBrief = {};
out.portfolioBrief = enrichPortfolioBriefWithMemory(
  out.portfolioBrief,
  decisionMemoryMerged,
  executionMemoryMerged,
  dedupeIdeas(recentIdeasMerged),
);

if (!out.config) {
  out.config = {
    strategyVersion: out.run.strategyVersion || "strategy_v3",
    configVersion: out.run.configVersion || "config_v3",
    promptVersion: out.run.promptVersion || "prompt_v3",
    enable_fx: toBool(out.run.enable_fx, true),
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

const fallbackMacroRegime = String(out.fx_context?.macro_regime || out.fx_macro?.market_regime || "Neutral");
const derivedFxPairs = [];
if (Array.isArray(out.opportunity_pack?.rows)) {
  for (const r of out.opportunity_pack.rows) {
    const sym = String(r?.symbol_internal || r?.symbol || r?.symbol_yahoo || "").trim();
    const ac = String(r?.asset_class || "").toUpperCase();
    if (!(ac === "FX" || isFxSymbol(sym))) continue;
    const norm = normalizeFxPairObj(r, fallbackMacroRegime);
    if (norm) derivedFxPairs.push(norm);
  }
}

for (const p of out.fx_pairs) {
  const norm = normalizeFxPairObj(p, fallbackMacroRegime);
  if (norm) derivedFxPairs.push(norm);
}
for (const c of out.fx_candidates) {
  const norm = normalizeFxPairObj(c, fallbackMacroRegime);
  if (norm) derivedFxPairs.push(norm);
}

const byPair = new Map();
for (const p of derivedFxPairs) {
  const prev = byPair.get(p.pair);
  if (!prev || pairQualityScore(p) > pairQualityScore(prev)) byPair.set(p.pair, p);
}
let fxPairs = Array.from(byPair.values());

const fxRates = { EUR: 1.0 };
if (isObj(out.fx_rates)) {
  for (const [k, v] of Object.entries(out.fx_rates)) {
    const n = toNum(v, null);
    if (n > 0) fxRates[String(k).toUpperCase()] = n;
  }
}
const graphRates = buildFxRates(fxPairs);
for (const [k, v] of Object.entries(graphRates)) {
  if (!(toNum(fxRates[k], null) > 0) && toNum(v, null) > 0) fxRates[k] = v;
}

fxPairs = enrichConversions(fxPairs, fxRates);
fxPairs.sort((a, b) => {
  const c = toNum(b.confidence, 0) - toNum(a.confidence, 0);
  if (c !== 0) return c;
  return toNum(b.ev_r, 0) - toNum(a.ev_r, 0);
});

const contextAsOf = out.fx_context?.as_of
  || out.fx_macro?.as_of
  || out.fx_macro?.asOf
  || out.opportunity_pack?.generatedAt
  || new Date().toISOString();
const macroRegime = String(out.fx_context?.macro_regime || out.fx_macro?.market_regime || fxPairs[0]?.fx_macro_regime || "Neutral");
const macroConfidence = clamp(Math.round(toNum(out.fx_context?.macro_confidence ?? out.fx_macro?.confidence ?? fxPairs[0]?.fx_macro_confidence, 0)), 0, 100);

out.fx_context = {
  as_of: contextAsOf,
  macro_regime: macroRegime,
  macro_confidence: macroConfidence,
  signals_freshness: {
    max_age_h1_hours: toNum(out.fx_context?.signals_freshness?.max_age_h1_hours, null),
    max_age_d1_hours: toNum(out.fx_context?.signals_freshness?.max_age_d1_hours, null),
  },
  fx_universe_count: fxPairs.length,
  fx_sleeve: {
    target_pct_min: toNum(out.fx_context?.fx_sleeve?.target_pct_min, 5),
    target_pct_max: toNum(out.fx_context?.fx_sleeve?.target_pct_max, 10),
    per_pair_pct_max: toNum(out.fx_context?.fx_sleeve?.per_pair_pct_max, 3),
    default_pair_pct: toNum(out.fx_context?.fx_sleeve?.default_pair_pct, 1.5),
  },
};

const blockedGateTokens = ["DATA_QUALITY_LOW", "INVALID_OPTIONS_STATE", "LIQUIDITY_STRESS"];
const inverseDedup = new Map();
for (const p of fxPairs) {
  const bias = normalizeBias(p.directional_bias);
  const conf = toNum(p.confidence, 0);
  const dq = toNum(p.data_quality, 0);
  const gates = parseGates(p.gates);
  const blocked = gates.some((g) => blockedGateTokens.includes(g));
  if (bias === "NEUTRAL" || conf < 50 || dq < 60 || blocked) continue;

  const inv = inversePair(p.pair);
  const key = [p.pair, inv].sort().join("|");
  const prev = inverseDedup.get(key);
  if (!prev || pairQualityScore(p) > pairQualityScore(prev)) inverseDedup.set(key, p);
}

const fxCandidates = Array.from(inverseDedup.values())
  .sort((a, b) => {
    const c = toNum(b.confidence, 0) - toNum(a.confidence, 0);
    if (c !== 0) return c;
    return toNum(b.ev_r, 0) - toNum(a.ev_r, 0);
  })
  .slice(0, 10);

out.fx_pairs = fxPairs;
out.fx_candidates = fxCandidates;
out.fx_rates = fxRates;
out.fx_macro = {
  as_of: out.fx_context.as_of,
  market_regime: out.fx_context.macro_regime,
  confidence: out.fx_context.macro_confidence,
};

const topCandidates = fxCandidates.slice(0, 6).map((p) => {
  const bias = p.directional_bias === "BUY_BASE" ? "long base" : "short base";
  return `${p.pair} ${bias} (conf ${toNum(p.confidence, 0)}, ev ${toNum(p.ev_r, 0).toFixed(2)})`;
});
out.fx_candidates_summary = topCandidates.length
  ? `Top FX candidates (max 6): ${topCandidates.join(", ")}`
  : "Top FX candidates (max 6): none";

if (out.opportunity_pack && isObj(out.opportunity_pack)) {
  out.opportunity_pack.fx_context = out.fx_context;
  out.opportunity_pack.fx_rates = out.fx_rates;
  out.opportunity_pack.fx_candidates = out.fx_candidates;
}

const enableFx = toBool(out.run.enable_fx ?? out.config.enable_fx, true);
const universeScope = Array.isArray(out.run.universe_scope)
  ? out.run.universe_scope
  : ["CURRENCY", "EQUITY", "ETF", "MUTUALFUND"];

const inputSnapshot = {
  portfolioUpdatedAt: getPortfolioUpdatedAt(out.portfolioBrief),
  technicalUpdatedAt: out.opportunity_pack?.generatedAt || null,
  researchUpdatedAt: out.opportunity_pack?.generatedAt || null,
  newsGeneratedAt: out.opportunity_pack?.generatedAt || null,
  universe_scope: universeScope,
  fxUniverseCount: enableFx ? out.fx_context.fx_universe_count : 0,
  fxCandidatesCount: enableFx ? out.fx_candidates.length : 0,
  fxSignalsFreshness: out.fx_context.signals_freshness,
  fxMacroAsOf: out.fx_context.as_of,
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
  fx_candidates_count: out.fx_candidates.length,
  fx_rates_count: Object.keys(out.fx_rates || {}).length,
  enable_fx: enableFx,
};

return [{ json: out }];

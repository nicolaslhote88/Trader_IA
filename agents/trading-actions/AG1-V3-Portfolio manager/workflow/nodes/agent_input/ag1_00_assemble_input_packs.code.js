// AG1.00 - Assemble Input Packs (actions/ETF/crypto)
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
};

const decisionMemoryMerged = {};
const executionMemoryMerged = {};
const recentIdeasMerged = [];

function isObj(x) { return !!x && typeof x === "object" && !Array.isArray(x); }
function normSymbol(v) { return String(v ?? "").trim().toUpperCase(); }

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
  for (const k of ["generatedAt", "totalValue", "cash", "exposurePct", "positionsCount", "marketValue", "source"]) {
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
      lines.push(`- ${p.symbol || p.Symbol}: qty=${p.quantity ?? p.qty ?? "n/a"} last=${p.lastPrice ?? p.price ?? "n/a"} | lastAction=${d.action || "n/a"} | exec=${e.lastExecutionStatus || "NO_ORDER"}`);
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
  if (!candidates.length) return String(pb.generatedAt || "").trim() || null;
  candidates.sort((a, b) => (Date.parse(b) || 0) - (Date.parse(a) || 0));
  return candidates[0] || null;
}

for (const it of incoming) {
  const j = it.json || {};
  if (!out.run) out.run = pickObject(j, ["run", "Run", "decisionMeta", "meta"]);
  const pb = pickObject(j, ["portfolioBrief", "PortfolioBrief"]);
  if (pb) out.portfolioBrief = mergePortfolioBrief(out.portfolioBrief, pb);
  if (!out.config) out.config = pickObject(j, ["config", "cfg", "settings"]);
  if (isObj(j.portfolioDecisionMemory)) {
    for (const [sym, d] of Object.entries(j.portfolioDecisionMemory)) if (sym && isObj(d)) decisionMemoryMerged[normSymbol(sym)] = d;
  }
  if (isObj(j.portfolioExecutionMemory)) {
    for (const [sym, d] of Object.entries(j.portfolioExecutionMemory)) if (sym && isObj(d)) executionMemoryMerged[normSymbol(sym)] = d;
  }
  if (Array.isArray(j.recentUnexecutedIdeas) && j.recentUnexecutedIdeas.length) recentIdeasMerged.push(...j.recentUnexecutedIdeas.filter(isObj));
  if (!out.sector_brief) out.sector_brief = pickText(j, ["sector_brief", "sectorBrief", "sector", "sector_momentum"]);
  if (!out.opportunity_brief) out.opportunity_brief = pickText(j, ["opportunity_brief", "opportunityBrief", "opportunity", "matrix"]);
  if (!out.opportunity_pack && isObj(j.opportunity_pack)) out.opportunity_pack = j.opportunity_pack;
  if (!out.opportunity_stats && isObj(j.opportunity_stats)) out.opportunity_stats = j.opportunity_stats;
  if (!out.matrix_thresholds && isObj(j.matrix_thresholds)) out.matrix_thresholds = j.matrix_thresholds;
  if (!out.sector_brief) out.sector_brief = pickText(j, ["text", "brief", "output"]);
  if (!out.opportunity_brief) out.opportunity_brief = pickText(j, ["text", "brief", "output"]);
}

if (!out.run) out.run = {};
if (!out.portfolioBrief) out.portfolioBrief = {};
out.portfolioBrief = enrichPortfolioBriefWithMemory(out.portfolioBrief, decisionMemoryMerged, executionMemoryMerged, dedupeIdeas(recentIdeasMerged));
if (!out.config) out.config = { strategyVersion: out.run.strategyVersion || "strategy_v3", configVersion: out.run.configVersion || "config_v3", promptVersion: out.run.promptVersion || "prompt_v3" };
if (!out.sector_brief) out.sector_brief = "";
if (!out.opportunity_brief) out.opportunity_brief = "";
if (!out.opportunity_pack && out.opportunity_brief) out.opportunity_pack = { generatedAt: new Date().toISOString(), rows: [], stats: out.opportunity_stats || {}, thresholds: out.matrix_thresholds || {} };

const universeScope = Array.isArray(out.run.universe_scope)
  ? out.run.universe_scope.filter((x) => String(x || "").toUpperCase() !== "CURRENCY")
  : ["EQUITY", "ETF", "CRYPTO"];

const inputSnapshot = {
  portfolioUpdatedAt: getPortfolioUpdatedAt(out.portfolioBrief),
  technicalUpdatedAt: out.opportunity_pack?.generatedAt || null,
  researchUpdatedAt: out.opportunity_pack?.generatedAt || null,
  newsGeneratedAt: out.opportunity_pack?.generatedAt || null,
  universe_scope: universeScope,
};

out.run = { ...out.run, strategyVersion: out.run.strategyVersion || out.config.strategyVersion || "strategy_v3", configVersion: out.run.configVersion || out.config.configVersion || "config_v3", promptVersion: out.run.promptVersion || out.config.promptVersion || "prompt_v3", universe_scope: universeScope, inputSnapshot };
out.__debug = { incomingItems: incoming.length, has_run: !!Object.keys(out.run).length, has_portfolioBrief: !!Object.keys(out.portfolioBrief).length, has_sector_brief: !!out.sector_brief, has_opportunity_brief: !!out.opportunity_brief, has_opportunity_pack: !!out.opportunity_pack };

return [{ json: out }];

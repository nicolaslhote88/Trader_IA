// AG1.00 - Assemble compact, authoritative input packs.
// Mode: Run Once for All Items.

const incoming = $input.all();

function isObj(x) { return !!x && typeof x === "object" && !Array.isArray(x); }
function normSymbol(v) { return String(v ?? "").trim().toUpperCase(); }
function toNumOrNull(v) { const n = Number(v); return Number.isFinite(n) ? n : null; }
function mergeDeep(base, value) {
  const out = isObj(base) ? { ...base } : {};
  if (!isObj(value)) return out;
  for (const [key, next] of Object.entries(value)) {
    out[key] = isObj(next) ? mergeDeep(out[key], next) : next;
  }
  return out;
}
function firstObject(j, keys) {
  for (const key of keys) if (isObj(j?.[key])) return j[key];
  return null;
}
function firstText(j, keys) {
  for (const key of keys) {
    const value = String(j?.[key] ?? "").trim();
    if (value) return value;
  }
  return "";
}
function latestTimestamp(values) {
  return values.filter(Boolean).sort((a, b) => (Date.parse(b) || 0) - (Date.parse(a) || 0))[0] || null;
}

let run = {};
let config = {};
let meta = {};
let portfolioBrief = {};
let sectorBrief = "";
let opportunityBrief = "";
let opportunityPack = null;
let opportunityStats = null;
let matrixThresholds = null;
const decisionMemory = {};
const executionMemory = {};
const recentIdeas = [];

for (const item of incoming) {
  const j = item.json || {};
  run = mergeDeep(run, firstObject(j, ["run", "Run", "decisionMeta"]) || {});
  config = mergeDeep(config, firstObject(j, ["config", "cfg", "settings"]) || {});
  meta = mergeDeep(meta, firstObject(j, ["meta", "Meta"]) || {});

  const pb = firstObject(j, ["portfolioBrief", "PortfolioBrief"]);
  if (pb) {
    portfolioBrief = mergeDeep(portfolioBrief, pb);
    if (Array.isArray(pb.positions) && pb.positions.length) portfolioBrief.positions = pb.positions;
  }
  for (const [symbol, value] of Object.entries(j.portfolioDecisionMemory || {})) {
    if (isObj(value)) decisionMemory[normSymbol(symbol)] = value;
  }
  for (const [symbol, value] of Object.entries(j.portfolioExecutionMemory || {})) {
    if (isObj(value)) executionMemory[normSymbol(symbol)] = value;
  }
  if (Array.isArray(j.recentUnexecutedIdeas)) recentIdeas.push(...j.recentUnexecutedIdeas.filter(isObj));
  if (!sectorBrief) sectorBrief = firstText(j, ["sector_brief", "sectorBrief", "sector_momentum"]);
  if (!opportunityBrief) opportunityBrief = firstText(j, ["opportunity_brief", "opportunityBrief"]);
  if (!opportunityPack && isObj(j.opportunity_pack)) opportunityPack = j.opportunity_pack;
  if (!opportunityStats && isObj(j.opportunity_stats)) opportunityStats = j.opportunity_stats;
  if (!matrixThresholds && isObj(j.matrix_thresholds)) matrixThresholds = j.matrix_thresholds;
}

const positions = (Array.isArray(portfolioBrief.positions) ? portfolioBrief.positions : [])
  .filter((p) => {
    const symbol = normSymbol(p?.symbol || p?.Symbol);
    return symbol && !["CASH_EUR", "__META__"].includes(symbol);
  })
  .map((p) => {
    const symbol = normSymbol(p.symbol || p.Symbol);
    const lastDecision = isObj(p.lastDecision) ? p.lastDecision : decisionMemory[symbol];
    const execution = isObj(p.executionMemory) ? p.executionMemory : executionMemory[symbol];
    return {
      symbol,
      quantity: toNumOrNull(p.quantity ?? p.Quantity ?? p.qty),
      lastPrice: toNumOrNull(p.lastPrice ?? p.LastPrice ?? p.price),
      marketValue: toNumOrNull(p.marketValue ?? p.MarketValue ?? p.market_value),
      weightPct: toNumOrNull(p.weightPct ?? p.WeightPct ?? p.weight_pct),
      sector: String(p.sector ?? p.Sector ?? "").trim() || "UNKNOWN",
      unrealizedPnlPct: toNumOrNull(p.unrealizedPnlPct ?? p.UnrealizedPnlPct),
      lastAction: String(lastDecision?.action || "").trim() || null,
      lastExecutionStatus: String(execution?.lastExecutionStatus || "").trim() || null,
    };
  });

const eligibleSymbols = new Set((opportunityPack?.rows || []).map((row) => normSymbol(row?.symbol)).filter(Boolean));
const heldSymbols = new Set(positions.map((p) => p.symbol));
const memoryIdeas = recentIdeas
  .filter((idea) => eligibleSymbols.has(normSymbol(idea.symbol)) || heldSymbols.has(normSymbol(idea.symbol)))
  .sort((a, b) => (Date.parse(b?.ts || "") || 0) - (Date.parse(a?.ts || "") || 0))
  .slice(0, 8)
  .map((idea) => ({
    symbol: normSymbol(idea.symbol),
    action: String(idea.action || "").toUpperCase() || null,
    ts: idea.ts || null,
    status: idea.executionStatus || idea.status || null,
  }));

const summary = isObj(portfolioBrief.summary) ? portfolioBrief.summary : {};
const portfolioPack = {
  generatedAt: portfolioBrief.generatedAt || null,
  totalValueEUR: toNumOrNull(portfolioBrief.totalValue ?? summary.totalValue ?? summary.totalPortfolioValueEUR),
  cashEUR: toNumOrNull(portfolioBrief.cash ?? summary.cash ?? summary.cashEUR),
  exposurePct: toNumOrNull(portfolioBrief.exposurePct ?? summary.exposurePct),
  positionsCount: positions.length,
  positions,
  relevantRecentIdeas: memoryIdeas,
};

if (!opportunityPack) {
  opportunityPack = {
    generatedAt: new Date().toISOString(),
    stats: opportunityStats || {},
    thresholds: matrixThresholds || {},
    rows: [],
  };
}

const universeScope = Array.isArray(run.universe_scope)
  ? run.universe_scope.filter((x) => String(x || "").toUpperCase() !== "CURRENCY")
  : ["EQUITY", "ETF"];
const portfolioDates = positions.map((p) => p.updatedAt).filter(Boolean);
const inputSnapshot = {
  portfolioUpdatedAt: latestTimestamp([portfolioBrief.portfolioUpdatedAt, portfolioBrief.updatedAt, portfolioBrief.generatedAt, ...portfolioDates]),
  technicalUpdatedAt: opportunityPack.generatedAt || null,
  researchUpdatedAt: opportunityPack.generatedAt || null,
  newsGeneratedAt: opportunityPack.newsGeneratedAt || null,
  universe_scope: universeScope,
};

config = {
  strategyVersion: run.strategyVersion || "strategy_v4_consensus",
  configVersion: run.configVersion || "ag1_v4_consensus_v1",
  promptVersion: "prompt_v4_consensus_global_context_v3",
  ...config,
};
run = {
  ...run,
  strategyVersion: run.strategyVersion || config.strategyVersion,
  configVersion: run.configVersion || config.configVersion,
  promptVersion: "prompt_v4_consensus_global_context_v3",
  model: "ag1_v4_consensus",
  universe_scope: universeScope,
  inputSnapshot,
};

return [{
  json: {
    run,
    config,
    meta,
    portfolioBrief,
    portfolio_pack: portfolioPack,
    sector_brief: sectorBrief,
    opportunity_brief: opportunityBrief,
    opportunity_pack: opportunityPack,
    opportunity_stats: opportunityStats || opportunityPack.stats || {},
    matrix_thresholds: matrixThresholds || opportunityPack.thresholds || {},
    __debug: {
      incomingItems: incoming.length,
      configKeys: Object.keys(config).sort(),
      positions: positions.length,
      opportunities: Array.isArray(opportunityPack.rows) ? opportunityPack.rows.length : 0,
      relevantRecentIdeas: memoryIdeas.length,
    },
  },
}];

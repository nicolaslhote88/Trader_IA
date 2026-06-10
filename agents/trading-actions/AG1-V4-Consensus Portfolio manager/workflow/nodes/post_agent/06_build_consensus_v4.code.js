// AG1 V4 - Build 2/3 consensus from GPT, Grok and Gemini proposals.
// Mode: Run Once for All Items.

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }
function toNumOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}
function clampText(v, max = 0) {
  const s = String(v ?? "").replace(/\s+/g, " ").trim();
  return max > 0 ? s.slice(0, max) : s;
}
function normSymbol(v) { return String(v ?? "").trim().toUpperCase(); }
function normAction(v) { return String(v ?? "").trim().toUpperCase(); }
function deepClone(obj) {
  try { return JSON.parse(JSON.stringify(obj)); } catch { return obj; }
}
function safeArray(v) { return Array.isArray(v) ? v : []; }

function normAssetClass(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (!s || ["EQUITY", "STOCK", "STK"].includes(s)) return "EQUITY";
  if (s === "ETF") return "ETF";
  if (s === "CRYPTO") return "CRYPTO";
  return s;
}

function isFxSymbol(action) {
  const symbol = normSymbol(action?.symbol_internal || action?.symbol);
  const yahoo = String(action?.symbol_yahoo || "").trim().toUpperCase();
  const ac = normAssetClass(action?.assetClass ?? action?.AssetClass);
  return symbol.startsWith("FX:") || yahoo.endsWith("=X") || ["FX", "FOREX", "CURRENCY"].includes(ac);
}

function normalizeIntent(actionName) {
  const a = normAction(actionName);
  if (["OPEN", "INCREASE", "BUY"].includes(a)) return "BUY";
  if (["DECREASE", "CLOSE", "SELL"].includes(a)) return "SELL";
  if (a === "HOLD") return "HOLD";
  if (a === "WATCH") return "WATCH";
  return "UNKNOWN";
}

function sideForIntent(intent) {
  if (intent === "BUY") return "BUY";
  if (intent === "SELL") return "SELL";
  return null;
}

function normalizeOrderType(v) {
  const s = String(v ?? "").trim().toUpperCase();
  if (["LMT", "LIMIT"].includes(s)) return "LIMIT";
  if (["MKT", "MARKET"].includes(s)) return "MARKET";
  return s || "MARKET";
}

function pickLimitPrice(action) {
  return toNumOrNull(action?.entryPlan?.limitPrice ?? action?.limitPrice);
}

function pickTargetQty(action) {
  return toNumOrNull(action?.targetQty ?? action?.target_qty);
}

function pickTargetWeightPct(action) {
  return toNumOrNull(action?.targetWeightPct ?? action?.target_weight_pct);
}

function pickConfidence(action) {
  const n = toNumOrNull(action?.confidence);
  if (n === null) return null;
  return Math.max(0, Math.min(100, n));
}

function median(values) {
  const arr = values.filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
  if (!arr.length) return null;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}

function conservativeQty(values) {
  const arr = values.filter((x) => Number.isFinite(x) && x > 0);
  if (!arr.length) return null;
  return Math.floor(Math.min(...arr));
}

function conservativeWeight(values) {
  const arr = values.filter((x) => Number.isFinite(x) && x > 0);
  if (!arr.length) return null;
  return Math.min(...arr);
}

function inferModelKey(item, index) {
  const raw = String(
    item.model_key ||
    item.modelKey ||
    item.llm_model_key ||
    item.llmModelKey ||
    item.model ||
    item.modelName ||
    item.model_name ||
    ""
  ).trim().toLowerCase();
  if (raw.includes("grok") || raw.includes("xai")) return "grok41_reasoning";
  if (raw.includes("gemini") || raw.includes("google")) return "gemini30_pro";
  if (raw.includes("gpt") || raw.includes("openai") || raw.includes("chatgpt")) return "chatgpt52";
  return ["chatgpt52", "grok41_reasoning", "gemini30_pro"][index] || `model_${index + 1}`;
}

function extractDecision(item) {
  if (isObj(item.output) && Array.isArray(item.output.actions)) return item.output;
  if (isObj(item.agentDecision) && Array.isArray(item.agentDecision.actions)) return item.agentDecision;
  if (Array.isArray(item.output)) return { actions: item.output };
  return isObj(item.output) ? item.output : {};
}

function buildPortfolioSummary(context) {
  if (isObj(context.portfolioSummary)) return context.portfolioSummary;
  const pb = isObj(context.portfolioBrief) ? context.portfolioBrief : {};
  const summary = isObj(pb.summary) ? pb.summary : {};
  const cashEUR = toNumOrNull(pb.cash ?? summary.cash ?? summary.cashEUR) ?? 10000;
  const totalPortfolioValueEUR = toNumOrNull(pb.totalValue ?? summary.totalValue ?? summary.totalPortfolioValueEUR) ?? cashEUR;
  return {
    cashEUR,
    totalPortfolioValueEUR,
    marketValueEUR: toNumOrNull(pb.marketValue ?? summary.marketValue) ?? Math.max(0, totalPortfolioValueEUR - cashEUR),
    exposurePct: toNumOrNull(pb.exposurePct ?? summary.exposurePct) ?? 0,
    positions: safeArray(pb.positions),
  };
}

function buildPositionQtyMap(portfolioSummary) {
  const out = {};
  for (const p of safeArray(portfolioSummary.positions)) {
    const s = normSymbol(p?.symbol || p?.Symbol);
    if (!s || s === "CASH_EUR" || s === "__META__") continue;
    const qty = toNumOrNull(p?.quantity ?? p?.Quantity ?? p?.qty) ?? 0;
    out[s] = qty;
  }
  return out;
}

function selectActionName(intent, votes, currentQty) {
  const counts = {};
  for (const v of votes) {
    counts[v.action] = (counts[v.action] || 0) + 1;
  }
  if (intent === "SELL") {
    if ((counts.CLOSE || 0) >= 2) return "CLOSE";
    return currentQty > 0 ? "DECREASE" : "CLOSE";
  }
  if (intent === "BUY") {
    if (currentQty > 0) return "INCREASE";
    if ((counts.INCREASE || 0) > (counts.OPEN || 0) && currentQty > 0) return "INCREASE";
    return "OPEN";
  }
  return intent;
}

function buildSelectedAction(group, posQtyMap, runId) {
  const votes = group.votes;
  const representative = deepClone(votes[0].rawAction || {});
  const symbol = group.symbol;
  const intent = group.intent;
  const side = sideForIntent(intent);
  const currentQty = posQtyMap[symbol] || 0;
  const selectedAction = selectActionName(intent, votes, currentQty);
  const qty = conservativeQty(votes.map((v) => v.targetQty));
  const weight = conservativeWeight(votes.map((v) => v.targetWeightPct));
  const limits = votes.map((v) => v.limitPrice).filter((x) => Number.isFinite(x) && x > 0);
  const minLimit = limits.length ? Math.min(...limits) : null;
  const maxLimit = limits.length ? Math.max(...limits) : null;
  const limitSpreadPct = minLimit && maxLimit ? ((maxLimit - minLimit) / minLimit) * 100 : 0;
  const limitCompatible = limits.length < 2 || limitSpreadPct <= 5;
  const selectedLimit = limits.length ? (side === "BUY" ? minLimit : maxLimit) : null;
  const confidence = Math.round(median(votes.map((v) => v.confidence).filter((x) => x !== null)) ?? 50);
  const modelKeys = votes.map((v) => v.modelKey).sort();
  const consensusId = `CONS_${runId}_${symbol}_${intent}`;

  if (!limitCompatible) {
    return {
      decision: {
        consensus_id: consensusId,
        symbol,
        intent,
        action: selectedAction,
        side,
        vote_count: votes.length,
        valid_model_count: group.validModelCount,
        model_keys: modelKeys.join(","),
        status: "REJECTED_INCOMPATIBLE_LIMITS",
        reason: `Limit prices diverge by ${limitSpreadPct.toFixed(2)} pct`,
        selected_qty: qty,
        selected_weight_pct: weight,
        selected_limit_price: selectedLimit,
        confidence,
        payload_json: { votes },
      },
      action: null,
    };
  }

  const entryPlan = isObj(representative.entryPlan) ? { ...representative.entryPlan } : {};
  if (selectedLimit !== null) {
    entryPlan.orderType = "LIMIT";
    entryPlan.limitPrice = selectedLimit;
  } else if (!entryPlan.orderType) {
    entryPlan.orderType = side === "BUY" ? "LIMIT" : "MARKET";
  }

  const action = {
    ...representative,
    symbol,
    symbol_internal: symbol,
    assetClass: normAssetClass(representative.assetClass ?? representative.AssetClass),
    action: selectedAction,
    signal: side,
    confidence,
    targetQty: qty ?? representative.targetQty ?? null,
    targetWeightPct: weight ?? representative.targetWeightPct ?? null,
    entryPlan,
    rationale: clampText(
      `AG1 V4 consensus ${votes.length}/3 (${modelKeys.join(", ")}): ${representative.rationale || ""}`,
      2048
    ),
    consensusMeta: {
      consensusId,
      runId,
      intent,
      voteCount: votes.length,
      validModelCount: group.validModelCount,
      modelKeys,
      sizingRule: "min_qty_min_weight_conservative",
      limitRule: side === "BUY" ? "min_limit" : "max_limit",
    },
  };

  return {
    decision: {
      consensus_id: consensusId,
      symbol,
      intent,
      action: selectedAction,
      side,
      vote_count: votes.length,
      valid_model_count: group.validModelCount,
      model_keys: modelKeys.join(","),
      status: "CONSENSUS_APPROVED",
      reason: `Executable consensus ${votes.length}/3`,
      selected_qty: qty,
      selected_weight_pct: weight,
      selected_limit_price: selectedLimit,
      confidence,
      payload_json: { action, votes },
    },
    action,
  };
}

const items = $input.all().map((it) => it.json || {});
const context = items.find((it) => isObj(it.portfolioBrief) || isObj(it.opportunity_pack) || isObj(it.run)) || {};
const run = isObj(context.run) ? context.run : {};
const runId = String(run.runId || run.run_id || `RUN_${Date.now()}`);
const dbPath = String(context.db_path || context.ag1_db_path || run.db_path || "/files/duckdb/ag1_v4_consensus.duckdb");
const meta = isObj(context.meta) ? context.meta : { initialCapitalEUR: 10000 };
const portfolioSummary = buildPortfolioSummary(context);
const posQtyMap = buildPositionQtyMap(portfolioSummary);
const ts = new Date().toISOString();

const proposalItems = items.filter((it) => it !== context && (isObj(it.output) || isObj(it.agentDecision) || Array.isArray(it.output)));
const modelProposals = [];
const votes = [];
const warnings = [];

for (let i = 0; i < proposalItems.length; i += 1) {
  const item = proposalItems[i];
  const modelKey = inferModelKey(item, i);
  const decision = extractDecision(item);
  const actions = safeArray(decision.actions);
  const parseOk = Array.isArray(decision.actions);
  modelProposals.push({
    proposal_id: `PROP_${runId}_${modelKey}`,
    run_id: runId,
    ts,
    model_key: modelKey,
    model_name: item.modelName || item.model_name || item.model || modelKey,
    extractor_status: item.extractorStatus || item.extractor_status || null,
    parse_ok: parseOk,
    decision_json: decision,
    actions_json: actions,
    warnings_json: decision.dataCaveats || item.warnings || [],
    error: parseOk ? null : "No actions array in model output",
  });

  if (!parseOk) {
    warnings.push(`CONSENSUS_MODEL_INVALID:${modelKey}`);
    continue;
  }

  for (let aIdx = 0; aIdx < actions.length; aIdx += 1) {
    const action = actions[aIdx] || {};
    const symbol = normSymbol(action.symbol_internal || action.symbol);
    const rawAction = normAction(action.action);
    const intent = normalizeIntent(rawAction);
    const assetClass = normAssetClass(action.assetClass ?? action.AssetClass);
    const executable = ["BUY", "SELL"].includes(intent) && symbol && !isFxSymbol(action) && ["EQUITY", "ETF", "CRYPTO"].includes(assetClass);
    const vote = {
      vote_id: `VOTE_${runId}_${modelKey}_${symbol || "UNKNOWN"}_${intent}_${aIdx}`,
      run_id: runId,
      ts,
      model_key: modelKey,
      modelKey,
      symbol: symbol || "UNKNOWN",
      intent,
      action: rawAction,
      side: sideForIntent(intent),
      executable,
      confidence: pickConfidence(action),
      target_qty: pickTargetQty(action),
      targetQty: pickTargetQty(action),
      target_weight_pct: pickTargetWeightPct(action),
      targetWeightPct: pickTargetWeightPct(action),
      limit_price: pickLimitPrice(action),
      limitPrice: pickLimitPrice(action),
      rationale: clampText(action.rationale, 2048) || null,
      rawAction: action,
      payload_json: action,
    };
    votes.push(vote);
    if (symbol && isFxSymbol(action)) warnings.push(`CONSENSUS_VOTE_IGNORED_FX:${modelKey}:${symbol}`);
  }
}

const validModelCount = modelProposals.filter((p) => p.parse_ok).length;
const groups = new Map();
for (const vote of votes) {
  if (!vote.executable) continue;
  const key = `${vote.symbol}|${vote.intent}`;
  if (!groups.has(key)) groups.set(key, { symbol: vote.symbol, intent: vote.intent, votes: [], validModelCount });
  groups.get(key).votes.push(vote);
}

const consensusDecisions = [];
const approvedActions = [];
for (const group of groups.values()) {
  const uniqueModels = new Set(group.votes.map((v) => v.modelKey));
  if (uniqueModels.size < 2 || validModelCount < 2) {
    consensusDecisions.push({
      consensus_id: `CONS_${runId}_${group.symbol}_${group.intent}`,
      run_id: runId,
      ts,
      symbol: group.symbol,
      intent: group.intent,
      action: group.intent,
      side: sideForIntent(group.intent),
      vote_count: uniqueModels.size,
      valid_model_count: validModelCount,
      model_keys: Array.from(uniqueModels).sort().join(","),
      status: "NO_CONSENSUS",
      reason: "Fewer than two valid model votes for the same symbol and intent",
      payload_json: { votes: group.votes },
    });
    continue;
  }
  const selected = buildSelectedAction(group, posQtyMap, runId);
  consensusDecisions.push(selected.decision);
  for (const v of group.votes) v.consensus_id = selected.decision.consensus_id;
  if (selected.action) approvedActions.push(selected.action);
}

if (!approvedActions.length) {
  warnings.push(validModelCount < 2 ? "CONSENSUS_NO_TRADE:LESS_THAN_TWO_VALID_MODELS" : "CONSENSUS_NO_TRADE:NO_2_OF_3_EXECUTABLE_AGREEMENT");
}

const decision = approvedActions.length ? "TRADE" : "NO_TRADE";
const agentDecision = {
  decisionMeta: {
    runId,
    model: "ag1_v4_consensus",
    strategyVersion: run.strategyVersion || "strategy_v4_consensus",
    validModelCount,
    proposalCount: modelProposals.length,
    consensusRule: "at_least_2_of_3_same_symbol_and_intent",
  },
  portfolioPlan: {
    posture: decision === "TRADE" ? "CONSENSUS_TRADE" : "CONSENSUS_NO_TRADE",
    maxNewExposurePct: null,
  },
  actions: approvedActions,
  riskNotes: warnings,
  dataCaveats: modelProposals.flatMap((p) => safeArray(p.warnings_json)),
  backfillRequests: [],
  consensus: {
    modelProposals,
    votes,
    decisions: consensusDecisions,
  },
};

return [{
  json: {
    decision,
    commentary: `AG1 V4 consensus: ${approvedActions.length} executable action(s), ${validModelCount}/3 valid model outputs`,
    agentDecision,
    modelProposals,
    consensusVotes: votes,
    consensusDecisions,
    orders: [],
    warnings,
    run: { ...run, runId, model: "ag1_v4_consensus", db_path: dbPath },
    config: context.config || {},
    meta,
    portfolioSummary,
    ctx: {
      meta,
      config: context.config || {},
      run: { ...run, runId, model: "ag1_v4_consensus", db_path: dbPath },
      portfolioSummary,
      consensus: agentDecision.consensus,
    },
    transfer_pack: {
      db_path: dbPath,
      run: { ...run, runId, model: "ag1_v4_consensus", db_path: dbPath },
      config: context.config || {},
      meta,
      portfolioSummary,
    },
    db_path: dbPath,
  },
}];

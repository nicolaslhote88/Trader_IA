// Node 7 — Validate & Enforce Safety (v5 - Qty-first, Backward Compatible)
// Mode: Run Once for All Items
// Output: [{ json: { decision, commentary, agentDecision, orders, metrics, warnings, ctx } }]

// -------------------- HELPERS --------------------
function isObj(x) {
  return x && typeof x === "object" && !Array.isArray(x);
}

function safeJsonParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function toNumOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function clampText(s, n) {
  return String(s ?? "").replace(/\s+/g, " ").trim().slice(0, n);
}

function normSymbol(v) {
  return String(v ?? "").trim();
}

function uniq(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function deepClone(obj) {
  try { return JSON.parse(JSON.stringify(obj)); } catch { return obj; }
}

function pickFirstRelevantInput(all) {
  for (const it of all) {
    const j = it?.json ?? {};
    if (j.portfolioSummary || j.portfolioRows || j.ctx?.portfolioSummary) return j;
  }
  return all[0]?.json ?? {};
}

function extractNewsItems(input) {
  const candidates = [
    input?.market?.newsItems,
    input?.market?.marketNewsPack?.newsItems,
    input?.market?.marketNewsPack?.topItems,
    input?.market?.marketNewsPack?.items,
    input?.marketNewsPack?.newsItems,
    input?.marketNewsPack?.topItems,
    input?.marketNewsPack?.items,
    input?.newsItems,
    input?.news,
    input?.market?.news,
  ];
  for (const c of candidates) {
    if (Array.isArray(c) && c.length) return c;
  }
  return [];
}

function getPortfolioPositionsSymbols(portfolioSummary) {
  const pos = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
  return uniq(
    pos
      .map((p) => normSymbol(p.Symbol ?? p.symbol))
      .filter((s) => s && s !== "CASH_EUR" && s !== "__META__")
  );
}

function buildPricesFallbackFromPortfolio(portfolioSummary) {
  const pricesFallback = {};
  const posArr = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];

  for (const p of posArr) {
    const s = normSymbol(p.Symbol ?? p.symbol);
    if (!s || s === "CASH_EUR" || s === "__META__") continue;

    let last = Number(p.LastPrice ?? NaN);
    if (!Number.isFinite(last) || last <= 0) {
      const q = Number(p.Quantity ?? NaN);
      const mv = Number(p.MarketValue ?? NaN);
      if (Number.isFinite(q) && q > 0 && Number.isFinite(mv)) last = mv / q;
    }

    if (Number.isFinite(last) && last > 0) {
      pricesFallback[s] = {
        lastPrice: last,
        source: "portfolio_lastPrice",
        ts: String(p.UpdatedAt ?? ""),
        stale: false,
      };
    }
  }
  return pricesFallback;
}

// -------------------- AGENT DECISION PARSING --------------------
function extractAgentDecisionObject(input) {
  // 1) Already a structured object at input.output
  if (
    isObj(input.output) &&
    (Array.isArray(input.output.actions) || isObj(input.output.portfolioPlan) || isObj(input.output.decisionMeta))
  ) {
    return { ...input.output, _parseStatus: "OK_OBJECT_OUTPUT" };
  }

  // 2) Already a structured object at input.agentDecision
  if (isObj(input.agentDecision)) {
    return { ...input.agentDecision, _parseStatus: "OK_AGENTDECISION" };
  }

  // 3) Information Extractor can return actions[] directly
  if (
    Array.isArray(input.output) &&
    input.output.length > 0 &&
    input.output.every((x) => isObj(x) && String(x.action ?? "").length)
  ) {
    return {
      actions: input.output,
      dataCaveats: [],
      backfillRequests: [],
      _parseStatus: "OK_ACTIONS_ARRAY_OUTPUT",
    };
  }

  // 4) LangChain / OpenAI-ish message envelopes
  const msg0 = Array.isArray(input.output) ? input.output[0] : null;
  const content0 = msg0 && Array.isArray(msg0.content) ? msg0.content[0] : null;
  let decisionRaw = content0?.text ?? content0?.text?.text ?? null;

  if (isObj(decisionRaw)) return { ...decisionRaw, _parseStatus: "OK_OPENAI_OBJECT" };

  if (typeof decisionRaw === "string") {
    const parsed = safeJsonParse(decisionRaw);
    if (isObj(parsed)) return { ...parsed, _parseStatus: "OK_OPENAI_TEXT_JSON" };
    return { _rawText: decisionRaw, _parseStatus: "TEXT_NOT_JSON" };
  }

  if (decisionRaw == null) {
    return { _parseStatus: "MISSING_DECISION", _why: "No decision payload found." };
  }

  return { _rawValue: decisionRaw, _parseStatus: "UNEXPECTED_TYPE" };
}

function coerceAgentDecisionToExpectedShape(agentDecision) {
  if (!isObj(agentDecision)) {
    return { _parseStatus: "NOT_OBJECT", actions: [], dataCaveats: [], backfillRequests: [] };
  }
  const d = deepClone(agentDecision);
  if (!Array.isArray(d.actions)) d.actions = [];
  if (!Array.isArray(d.dataCaveats)) d.dataCaveats = [];
  if (!Array.isArray(d.backfillRequests)) d.backfillRequests = [];
  return d;
}

// -------------------- PORTFOLIO/PRICES --------------------
function computeTotalPortfolioValueEUR(portfolioSummary, prices) {
  const explicit = Number(portfolioSummary?.totalPortfolioValueEUR ?? portfolioSummary?.totalValueEUR ?? NaN);
  if (Number.isFinite(explicit) && explicit > 0) return explicit;

  const cash = Number(portfolioSummary?.cashEUR ?? 0);
  const posArr = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
  let equity = 0;

  for (const p of posArr) {
    const s = normSymbol(p.Symbol ?? p.symbol);
    if (!s || s === "CASH_EUR" || s === "__META__") continue;

    const q = Number(p.Quantity ?? 0);
    if (!Number.isFinite(q) || q === 0) continue;

    const last = Number(prices?.[s]?.lastPrice ?? p.LastPrice ?? NaN);
    if (Number.isFinite(last) && last > 0) equity += q * last;
    else {
      const mv = Number(p.MarketValue ?? NaN);
      if (Number.isFinite(mv)) equity += mv;
    }
  }
  return cash + equity;
}

function buildHoldingsMap(portfolioSummary, prices) {
  const map = {};
  const posArr = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];

  for (const p of posArr) {
    const s = normSymbol(p.Symbol ?? p.symbol);
    if (!s || s === "CASH_EUR" || s === "__META__") continue;

    const qty = Number(p.Quantity ?? 0);
    const last = Number(prices?.[s]?.lastPrice ?? p.LastPrice ?? NaN);

    map[s] = {
      Symbol: s,
      Quantity: Number.isFinite(qty) ? qty : 0,
      LastPrice: Number.isFinite(last) ? last : null,
    };
  }
  return map;
}

// -------------------- ORDER GENERATION (QTY-FIRST) --------------------
function generateOrdersFromActions(agentDecision, portfolioSummary, prices, feesConfig) {
  const warnings = [];
  const ordersSell = [];
  const ordersBuy = [];

  const actions = Array.isArray(agentDecision?.actions) ? agentDecision.actions : [];
  const holdings = buildHoldingsMap(portfolioSummary, prices);

  // Needed for legacy sizing via targetWeightPct
  const totalValue = computeTotalPortfolioValueEUR(portfolioSummary, prices);
  if (!Number.isFinite(totalValue) || totalValue <= 0) {
    warnings.push("PORTFOLIO_VALUE_INVALID: cannot size orders.");
    return { orders: [], warnings };
  }

  // Cash simulation to gate buys
  let cashSim = Number(portfolioSummary?.cashEUR ?? 0);
  const fixed = Number(feesConfig?.orderFeeFixedEUR ?? 0);
  const pct = Number(feesConfig?.orderFeePct ?? 0);

  // Helper: resolve current qty (0 if not held)
  const currentQty = (sym) => {
    const h = holdings[sym];
    const q = Number(h?.Quantity ?? 0);
    return Number.isFinite(q) ? q : 0;
  };

  for (const a of actions) {
    const sym = normSymbol(a?.symbol);
    const act = String(a?.action ?? "").toUpperCase();
    if (!sym) continue;
    if (!["OPEN", "INCREASE", "DECREASE", "CLOSE"].includes(act)) continue;

    const px = Number(prices?.[sym]?.lastPrice ?? NaN);
    if (!Number.isFinite(px) || px <= 0) {
      warnings.push(`SKIP_NO_PRICE:${sym}:${act}`);
      continue;
    }

    const q0 = currentQty(sym);

    const entryPlan = isObj(a?.entryPlan) ? a.entryPlan : {};
    const riskPlan = isObj(a?.riskPlan) ? a.riskPlan : {};

    // --- QTY-FIRST TARGET RESOLUTION (backward compatible) ---
    const targetQtyExplicit = toNumOrNull(a?.targetQty ?? a?.targetQuantity ?? null);
    const targetNotionalExplicit = toNumOrNull(a?.targetNotionalEUR ?? a?.targetNotional ?? null);

    let targetQty = null;

    if (act === "CLOSE") {
      targetQty = 0;
    } else if (targetQtyExplicit != null) {
      targetQty = targetQtyExplicit;
    } else if (targetNotionalExplicit != null) {
      targetQty = targetNotionalExplicit / px;
    } else if (a?.targetWeightPct != null) {
      const targetNotional = (Number(a.targetWeightPct) / 100) * totalValue;
      if (Number.isFinite(targetNotional)) targetQty = targetNotional / px;
    }

    if (targetQty == null || !Number.isFinite(targetQty)) {
      warnings.push(`SKIP_NO_TARGET:${sym}:${act}`);
      continue;
    }

    // Orders are integer quantities by design
    // - SELL: ceil to ensure reaching targetQty or below
    // - BUY : floor to avoid cash overshoot
    if (act === "DECREASE" || act === "CLOSE") {
      if (q0 <= 0) continue;

      const rawSell = q0 - targetQty;
      const qtyToSell = Math.min(q0, Math.ceil(rawSell));

      if (qtyToSell >= 1) {
        ordersSell.push({
          symbol: sym,
          side: "SELL",
          quantity: qtyToSell,
          price: null,
          rationale: clampText(a?.rationale, 240),
          orderType: String(entryPlan.orderType || "MARKET"),
          limitPrice: toNumOrNull(entryPlan.limitPrice),
          stopLossPct: toNumOrNull(riskPlan.stopLossPct),
          confidence: a?.confidence,
          priority: a?.priority,
        });
      }
    }

    if (act === "OPEN" || act === "INCREASE") {
      const rawBuy = targetQty - q0;
      const qtyToBuy = Math.floor(rawBuy);

      if (qtyToBuy >= 1) {
        ordersBuy.push({
          symbol: sym,
          side: "BUY",
          quantity: qtyToBuy,
          price: null,
          rationale: clampText(a?.rationale, 240),
          orderType: String(entryPlan.orderType || "MARKET"),
          limitPrice: toNumOrNull(entryPlan.limitPrice),
          stopLossPct: toNumOrNull(riskPlan.stopLossPct),
          confidence: a?.confidence,
          priority: a?.priority,
        });
      }
    }
  }

  // Execution simulation: sells first, then buys if cash is sufficient
  const finalOrders = [];

  for (const o of ordersSell) {
    finalOrders.push(o);
    const px = Number(prices?.[o.symbol]?.lastPrice ?? NaN);
    if (Number.isFinite(px) && px > 0) {
      const proceeds = (o.quantity * px) * (1 - pct) - fixed;
      cashSim += proceeds;
    }
  }

  for (const o of ordersBuy) {
    const px = Number(prices?.[o.symbol]?.lastPrice ?? NaN);
    if (Number.isFinite(px) && px > 0) {
      const cost = (o.quantity * px) * (1 + pct) + fixed;
      if (cashSim >= cost) {
        finalOrders.push(o);
        cashSim -= cost;
      } else {
        warnings.push(`SKIP_INSUFFICIENT_CASH:${o.symbol}`);
      }
    }
  }

  return { orders: finalOrders, warnings };
}

function estimateFeesEUR(orders, feesConfig, prices) {
  const fixed = Number(feesConfig?.orderFeeFixedEUR ?? 0);
  const pct = Number(feesConfig?.orderFeePct ?? 0);
  let total = 0;

  for (const o of orders) {
    const s = normSymbol(o.symbol);
    const q = Number(o.quantity ?? NaN);
    if (!s || !Number.isFinite(q) || q <= 0) continue;
    const px = Number(o.price ?? prices?.[s]?.lastPrice ?? NaN);
    if (!Number.isFinite(px) || px <= 0) continue;
    total += fixed + pct * (q * px);
  }
  return total;
}

// -------------------- MAIN --------------------
const all = $input.all();
const input = pickFirstRelevantInput(all);

const now = new Date();
const ts = now.toISOString();

const portfolioSummary = input.portfolioSummary ?? input.ctx?.portfolioSummary ?? { cashEUR: 0, positions: [] };
const meta0 = input.meta ?? input.ctx?.meta ?? {};
const config0 = input.config ?? input.ctx?.config ?? {};
const feesConfig = input.feesConfig ?? input.ctx?.feesConfig ?? { orderFeeFixedEUR: 0, orderFeePct: 0 };

const market0 = input.market ?? input.ctx?.market ?? {};
const upstreamPrices = isObj(market0.prices) ? market0.prices : {};
const upstreamNewsItems = extractNewsItems(input);

// Agent decision parsing
let agentDecision = extractAgentDecisionObject(input);
agentDecision = coerceAgentDecisionToExpectedShape(agentDecision);

// Prices merge (fallback from portfolio)
const pricesFallback = buildPricesFallbackFromPortfolio(portfolioSummary);
const mergedPrices = { ...pricesFallback, ...upstreamPrices };

// --- PATCH : INJECT LIMIT PRICES AS PROXY (only if missing market price) ---
const injectedPrices = [];
if (Array.isArray(agentDecision.actions)) {
  for (const action of agentDecision.actions) {
    const sym = normSymbol(action.symbol);
    const limit = toNumOrNull(action.entryPlan?.limitPrice);
    if (sym && limit && limit > 0) {
      const currentPrice = mergedPrices[sym]?.lastPrice;
      if (!Number.isFinite(Number(currentPrice)) || Number(currentPrice) <= 0) {
        mergedPrices[sym] = { lastPrice: limit, source: "agent_limit_proxy", stale: false };
        injectedPrices.push(sym);
      }
    }
  }
}

// Universe + coverage check
const heldSymbols = getPortfolioPositionsSymbols(portfolioSummary);
const tradeSymbols = uniq(
  (Array.isArray(agentDecision?.actions) ? agentDecision.actions : [])
    .filter((a) => ["OPEN", "INCREASE", "DECREASE", "CLOSE"].includes(String(a?.action ?? "").toUpperCase()))
    .map((a) => normSymbol(a?.symbol))
);

let pricedForRequested = 0;
const missingRequested = [];
for (const s of tradeSymbols) {
  const px = Number(mergedPrices?.[s]?.lastPrice ?? NaN);
  if (Number.isFinite(px) && px > 0) pricedForRequested += 1;
  else missingRequested.push(s);
}

const requestedSymbolsCount = tradeSymbols.length;
const okForTrading = requestedSymbolsCount ? (pricedForRequested === requestedSymbolsCount) : true;
const pricesCoverageRequested = requestedSymbolsCount ? (pricedForRequested / requestedSymbolsCount) : 1;

// Generate orders (qty-first)
let { orders, warnings: orderWarnings } = generateOrdersFromActions(
  agentDecision,
  portfolioSummary,
  mergedPrices,
  feesConfig
);

// Safety veto (hard gate)
const safetyWarnings = [];
if (!okForTrading) {
  if (tradeSymbols.length) {
    safetyWarnings.push(`SAFETY_VETO_MISSING_PRICES:${missingRequested.join(",")}`);
    orders = [];
  }
}

if (injectedPrices.length > 0) {
  safetyWarnings.push(`NOTICE: Used agent limit price as proxy for: ${injectedPrices.join(", ")}`);
}

// Warnings aggregation
const warnings = [];

// Parse problems
if (agentDecision?._parseStatus && !String(agentDecision._parseStatus).startsWith("OK")) {
  warnings.push(`AGENT_DECISION_PARSE:${agentDecision._parseStatus}`);
  if (agentDecision?._why) warnings.push(`AGENT_DECISION_PARSE_WHY:${agentDecision._why}`);
}

// Carry caveats
if (Array.isArray(agentDecision?.dataCaveats)) {
  for (const c of agentDecision.dataCaveats) warnings.push(String(c));
}

// Missing prices for held positions (informational)
for (const s of heldSymbols) {
  const px = Number(mergedPrices?.[s]?.lastPrice ?? NaN);
  if (!Number.isFinite(px) || px <= 0) warnings.push(`MISSING_PRICE_FOR_HELD:${s}`);
}

for (const w of orderWarnings) warnings.push(String(w));
for (const w of safetyWarnings) warnings.push(String(w));

// Metrics
const aiCostEUR = Number(input?.metrics?.aiCostEUR ?? 0);
const expectedFeesEUR = estimateFeesEUR(orders, feesConfig, mergedPrices);

// Decision
const decision = orders.length ? "TRADE" : "NO_TRADE";

// Build ctx for downstream nodes (8 / 8B / Sheets)
const runId =
  String(input?.runId ?? input?.ctx?.run?.runId ?? "").trim() ||
  String(agentDecision?.decisionMeta?.run_id ?? "").trim() ||
  "";

const promptVersion =
  String(input?.promptVersion ?? input?.ctx?.run?.promptVersion ?? "").trim() ||
  String(agentDecision?.decisionMeta?.strategy_version ?? "").trim() ||
  "prompt_v1";

const model =
  String(input?.model ?? input?.ctx?.run?.model ?? "").trim() ||
  String(agentDecision?.decisionMeta?.model ?? "").trim() ||
  "gpt-5.2";

// Data quality object consumed by Node 8B
const dataQuality = {
  okForTrading: !!okForTrading,
  requestedSymbolsCount,
  pricedSymbolsCount: pricedForRequested,
  pricesCoverageRequested, // 0..1
  missingRequested,
  injectedPrices,
};

const ctx = {
  sheetId: input.sheetId ?? input.ctx?.sheetId,
  meta: meta0,
  config: config0,
  feesConfig,
  run: {
    runId,
    timestampParis: ts,
    model,
    promptVersion,
  },
  portfolioSummary,
  market: {
    prices: mergedPrices,
    newsItems: upstreamNewsItems,
    dataQuality,
  },
};

const cash0 = Number(portfolioSummary?.cashEUR ?? 0);
const actionsCount = Array.isArray(agentDecision?.actions) ? agentDecision.actions.length : 0;
const caveatsCount = Array.isArray(agentDecision?.dataCaveats) ? agentDecision.dataCaveats.length : 0;

const commentary = `PM_DECISION=${decision} | orders=${orders.length} | actions=${actionsCount} | cash0=${cash0} | caveats=${caveatsCount} | priceCoverage=${Math.round(pricesCoverageRequested * 1000) / 10}%`;

return [
  {
    json: {
      decision,
      commentary,
      agentDecision,
      orders,
      metrics: {
        aiCostEUR: Number.isFinite(aiCostEUR) ? aiCostEUR : 0,
        expectedFeesEUR: Number.isFinite(expectedFeesEUR) ? expectedFeesEUR : 0,
      },
      warnings,
      ctx,
    },
  },
];

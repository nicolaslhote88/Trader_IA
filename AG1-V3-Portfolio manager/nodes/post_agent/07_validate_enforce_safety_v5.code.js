// Node 7 - Validate & Enforce Safety (v5, FX executable)
// Mode: Run Once for All Items
// Output: [{ json: { decision, commentary, agentDecision, orders, metrics, warnings, ctx } }]

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

function toNum(x, dflt = 0) {
  const n = Number(x);
  return Number.isFinite(n) ? n : dflt;
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

function clampText(s, n) {
  return String(s ?? "").replace(/\s+/g, " ").trim().slice(0, n);
}

function normSymbol(v) {
  return String(v ?? "").trim();
}

function normAssetClass(v, symbol) {
  const raw = String(v ?? "").trim().toUpperCase();
  if (raw === "FX" || raw === "FOREX") return "FX";
  if (raw === "CRYPTO") return "CRYPTO";
  if (raw === "EQUITY" || raw === "STOCK" || raw === "ETF") return "EQUITY";
  const s = String(symbol ?? "").toUpperCase();
  if (s.startsWith("FX:") || s.endsWith("=X")) return "FX";
  return "EQUITY";
}

function fxSymbolInternal(symbol) {
  const s = String(symbol ?? "").toUpperCase();
  if (s.startsWith("FX:")) {
    const pair = s.replace("FX:", "").replace(/[^A-Z]/g, "").slice(0, 6);
    return pair.length === 6 ? `FX:${pair}` : s;
  }
  const pair = s.replace("=X", "").replace("/", "").replace(/[^A-Z]/g, "").slice(0, 6);
  return pair.length === 6 ? `FX:${pair}` : s;
}

function fxSymbolYahoo(symbol) {
  const s = String(symbol ?? "").toUpperCase();
  if (s.endsWith("=X")) {
    const pair = s.replace("=X", "").replace(/[^A-Z]/g, "").slice(0, 6);
    return pair.length === 6 ? `${pair}=X` : s;
  }
  const pair = s.replace("FX:", "").replace("/", "").replace(/[^A-Z]/g, "").slice(0, 6);
  return pair.length === 6 ? `${pair}=X` : s;
}

function parseFxMeta(symbol) {
  const internal = fxSymbolInternal(symbol);
  const pair6 = internal.replace("FX:", "").replace(/[^A-Z]/g, "").slice(0, 6);
  if (pair6.length !== 6) return null;
  return {
    symbolInternal: `FX:${pair6}`,
    symbolYahoo: `${pair6}=X`,
    pair6,
    base: pair6.slice(0, 3),
    quote: pair6.slice(3, 6),
  };
}

function normalizeDependencies(dep) {
  if (Array.isArray(dep)) {
    return uniq(dep.map((x) => clampText(x, 64)).filter(Boolean));
  }
  if (isObj(dep)) {
    const out = [];
    if (dep.needPrice) out.push("PRICE");
    if (dep.needNote) out.push("NOTE");
    if (dep.needNews) out.push("NEWS");
    if (dep.needTechnical) out.push("TECHNICAL");
    if (dep.needConsensus) out.push("CONSENSUS");
    return out;
  }
  return [];
}

function normalizeAgentActionsForFx(agentDecision, enableFx) {
  if (!Array.isArray(agentDecision?.actions)) return agentDecision;

  const next = [];
  for (const raw of agentDecision.actions) {
    const a = isObj(raw) ? deepClone(raw) : {};
    const symRaw = normSymbol(a.symbol_internal || a.symbol || a.symbol_yahoo);
    const assetClass = normAssetClass(a.assetClass ?? a.asset_class, symRaw);
    const actIn = String(a.action ?? "").toUpperCase();

    a.assetClass = assetClass;
    a.asset_class = undefined;

    if (assetClass === "FX") {
      const meta = parseFxMeta(symRaw);
      if (meta) {
        a.symbol = meta.symbolInternal;
        a.symbol_internal = meta.symbolInternal;
        a.symbol_yahoo = meta.symbolYahoo;
      }

      if (!enableFx) {
        let fxAction = actIn;
        if (["OPEN", "INCREASE", "BUY", "PROPOSE_OPEN"].includes(actIn)) fxAction = "PROPOSE_OPEN";
        else if (["DECREASE", "CLOSE", "SELL", "PROPOSE_CLOSE"].includes(actIn)) fxAction = "PROPOSE_CLOSE";
        else if (!["WATCH", "HOLD"].includes(actIn)) fxAction = "WATCH";

        a.action = fxAction;
        a.execution_required = false;
        a.needs_risk_approval = true;

        const deps = normalizeDependencies(a.dependencies);
        if (!deps.includes("AG5_RISK_APPROVAL")) deps.push("AG5_RISK_APPROVAL");
        if (!deps.includes("AG6_EXECUTION")) deps.push("AG6_EXECUTION");
        a.dependencies = deps;
      } else {
        let fxAction = actIn;
        if (actIn === "BUY") fxAction = "OPEN";
        if (actIn === "SELL") fxAction = "CLOSE";
        if (!["OPEN", "INCREASE", "DECREASE", "CLOSE", "PROPOSE_OPEN", "PROPOSE_CLOSE", "WATCH", "HOLD"].includes(fxAction)) {
          fxAction = "WATCH";
        }

        a.action = fxAction;
        a.dependencies = normalizeDependencies(a.dependencies).filter((d) => d !== "AG5_RISK_APPROVAL" && d !== "AG6_EXECUTION");

        if (["OPEN", "INCREASE", "DECREASE", "CLOSE"].includes(fxAction)) {
          a.execution_required = true;
          a.needs_risk_approval = false;
        } else {
          a.execution_required = false;
          a.needs_risk_approval = false;
        }
      }

      if (!isObj(a.proposed_position)) {
        a.proposed_position = {
          direction: ["OPEN", "INCREASE", "BUY", "PROPOSE_OPEN"].includes(String(a.action || "").toUpperCase()) ? "LONG" : "NEUTRAL",
          conviction: Number.isFinite(Number(a.confidence)) ? Number(a.confidence) : null,
        };
      }
    } else {
      if (a.execution_required === undefined) a.execution_required = null;
      if (a.needs_risk_approval === undefined) a.needs_risk_approval = null;
    }

    next.push(a);
  }

  agentDecision.actions = next;
  return agentDecision;
}

function appendFxPairProposals(agentDecision, input, enableFx) {
  const rawCandidates = Array.isArray(input?.fx_candidates) && input.fx_candidates.length
    ? input.fx_candidates
    : (Array.isArray(input?.fx_pairs) ? input.fx_pairs : []);
  if (!rawCandidates.length) return agentDecision;

  const pairKey = (pair6) => {
    const p = String(pair6 || "").toUpperCase().replace(/[^A-Z]/g, "").slice(0, 6);
    if (p.length !== 6) return "";
    const inv = `${p.slice(3, 6)}${p.slice(0, 3)}`;
    return [p, inv].sort().join("|");
  };

  const prep = [];
  for (const p of rawCandidates) {
    const meta = parseFxMeta(p?.symbol_internal || p?.pair || p?.symbol || p?.symbol_yahoo || "");
    if (!meta?.symbolInternal) continue;
    const bias = String(p?.directional_bias ?? "NEUTRAL").toUpperCase();
    const confRaw = Number(p?.confidence);
    const confidence = Number.isFinite(confRaw) ? Math.max(0, Math.min(100, Math.round(confRaw))) : 0;
    const gates = String(p?.gates ?? "OK").toUpperCase();
    const blocked = ["DATA_QUALITY_LOW", "INVALID_OPTIONS_STATE", "LIQUIDITY_STRESS"].some((g) => gates.includes(g));
    if (bias === "NEUTRAL" || confidence < 50 || blocked) continue;

    prep.push({
      ...p,
      __meta: meta,
      __bias: bias,
      __confidence: confidence,
      __key: pairKey(meta.pair6),
      __ev: Number.isFinite(Number(p?.ev_r)) ? Number(p?.ev_r) : -999,
    });
  }

  const dedup = new Map();
  for (const p of prep) {
    if (!p.__key) continue;
    const prev = dedup.get(p.__key);
    if (!prev || p.__confidence > prev.__confidence || (p.__confidence === prev.__confidence && p.__ev > prev.__ev)) {
      dedup.set(p.__key, p);
    }
  }

  const fxPairs = Array.from(dedup.values())
    .sort((a, b) => (b.__confidence - a.__confidence) || (b.__ev - a.__ev))
    .slice(0, 6);
  if (!fxPairs.length) return agentDecision;

  const existing = new Set();
  for (const a of agentDecision.actions || []) {
    const ac = normAssetClass(a?.assetClass ?? a?.asset_class, a?.symbol);
    if (ac !== "FX") continue;
    const meta = parseFxMeta(a?.symbol_internal || a?.symbol || "");
    if (meta?.symbolInternal) existing.add(meta.symbolInternal);
  }

  for (const p of fxPairs) {
    const meta = p.__meta || parseFxMeta(p?.symbol_internal || p?.pair || p?.symbol || "");
    if (!meta?.symbolInternal || existing.has(meta.symbolInternal)) continue;

    const bias = p.__bias || String(p?.directional_bias ?? "NEUTRAL").toUpperCase();
    const action = bias === "BUY_BASE" ? "PROPOSE_OPEN" : (bias === "SELL_BASE" ? "PROPOSE_CLOSE" : "WATCH");
    const confidence = Number.isFinite(Number(p.__confidence)) ? Number(p.__confidence) : null;
    const urgent = toBool(p?.urgent_event_window, false);

    (agentDecision.actions = Array.isArray(agentDecision.actions) ? agentDecision.actions : []).push({
      symbol: meta.symbolInternal,
      symbol_internal: meta.symbolInternal,
      symbol_yahoo: meta.symbolYahoo,
      assetClass: "FX",
      action,
      priority: action === "WATCH" ? 3 : 2,
      confidence,
      horizonDays: action === "WATCH" ? null : 5,
      targetWeightPct: null,
      targetQty: null,
      rationale: clampText(p?.rationale || `${meta.symbolInternal} ${bias}`, 300),
      entryPlan: { orderType: null, limitPrice: null, timeInForce: null },
      riskPlan: { stopLossPct: null, takeProfitPct: null, maxLossEUR: null },
      dependencies: [],
      execution_required: false,
      needs_risk_approval: false,
      proposed_position: {
        direction: action === "PROPOSE_OPEN" ? "LONG" : "NEUTRAL",
        conviction: confidence,
      },
      nextReviewDays: urgent ? 1 : 3,
      source: enableFx ? "AG4_PROPOSAL" : "AG4_PROPOSAL_FX_DISABLED",
    });
    existing.add(meta.symbolInternal);
  }

  return agentDecision;
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

function extractPriceFromOpportunityRow(row) {
  const candidates = [
    row?.entry,
    row?.entry_price,
    row?.entryPrice,
    row?.regular_market_price,
    row?.Regular_Market_Price,
    row?.last_close,
    row?.Last_Close,
    row?.price,
    row?.lastPrice,
    row?.close,
  ];
  for (const c of candidates) {
    const n = toNumOrNull(c);
    if (n != null && n > 0) return n;
  }
  return null;
}

function buildPricesFromOpportunityPack(input) {
  const out = {};
  const rows = Array.isArray(input?.opportunity_pack?.rows) ? input.opportunity_pack.rows : [];
  const ts = String(input?.opportunity_pack?.generatedAt ?? "");
  for (const r of rows) {
    const symRaw = normSymbol(r?.symbol_internal || r?.symbol || r?.symbol_yahoo);
    if (!symRaw) continue;
    const ac = normAssetClass(r?.asset_class ?? r?.assetClass, symRaw);
    const px = extractPriceFromOpportunityRow(r);
    if (!(px > 0)) continue;

    const basePayload = { lastPrice: px, source: "opportunity_pack", ts, stale: false };
    if (ac === "FX") {
      const meta = parseFxMeta(symRaw);
      if (!meta) continue;
      out[meta.symbolInternal] = basePayload;
      out[meta.symbolYahoo] = basePayload;
      out[meta.pair6] = basePayload;
      const sy = normSymbol(r?.symbol_yahoo).toUpperCase();
      if (sy) out[sy] = basePayload;
      continue;
    }

    const s = normSymbol(symRaw).toUpperCase();
    if (!s) continue;
    out[s] = basePayload;
    const sy = normSymbol(r?.symbol_yahoo).toUpperCase();
    if (sy) out[sy] = basePayload;
  }
  return out;
}

function buildFxRateGraph(prices) {
  const graph = {};
  const putEdge = (from, to, rate) => {
    if (!from || !to || from === to) return;
    const r = toNumOrNull(rate);
    if (!(r > 0)) return;
    const f = String(from).toUpperCase();
    const t = String(to).toUpperCase();
    if (!graph[f]) graph[f] = {};
    const prev = toNumOrNull(graph[f][t]);
    if (!(prev > 0)) graph[f][t] = r;
  };

  for (const [key, val] of Object.entries(isObj(prices) ? prices : {})) {
    const k = normSymbol(key).toUpperCase();
    if (!k) continue;
    const looksFx = k.startsWith("FX:") || k.endsWith("=X") || /^[A-Z]{6}$/.test(k);
    if (!looksFx) continue;
    const meta = parseFxMeta(k);
    if (!meta) continue;
    const px = toNumOrNull(val?.lastPrice ?? val?.close ?? null);
    if (!(px > 0)) continue;
    putEdge(meta.base, meta.quote, px);
    putEdge(meta.quote, meta.base, 1 / px);
  }
  return graph;
}

function findFxRate(graph, fromCcy, toCcy) {
  const from = String(fromCcy || "").toUpperCase();
  const to = String(toCcy || "").toUpperCase();
  if (!from || !to) return null;
  if (from === to) return 1;
  const direct = toNumOrNull(graph?.[from]?.[to]);
  if (direct != null && direct > 0) return direct;
  const first = graph?.[from] || {};
  for (const [mid, r1] of Object.entries(first)) {
    const rr1 = toNumOrNull(r1);
    if (!(rr1 > 0)) continue;
    const r2 = toNumOrNull(graph?.[String(mid).toUpperCase()]?.[to]);
    if (r2 != null && r2 > 0) return rr1 * r2;
  }
  return null;
}

function collectFxQuotesForConversions(actions, portfolioSummary, enableFx) {
  const out = new Set();
  if (!enableFx) return out;

  for (const a of Array.isArray(actions) ? actions : []) {
    const act = String(a?.action ?? "").toUpperCase();
    if (!["OPEN", "INCREASE", "DECREASE", "CLOSE"].includes(act)) continue;
    const s = normSymbol(a?.symbol_internal || a?.symbol);
    if (normAssetClass(a?.assetClass ?? a?.asset_class, s) !== "FX") continue;
    const meta = parseFxMeta(s);
    if (meta?.quote) out.add(meta.quote);
  }

  const pos = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
  for (const p of pos) {
    const s = normSymbol(p?.Symbol ?? p?.symbol);
    if (!s) continue;
    if (normAssetClass(p?.AssetClass ?? p?.assetClass ?? p?.asset_class, s) !== "FX") continue;
    const meta = parseFxMeta(s);
    if (meta?.quote) out.add(meta.quote);
  }
  return out;
}

function ensureFxConversionPrices(pricesObj, requiredQuotesSet) {
  const prices = isObj(pricesObj) ? pricesObj : {};
  const requiredQuotes = Array.from(requiredQuotesSet instanceof Set ? requiredQuotesSet : []);
  if (!requiredQuotes.length) return [];

  const added = [];
  const graph = buildFxRateGraph(prices);
  const nowTs = new Date().toISOString();

  for (const q0 of requiredQuotes) {
    const quote = String(q0 || "").trim().toUpperCase();
    if (!quote || quote === "EUR") continue;

    const existing = resolveQuoteToEurFromPrices(quote, prices);
    if (existing.ok && existing.quote_to_eur > 0) continue;

    let eurToQuote = findFxRate(graph, "EUR", quote);
    if (!(eurToQuote > 0)) {
      const quoteToEur = findFxRate(graph, quote, "EUR");
      if (quoteToEur > 0) eurToQuote = 1 / quoteToEur;
    }
    if (!(eurToQuote > 0)) continue;

    const direct = { lastPrice: eurToQuote, source: "fx_synth_cross", ts: nowTs, stale: false };
    const inverse = { lastPrice: 1 / eurToQuote, source: "fx_synth_cross", ts: nowTs, stale: false };
    const directKeys = [`FX:EUR${quote}`, `EUR${quote}=X`, `EUR${quote}`];
    const inverseKeys = [`FX:${quote}EUR`, `${quote}EUR=X`, `${quote}EUR`];

    for (const k of directKeys) {
      const kk = String(k).toUpperCase();
      if (!prices[kk]) prices[kk] = direct;
    }
    for (const k of inverseKeys) {
      const kk = String(k).toUpperCase();
      if (!prices[kk]) prices[kk] = inverse;
    }

    if (!graph.EUR) graph.EUR = {};
    if (!graph[quote]) graph[quote] = {};
    graph.EUR[quote] = eurToQuote;
    graph[quote].EUR = 1 / eurToQuote;
    added.push(`EUR${quote}=X`);
  }

  if (added.length) enrichFxPriceAliases(prices);
  return uniq(added);
}

function enrichFxPriceAliases(pricesObj) {
  const prices = isObj(pricesObj) ? pricesObj : {};
  const keys = Object.keys(prices);
  for (const key of keys) {
    const k = normSymbol(key).toUpperCase();
    if (!k || (!k.startsWith("FX:") && !k.endsWith("=X"))) continue;
    const meta = parseFxMeta(k);
    if (!meta) continue;
    if (!prices[meta.symbolInternal]) prices[meta.symbolInternal] = prices[key];
    if (!prices[meta.symbolYahoo]) prices[meta.symbolYahoo] = prices[key];
    if (!prices[meta.pair6]) prices[meta.pair6] = prices[key];
  }
  return prices;
}

function getPriceFromMap(prices, symbol) {
  const s = normSymbol(symbol);
  if (!s) return { price: null, symbol: null };

  const candidates = [s];
  const up = s.toUpperCase();
  if (up.startsWith("FX:") || up.endsWith("=X")) {
    const meta = parseFxMeta(up);
    if (meta) candidates.push(meta.symbolInternal, meta.symbolYahoo, meta.pair6);
  } else if (/^[A-Z]{6}$/.test(up)) {
    candidates.push(`FX:${up}`, `${up}=X`);
  }

  for (const c of uniq(candidates.map((x) => normSymbol(x)).filter(Boolean))) {
    const px = toNumOrNull(prices?.[c]?.lastPrice ?? prices?.[c]?.close ?? null);
    if (px != null && px > 0) return { price: px, symbol: c };
  }
  return { price: null, symbol: null };
}

function resolveQuoteToEurFromPrices(quoteCcy, prices) {
  const quote = String(quoteCcy ?? "").trim().toUpperCase();
  if (!quote) return { ok: false, quote_to_eur: null, source_symbol: null };
  if (quote === "EUR") return { ok: true, quote_to_eur: 1, source_symbol: "EUR" };

  const inverse = [`FX:EUR${quote}`, `EUR${quote}=X`];
  for (const c of inverse) {
    const px = getPriceFromMap(prices, c).price;
    if (px != null && px > 0) return { ok: true, quote_to_eur: 1 / px, source_symbol: c };
  }

  const direct = [`FX:${quote}EUR`, `${quote}EUR=X`];
  for (const c of direct) {
    const px = getPriceFromMap(prices, c).price;
    if (px != null && px > 0) return { ok: true, quote_to_eur: px, source_symbol: c };
  }

  return { ok: false, quote_to_eur: null, source_symbol: null };
}

function buildFxEnabledSet(input, portfolioSummary) {
  const out = new Set();
  const pairs = Array.isArray(input?.fx_pairs) ? input.fx_pairs : [];
  for (const p of pairs) {
    const meta = parseFxMeta(p?.symbol_internal || p?.pair || p?.symbol || "");
    if (meta?.symbolInternal) out.add(meta.symbolInternal);
  }

  const rows = Array.isArray(input?.opportunity_pack?.rows) ? input.opportunity_pack.rows : [];
  for (const r of rows) {
    const ac = normAssetClass(r?.asset_class ?? r?.assetClass, r?.symbol);
    if (ac !== "FX") continue;
    const meta = parseFxMeta(r?.symbol || "");
    if (meta?.symbolInternal) out.add(meta.symbolInternal);
  }

  const pos = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
  for (const p of pos) {
    const ac = normAssetClass(p?.AssetClass ?? p?.assetClass ?? p?.asset_class, p?.Symbol ?? p?.symbol);
    if (ac !== "FX") continue;
    const meta = parseFxMeta((p?.Symbol ?? p?.symbol) || "");
    if (meta?.symbolInternal) out.add(meta.symbolInternal);
  }

  return out;
}

function estimateExistingFxMarginEUR(portfolioSummary, prices, fxLeverage) {
  const lev = Math.max(1, toNum(fxLeverage, 10));
  const pos = Array.isArray(portfolioSummary?.positions) ? portfolioSummary.positions : [];
  let out = 0;

  for (const p of pos) {
    const sym = normSymbol(p.Symbol ?? p.symbol);
    const ac = normAssetClass(p.AssetClass ?? p.assetClass ?? p.asset_class, sym);
    if (ac !== "FX") continue;

    const qty = Math.abs(toNum(p.Quantity, 0));
    if (!(qty > 0)) continue;

    const explicitMargin = toNumOrNull(p.MarginUsedEUR ?? p.margin_used_eur ?? p.marginUsedEUR ?? p.margin_used);
    if (explicitMargin != null && explicitMargin > 0) {
      out += explicitMargin;
      continue;
    }

    const meta = parseFxMeta(sym);
    if (!meta) continue;
    const px = toNumOrNull(p.AvgPrice ?? p.avg_price ?? p.LastPrice ?? p.last_price);
    if (!(px > 0)) continue;

    const conv = resolveQuoteToEurFromPrices(meta.quote, prices);
    if (!conv.ok || !(conv.quote_to_eur > 0)) continue;

    const marketValue = toNumOrNull(p.MarketValue ?? p.market_value);
    const unrealized = toNumOrNull(p.UnrealizedPnL ?? p.unrealized_pnl);
    if (marketValue != null && unrealized != null) {
      const implied = marketValue - unrealized;
      if (implied > 0) {
        out += implied;
        continue;
      }
    }

    out += Math.abs(qty * px * conv.quote_to_eur) / lev;
  }

  return out;
}

function buildTradePriceCoverage(actions, prices, enableFx) {
  let requested = 0;
  let priced = 0;
  const missing = [];

  for (const a of Array.isArray(actions) ? actions : []) {
    const act = String(a?.action ?? "").toUpperCase();
    if (!["OPEN", "INCREASE", "DECREASE", "CLOSE"].includes(act)) continue;

    const symRaw = normSymbol(a?.symbol_internal || a?.symbol);
    const ac = normAssetClass(a?.assetClass ?? a?.asset_class, symRaw);

    if (ac === "FX") {
      if (!enableFx) continue;
      const fxMeta = parseFxMeta(symRaw);
      if (!fxMeta) {
        requested += 1;
        missing.push(symRaw || "FX_UNKNOWN");
        continue;
      }

      requested += 1;
      const pairPx = getPriceFromMap(prices, fxMeta.symbolInternal).price;
      if (pairPx != null && pairPx > 0) priced += 1;
      else missing.push(fxMeta.symbolInternal);

      if (fxMeta.quote !== "EUR") {
        requested += 1;
        const conv = resolveQuoteToEurFromPrices(fxMeta.quote, prices);
        if (conv.ok && conv.quote_to_eur > 0) priced += 1;
        else missing.push(`FX_CONV:${fxMeta.quote}->EUR`);
      }
      continue;
    }

    const sym = normSymbol(a?.symbol);
    if (!sym) continue;
    requested += 1;
    const px = getPriceFromMap(prices, sym).price;
    if (px != null && px > 0) priced += 1;
    else missing.push(sym);
  }

  return {
    requestedSymbolsCount: requested,
    pricedForRequested: priced,
    missingRequested: uniq(missing),
  };
}

function extractAgentDecisionObject(input) {
  if (
    isObj(input.output) &&
    (Array.isArray(input.output.actions) || isObj(input.output.portfolioPlan) || isObj(input.output.decisionMeta))
  ) {
    return { ...input.output, _parseStatus: "OK_OBJECT_OUTPUT" };
  }

  if (isObj(input.agentDecision)) {
    return { ...input.agentDecision, _parseStatus: "OK_AGENTDECISION" };
  }

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

    const last = Number(getPriceFromMap(prices, s).price ?? p.LastPrice ?? NaN);
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
    const sRaw = normSymbol(p.Symbol ?? p.symbol);
    if (!sRaw || sRaw === "CASH_EUR" || sRaw === "__META__") continue;

    const ac = normAssetClass(p.AssetClass ?? p.assetClass ?? p.asset_class, sRaw);
    const s = ac === "FX" ? fxSymbolInternal(sRaw) : sRaw;
    const qty = Number(p.Quantity ?? 0);
    const last = Number(getPriceFromMap(prices, s).price ?? p.LastPrice ?? NaN);

    map[s] = {
      Symbol: s,
      Quantity: Number.isFinite(qty) ? qty : 0,
      LastPrice: Number.isFinite(last) ? last : null,
      AssetClass: ac,
      AvgPrice: toNumOrNull(p.AvgPrice ?? p.avg_price),
      MarketValue: toNumOrNull(p.MarketValue ?? p.market_value),
      UnrealizedPnL: toNumOrNull(p.UnrealizedPnL ?? p.unrealized_pnl),
      MarginUsedEUR: toNumOrNull(p.MarginUsedEUR ?? p.margin_used_eur ?? p.marginUsedEUR),
    };
  }
  return map;
}

function generateOrdersFromActions(agentDecision, portfolioSummary, prices, feesConfig, opts = {}) {
  const warnings = [];
  const equitySell = [];
  const equityBuy = [];
  const fxSell = [];
  const fxBuy = [];

  const actions = Array.isArray(agentDecision?.actions) ? agentDecision.actions : [];
  const holdings = buildHoldingsMap(portfolioSummary, prices);
  const totalValue = computeTotalPortfolioValueEUR(portfolioSummary, prices);
  if (!Number.isFinite(totalValue) || totalValue <= 0) {
    warnings.push("PORTFOLIO_VALUE_INVALID: cannot size orders.");
    return { orders: [], warnings };
  }

  let cashSim = Number(portfolioSummary?.cashEUR ?? 0);
  const fixed = Number(feesConfig?.orderFeeFixedEUR ?? 0);
  const pct = Number(feesConfig?.orderFeePct ?? 0);

  const enableFx = !!opts.enableFx;
  const cfg = isObj(opts.config) ? opts.config : {};
  const fxLeverageDefault = Math.max(1, toNum(cfg.fx_leverage_default, 10));
  const fxMaxMarginPct = Math.max(0, Math.min(1, toNum(cfg.fx_max_margin_pct, 0.30)));
  const fxMinNotionalEUR = Math.max(0, toNum(cfg.fx_min_notional_eur, 500));
  const fxFeeBps = Math.max(0, toNum(cfg.fx_fee_bps, 0));
  const fxAllowShort = toBool(cfg.fx_allow_short, false);
  const fxEnabledPairsOnly = toBool(cfg.fx_enabled_pairs_only, true);
  const fxEnabledSet = opts.fxEnabledSet instanceof Set ? opts.fxEnabledSet : new Set();

  let marginSim = Math.max(0, toNum(opts.startingFxMarginEUR, estimateExistingFxMarginEUR(portfolioSummary, prices, fxLeverageDefault)));
  const marginCapEUR = totalValue * fxMaxMarginPct;

  const currentQty = (sym) => {
    const h = holdings[sym];
    const q = Number(h?.Quantity ?? 0);
    return Number.isFinite(q) ? q : 0;
  };

  for (const a of actions) {
    const act = String(a?.action ?? "").toUpperCase();
    if (!["OPEN", "INCREASE", "DECREASE", "CLOSE"].includes(act)) continue;

    const symRaw = normSymbol(a?.symbol_internal || a?.symbol);
    const assetClass = normAssetClass(a?.assetClass ?? a?.asset_class, symRaw);
    const entryPlan = isObj(a?.entryPlan) ? a.entryPlan : {};
    const riskPlan = isObj(a?.riskPlan) ? a.riskPlan : {};

    if (assetClass === "FX") {
      if (!enableFx) continue;

      const meta = parseFxMeta(symRaw);
      const sym = meta?.symbolInternal || "";
      if (!sym || !meta) {
        warnings.push(`SKIP_INVALID_FX_SYMBOL:${symRaw}:${act}`);
        continue;
      }

      if (fxEnabledPairsOnly && fxEnabledSet.size > 0 && !fxEnabledSet.has(sym)) {
        warnings.push(`SKIP_FX_NOT_ENABLED_PAIR:${sym}`);
        continue;
      }

      const pairPx = getPriceFromMap(prices, sym).price;
      if (!(pairPx > 0)) {
        warnings.push(`SKIP_NO_PRICE:${sym}:${act}`);
        continue;
      }

      const conv = resolveQuoteToEurFromPrices(meta.quote, prices);
      if (!conv.ok || !(conv.quote_to_eur > 0)) {
        warnings.push(`SKIP_NO_CONVERSION:${sym}:QUOTE_${meta.quote}`);
        continue;
      }

      const targetQtyExplicit = toNumOrNull(a?.targetQty ?? a?.targetQuantity ?? null);
      const targetNotionalExplicit = toNumOrNull(a?.targetNotionalEUR ?? a?.targetNotional ?? null);

      let targetNotionalEUR = null;
      if (targetNotionalExplicit != null) {
        targetNotionalEUR = Math.abs(targetNotionalExplicit);
      } else if (a?.targetWeightPct != null) {
        const t = (Number(a.targetWeightPct) / 100) * totalValue;
        if (Number.isFinite(t) && t > 0) targetNotionalEUR = Math.abs(t);
      } else if (act === "OPEN" || act === "INCREASE") {
        targetNotionalEUR = Math.max(0, Math.min(1000, cashSim * 0.05));
      }

      const q0 = Math.max(0, currentQty(sym));

      if (act === "DECREASE" || act === "CLOSE") {
        if (q0 <= 0) continue;

        let qtyToSell = null;
        if (act === "CLOSE") {
          qtyToSell = q0;
        } else if (targetQtyExplicit != null) {
          qtyToSell = Math.max(0, q0 - Math.max(0, targetQtyExplicit));
        } else if (targetNotionalEUR != null) {
          const targetQty = targetNotionalEUR / (pairPx * conv.quote_to_eur);
          qtyToSell = Math.max(0, q0 - Math.max(0, targetQty));
        }

        if (!(qtyToSell > 1e-8)) {
          if (act !== "CLOSE") warnings.push(`SKIP_NO_TARGET:${sym}:${act}`);
          continue;
        }

        const qtyFinal = Number(qtyToSell.toFixed(6));
        const notionalEUR = qtyFinal * pairPx * conv.quote_to_eur;
        const marginUsed = notionalEUR / fxLeverageDefault;
        const marginReleaseEst = Math.max(0, Math.min(marginSim, q0 > 0 ? marginSim * (qtyFinal / q0) : marginUsed));

        fxSell.push({
          symbol: sym,
          side: "SELL",
          quantity: qtyFinal,
          price: null,
          rationale: clampText(a?.rationale, 240),
          orderType: String(entryPlan.orderType || "MARKET"),
          limitPrice: toNumOrNull(entryPlan.limitPrice),
          stopLossPct: toNumOrNull(riskPlan.stopLossPct),
          confidence: a?.confidence,
          priority: a?.priority,
          assetClass: "FX",
          pair6: meta.pair6,
          base_ccy: meta.base,
          quote_ccy: meta.quote,
          quote_to_eur: Number(conv.quote_to_eur.toFixed(10)),
          notional_eur: Number(notionalEUR.toFixed(6)),
          margin_used_eur: Number(marginUsed.toFixed(6)),
          margin_release_est_eur: Number(marginReleaseEst.toFixed(6)),
          fx_leverage: fxLeverageDefault,
          fx_fee_bps: fxFeeBps,
          conversion_symbol: conv.source_symbol,
        });
        continue;
      }

      if (act === "OPEN" || act === "INCREASE") {
        let qtyBase = null;
        if (targetQtyExplicit != null) qtyBase = Math.abs(targetQtyExplicit);
        else if (targetNotionalEUR != null) qtyBase = targetNotionalEUR / (pairPx * conv.quote_to_eur);

        if (!(qtyBase > 1e-8)) {
          warnings.push(`SKIP_NO_TARGET:${sym}:${act}`);
          continue;
        }

        if (!fxAllowShort && qtyBase < 0) {
          warnings.push(`SKIP_SHORT_DISABLED:${sym}`);
          continue;
        }

        const qtyFinal = Number(Math.abs(qtyBase).toFixed(6));
        const notionalEUR = qtyFinal * pairPx * conv.quote_to_eur;
        if (!(notionalEUR > 0)) {
          warnings.push(`SKIP_INVALID_NOTIONAL:${sym}`);
          continue;
        }

        if (notionalEUR < fxMinNotionalEUR) {
          warnings.push(`SKIP_MIN_NOTIONAL:${sym}:${Math.round(notionalEUR)}<${Math.round(fxMinNotionalEUR)}`);
          continue;
        }

        const marginUsed = notionalEUR / fxLeverageDefault;
        if (marginUsed > cashSim || (marginCapEUR > 0 && marginSim + marginUsed > marginCapEUR)) {
          warnings.push(`SKIP_INSUFFICIENT_MARGIN:${sym}`);
          continue;
        }

        fxBuy.push({
          symbol: sym,
          side: "BUY",
          quantity: qtyFinal,
          price: null,
          rationale: clampText(a?.rationale, 240),
          orderType: String(entryPlan.orderType || "MARKET"),
          limitPrice: toNumOrNull(entryPlan.limitPrice),
          stopLossPct: toNumOrNull(riskPlan.stopLossPct),
          confidence: a?.confidence,
          priority: a?.priority,
          assetClass: "FX",
          pair6: meta.pair6,
          base_ccy: meta.base,
          quote_ccy: meta.quote,
          quote_to_eur: Number(conv.quote_to_eur.toFixed(10)),
          notional_eur: Number(notionalEUR.toFixed(6)),
          margin_used_eur: Number(marginUsed.toFixed(6)),
          fx_leverage: fxLeverageDefault,
          fx_fee_bps: fxFeeBps,
          conversion_symbol: conv.source_symbol,
        });
      }
      continue;
    }

    const sym = normSymbol(a?.symbol);
    if (!sym) continue;

    const px = Number(getPriceFromMap(prices, sym).price ?? NaN);
    if (!Number.isFinite(px) || px <= 0) {
      warnings.push(`SKIP_NO_PRICE:${sym}:${act}`);
      continue;
    }

    const q0 = currentQty(sym);
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

    if (act === "DECREASE" || act === "CLOSE") {
      if (q0 <= 0) continue;

      const rawSell = q0 - targetQty;
      const qtyToSell = Math.min(q0, Math.ceil(rawSell));
      if (qtyToSell >= 1) {
        equitySell.push({
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
        equityBuy.push({
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

  const finalOrders = [];

  for (const o of equitySell) {
    finalOrders.push(o);
    const px = Number(getPriceFromMap(prices, o.symbol).price ?? NaN);
    if (Number.isFinite(px) && px > 0) {
      const proceeds = (o.quantity * px) * (1 - pct) - fixed;
      cashSim += proceeds;
    }
  }

  for (const o of fxSell) {
    finalOrders.push(o);
    const release = toNum(o.margin_release_est_eur, 0);
    if (release > 0) {
      cashSim += release;
      marginSim = Math.max(0, marginSim - release);
    }
  }

  for (const o of equityBuy) {
    const px = Number(getPriceFromMap(prices, o.symbol).price ?? NaN);
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

  for (const o of fxBuy) {
    const marginUsed = toNum(o.margin_used_eur, 0);
    if (!(marginUsed > 0)) {
      warnings.push(`SKIP_INVALID_MARGIN:${o.symbol}`);
      continue;
    }
    if (marginUsed > cashSim || (marginCapEUR > 0 && marginSim + marginUsed > marginCapEUR)) {
      warnings.push(`SKIP_INSUFFICIENT_MARGIN:${o.symbol}`);
      continue;
    }
    finalOrders.push(o);
    cashSim -= marginUsed;
    marginSim += marginUsed;
  }

  return { orders: finalOrders, warnings };
}

function estimateFeesEUR(orders, feesConfig, prices) {
  const fixed = Number(feesConfig?.orderFeeFixedEUR ?? 0);
  const pct = Number(feesConfig?.orderFeePct ?? 0);
  const fxFeeBpsDefault = Math.max(0, Number(feesConfig?.fx_fee_bps ?? 0));
  let total = 0;

  for (const o of orders) {
    const s = normSymbol(o.symbol);
    const q = Number(o.quantity ?? NaN);
    if (!s || !Number.isFinite(q) || q <= 0) continue;

    const isFx = String(o.assetClass || "").toUpperCase() === "FX" || s.toUpperCase().startsWith("FX:");
    if (isFx) {
      const notional = Number(o.notional_eur ?? NaN);
      const feeBps = Math.max(0, Number(o.fx_fee_bps ?? fxFeeBpsDefault));
      if (Number.isFinite(notional) && notional > 0 && feeBps > 0) {
        total += (notional * feeBps) / 10000;
      }
      continue;
    }

    const px = Number(o.price ?? getPriceFromMap(prices, s).price ?? NaN);
    if (!Number.isFinite(px) || px <= 0) continue;
    total += fixed + pct * (q * px);
  }
  return total;
}

const all = $input.all();
const input = pickFirstRelevantInput(all);

const now = new Date();
const ts = now.toISOString();

const portfolioSummary = input.portfolioSummary ?? input.ctx?.portfolioSummary ?? { cashEUR: 0, positions: [] };
const meta0 = input.meta ?? input.ctx?.meta ?? {};
const configRaw = input.config ?? input.ctx?.config ?? {};
const enableFx = toBool(input?.run?.enable_fx ?? input?.ctx?.run?.enable_fx ?? configRaw.enable_fx, false);
const config0 = {
  ...configRaw,
  fx_leverage_default: Math.max(1, toNum(configRaw.fx_leverage_default, 10)),
  fx_max_margin_pct: Math.max(0, Math.min(1, toNum(configRaw.fx_max_margin_pct, 0.30))),
  fx_min_notional_eur: Math.max(0, toNum(configRaw.fx_min_notional_eur, 500)),
  fx_fee_bps: Math.max(0, toNum(configRaw.fx_fee_bps, 0)),
  fx_allow_short: toBool(configRaw.fx_allow_short, false),
  fx_enabled_pairs_only: toBool(configRaw.fx_enabled_pairs_only, true),
  enable_fx: enableFx,
};

const feesRaw = input.feesConfig ?? input.ctx?.feesConfig ?? { orderFeeFixedEUR: 0, orderFeePct: 0 };
const feesConfig = {
  ...feesRaw,
  fx_fee_bps: Math.max(0, toNum(feesRaw.fx_fee_bps ?? config0.fx_fee_bps, config0.fx_fee_bps)),
};

const market0 = input.market ?? input.ctx?.market ?? {};
const upstreamPrices = isObj(market0.prices) ? market0.prices : {};
const upstreamNewsItems = extractNewsItems(input);

let agentDecision = extractAgentDecisionObject(input);
agentDecision = coerceAgentDecisionToExpectedShape(agentDecision);
agentDecision = normalizeAgentActionsForFx(agentDecision, enableFx);
agentDecision = appendFxPairProposals(agentDecision, input, enableFx);
agentDecision = normalizeAgentActionsForFx(agentDecision, enableFx);

const pricesFallback = buildPricesFallbackFromPortfolio(portfolioSummary);
const pricesFromPack = buildPricesFromOpportunityPack(input);
const mergedPrices = enrichFxPriceAliases({ ...pricesFallback, ...pricesFromPack, ...upstreamPrices });

const injectedPrices = [];
if (Array.isArray(agentDecision.actions)) {
  for (const action of agentDecision.actions) {
    const sym = normSymbol(action.symbol_internal || action.symbol);
    const limit = toNumOrNull(action.entryPlan?.limitPrice);
    if (sym && limit && limit > 0) {
      const currentPrice = getPriceFromMap(mergedPrices, sym).price;
      if (!Number.isFinite(Number(currentPrice)) || Number(currentPrice) <= 0) {
        mergedPrices[sym] = { lastPrice: limit, source: "agent_limit_proxy", stale: false };
        injectedPrices.push(sym);
      }
    }
  }
}
enrichFxPriceAliases(mergedPrices);
const requiredFxQuotes = collectFxQuotesForConversions(agentDecision?.actions, portfolioSummary, enableFx);
const syntheticFxConversions = ensureFxConversionPrices(mergedPrices, requiredFxQuotes);

const heldSymbols = getPortfolioPositionsSymbols(portfolioSummary);
const coverage = buildTradePriceCoverage(agentDecision?.actions, mergedPrices, enableFx);
const requestedSymbolsCount = coverage.requestedSymbolsCount;
const pricedForRequested = coverage.pricedForRequested;
const missingRequested = coverage.missingRequested;
const okForTrading = requestedSymbolsCount ? (pricedForRequested === requestedSymbolsCount) : true;
const pricesCoverageRequested = requestedSymbolsCount ? (pricedForRequested / requestedSymbolsCount) : 1;

const fxEnabledSet = buildFxEnabledSet(input, portfolioSummary);
let { orders, warnings: orderWarnings } = generateOrdersFromActions(
  agentDecision,
  portfolioSummary,
  mergedPrices,
  feesConfig,
  {
    enableFx,
    config: config0,
    fxEnabledSet,
    startingFxMarginEUR: estimateExistingFxMarginEUR(portfolioSummary, mergedPrices, config0.fx_leverage_default),
  }
);

const safetyWarnings = [];
if (!okForTrading && requestedSymbolsCount > 0) {
  safetyWarnings.push(`SAFETY_VETO_MISSING_PRICES:${missingRequested.join(",")}`);
  orders = [];
}

if (injectedPrices.length > 0) {
  safetyWarnings.push(`NOTICE: Used agent limit price as proxy for: ${injectedPrices.join(", ")}`);
}
if (syntheticFxConversions.length > 0) {
  safetyWarnings.push(`NOTICE: Synth FX conversions added: ${syntheticFxConversions.join(", ")}`);
}

const warnings = [];
if (agentDecision?._parseStatus && !String(agentDecision._parseStatus).startsWith("OK")) {
  warnings.push(`AGENT_DECISION_PARSE:${agentDecision._parseStatus}`);
  if (agentDecision?._why) warnings.push(`AGENT_DECISION_PARSE_WHY:${agentDecision._why}`);
}

if (Array.isArray(agentDecision?.dataCaveats)) {
  for (const c of agentDecision.dataCaveats) warnings.push(String(c));
}

for (const s0 of heldSymbols) {
  const s = normSymbol(s0);
  const px = Number(getPriceFromMap(mergedPrices, s).price ?? NaN);
  if (!Number.isFinite(px) || px <= 0) warnings.push(`MISSING_PRICE_FOR_HELD:${s}`);
}

for (const w of orderWarnings) warnings.push(String(w));
for (const w of safetyWarnings) warnings.push(String(w));

const aiCostEUR = Number(input?.metrics?.aiCostEUR ?? 0);
const expectedFeesEUR = estimateFeesEUR(orders, feesConfig, mergedPrices);
const decision = orders.length ? "TRADE" : "NO_TRADE";

const runId =
  String(input?.runId ?? input?.ctx?.run?.runId ?? "").trim() ||
  String(agentDecision?.decisionMeta?.run_id ?? "").trim() ||
  "";

const strategyVersion =
  String(input?.strategyVersion ?? input?.ctx?.run?.strategyVersion ?? input?.config?.strategyVersion ?? "").trim() ||
  String(agentDecision?.decisionMeta?.strategy_version ?? "").trim() ||
  "strategy_v3";

const configVersion =
  String(input?.configVersion ?? input?.ctx?.run?.configVersion ?? input?.config?.configVersion ?? "").trim() ||
  String(agentDecision?.decisionMeta?.config_version ?? "").trim() ||
  "config_v3";

const promptVersion =
  String(input?.promptVersion ?? input?.ctx?.run?.promptVersion ?? "").trim() ||
  String(agentDecision?.decisionMeta?.prompt_version ?? "").trim() ||
  "prompt_v3";

const model =
  String(input?.model ?? input?.ctx?.run?.model ?? "").trim() ||
  String(agentDecision?.decisionMeta?.model ?? "").trim() ||
  "gpt-5.2";

const universeScope = Array.isArray(input?.run?.universe_scope)
  ? input.run.universe_scope
  : (enableFx ? ["EQUITY", "CRYPTO", "FX"] : ["EQUITY", "CRYPTO"]);
const inputSnapshot = isObj(input?.run?.inputSnapshot) ? deepClone(input.run.inputSnapshot) : {};
if (!Array.isArray(inputSnapshot.universe_scope)) inputSnapshot.universe_scope = universeScope;
if (inputSnapshot.enable_fx === undefined) inputSnapshot.enable_fx = enableFx;
if (inputSnapshot.fx_enabled_pairs === undefined) inputSnapshot.fx_enabled_pairs = fxEnabledSet.size;

const dataQuality = {
  okForTrading: !!okForTrading,
  requestedSymbolsCount,
  pricedSymbolsCount: pricedForRequested,
  pricesCoverageRequested,
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
    strategyVersion,
    configVersion,
    promptVersion,
    enable_fx: enableFx,
    universe_scope: universeScope,
    inputSnapshot,
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
const fxActionsCount = Array.isArray(agentDecision?.actions)
  ? agentDecision.actions.filter((a) => normAssetClass(a?.assetClass ?? a?.asset_class, a?.symbol) === "FX").length
  : 0;
const caveatsCount = Array.isArray(agentDecision?.dataCaveats) ? agentDecision.dataCaveats.length : 0;

const commentary = `PM_DECISION=${decision} | orders=${orders.length} | actions=${actionsCount} | fx_actions=${fxActionsCount} | cash0=${cash0} | caveats=${caveatsCount} | priceCoverage=${Math.round(pricesCoverageRequested * 1000) / 10}%`;

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

const crypto = require("crypto");

function sha1(x) {
  return crypto.createHash("sha1").update(String(x || ""), "utf8").digest("hex");
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function text(v, maxLen = 4000) {
  const s = String(v || "").replace(/\s+/g, " ").trim();
  return s.slice(0, maxLen);
}

function clamp(v, lo = 0, hi = 100) {
  if (!Number.isFinite(v)) return lo;
  return Math.max(lo, Math.min(hi, v));
}

function pct(v) {
  const n = num(v);
  if (n === null) return null;
  if (Math.abs(n) <= 1.5) return n * 100;
  return n;
}

function scoreHigher(v, floor, ceil) {
  if (v === null || v === undefined) return null;
  if (v <= floor) return 0;
  if (v >= ceil) return 100;
  return Math.round(((v - floor) / (ceil - floor)) * 100);
}

function scoreLower(v, best, worst) {
  if (v === null || v === undefined) return null;
  if (v <= best) return 100;
  if (v >= worst) return 0;
  return Math.round((1 - (v - best) / (worst - best)) * 100);
}

function weighted(parts, fallback = 50) {
  let acc = 0;
  let wsum = 0;
  for (const p of parts) {
    if (!p || p.value === null || p.value === undefined) continue;
    const w = Number.isFinite(p.weight) ? p.weight : 1;
    acc += p.value * w;
    wsum += w;
  }
  if (wsum <= 0) return fallback;
  return Math.round(acc / wsum);
}

function priceFmt(v) {
  const n = num(v);
  if (n === null) return "n/a";
  return String(Math.round(n * 100) / 100);
}

function obj(v) {
  return v && typeof v === "object" && !Array.isArray(v) ? v : {};
}

return $input.all().map((item) => {
  const j = item.json || {};
  const nowIso = new Date().toISOString();

  const runId = String(j.run_id || "").trim();
  const asOfDate = String(j.asOfDate || nowIso.slice(0, 10));
  const symbol = String(j.Symbol || j.symbol || "").trim().toUpperCase();
  const boursoramaRef = String(j.BoursoramaRef || "").trim();

  const ok = j.ok === true;
  const err = text(j.error || "", 500);

  const profile = obj(j.profile);
  const price = obj(j.price);
  const val = obj(j.valuation);
  const prof = obj(j.profitability);
  const growth = obj(j.growth);
  const health = obj(j.financialHealth);
  const cons = obj(j.consensus);
  const meta = obj(j.meta);

  const companyName = text(j.Name || profile.longName || profile.shortName || symbol, 200);
  const sector = text(j.Sector || profile.sector || "", 120);
  const industry = text(profile.industry || "", 120);
  const country = text(profile.country || "", 80);
  const currency = text(profile.currency || "EUR", 16);
  const fetchedAt = text(j.fetchedAt || j.nowIso || nowIso, 64);

  const currentPrice = num(price.currentPrice);
  const marketCap = num(price.marketCap);
  const beta = num(price.beta);
  const debtToEquity = num(health.debtToEquity);
  const currentRatio = num(health.currentRatio);
  const quickRatio = num(health.quickRatio);
  const freeCashflow = num(prof.freeCashflow);

  const gm = pct(prof.grossMargins);
  const om = pct(prof.operatingMargins);
  const pm = pct(prof.profitMargins);
  const roe = pct(prof.returnOnEquity);
  const roa = pct(prof.returnOnAssets);

  const rg = pct(growth.revenueGrowth);
  const eg = pct(growth.earningsGrowth);
  const eqg = pct(growth.earningsQuarterlyGrowth);

  const trailingPE = num(val.trailingPE);
  const forwardPE = num(val.forwardPE);
  const peg = num(val.pegRatio);
  const pb = num(val.priceToBook);

  const recMean = num(cons.recommendationMean);
  const analysts = num(cons.numberOfAnalystOpinions);
  const targetMean = num(cons.targetMeanPrice);
  const targetHigh = num(cons.targetHighPrice);
  const targetLow = num(cons.targetLowPrice);
  const upside = num(cons.upsidePctToTargetMean);

  let fcfYield = null;
  if (freeCashflow !== null && marketCap !== null && marketCap > 0) {
    fcfYield = (freeCashflow / marketCap) * 100;
  }

  const qualityScore = weighted(
    [
      { value: scoreHigher(gm, 20, 65), weight: 1.0 },
      { value: scoreHigher(om, 8, 30), weight: 1.2 },
      { value: scoreHigher(pm, 5, 25), weight: 1.0 },
      { value: scoreHigher(roe, 8, 25), weight: 1.2 },
      { value: scoreHigher(roa, 2, 12), weight: 0.8 },
    ],
    50
  );

  const growthScore = weighted(
    [
      { value: scoreHigher(rg, -2, 15), weight: 1.0 },
      { value: scoreHigher(eg, -5, 20), weight: 1.2 },
      { value: scoreHigher(eqg, -10, 25), weight: 0.8 },
    ],
    50
  );

  const valuationScore = weighted(
    [
      { value: scoreLower(trailingPE, 10, 35), weight: 1.0 },
      { value: scoreLower(forwardPE, 9, 30), weight: 1.0 },
      { value: scoreLower(pb, 1.5, 8), weight: 0.7 },
      { value: scoreLower(peg, 0.8, 2.5), weight: 1.0 },
      { value: scoreHigher(upside, -5, 25), weight: 1.0 },
    ],
    50
  );

  const healthScore = weighted(
    [
      { value: scoreLower(debtToEquity, 30, 250), weight: 1.2 },
      { value: scoreHigher(currentRatio, 0.8, 2.2), weight: 1.0 },
      { value: scoreHigher(quickRatio, 0.7, 1.8), weight: 0.8 },
      { value: scoreHigher(fcfYield, 2, 8), weight: 1.0 },
    ],
    50
  );

  let dispersionPct = null;
  if (targetHigh !== null && targetLow !== null && targetMean !== null && targetMean > 0) {
    dispersionPct = ((targetHigh - targetLow) / targetMean) * 100;
  }

  const consensusScore = weighted(
    [
      { value: scoreLower(recMean, 1.5, 4.2), weight: 1.2 },
      { value: scoreHigher(analysts, 3, 20), weight: 0.8 },
      { value: scoreHigher(upside, 0, 25), weight: 1.0 },
      { value: scoreLower(dispersionPct, 10, 80), weight: 0.6 },
    ],
    50
  );

  const coverage = num(meta.dataCoveragePctApprox);

  let triageScore = Math.round(
    qualityScore * 0.32 +
      growthScore * 0.2 +
      valuationScore * 0.2 +
      healthScore * 0.18 +
      consensusScore * 0.1
  );
  if (coverage !== null && coverage < 35) triageScore -= 8;
  if (!ok) triageScore = Math.min(triageScore, 40);
  triageScore = clamp(triageScore);

  let riskScore = Math.round(100 - (healthScore * 0.45 + qualityScore * 0.3 + consensusScore * 0.15 + (coverage !== null ? coverage : 50) * 0.1));
  if (debtToEquity !== null && debtToEquity > 250) riskScore += 12;
  if (currentRatio !== null && currentRatio < 1.0) riskScore += 8;
  if (rg !== null && rg < 0) riskScore += 6;
  if (eg !== null && eg < 0) riskScore += 6;
  if (!ok) riskScore = Math.max(riskScore, 80);
  riskScore = clamp(riskScore);

  const bull = [];
  if (qualityScore >= 65) bull.push("High quality profitability profile.");
  if (growthScore >= 60) bull.push("Revenue/earnings growth remains supportive.");
  if (valuationScore >= 60) bull.push("Valuation appears acceptable vs fundamentals.");
  if (consensusScore >= 60) bull.push("Analyst consensus and target imply upside.");
  if (fcfYield !== null && fcfYield > 4) bull.push("Free cash-flow yield is healthy.");

  const bear = [];
  if (qualityScore < 45) bear.push("Weak quality metrics (margins/returns).");
  if (growthScore < 45) bear.push("Growth profile is soft or negative.");
  if (valuationScore < 40) bear.push("Valuation is stretched vs peers/risk.");
  if (debtToEquity !== null && debtToEquity > 220) bear.push("High leverage raises balance-sheet risk.");
  if (consensusScore < 45) bear.push("Sell-side sentiment is not supportive.");
  if (coverage !== null && coverage < 35) bear.push("Low data coverage lowers confidence.");
  if (!ok) bear.push(`Data provider issue: ${err || "unknown error"}.`);

  let horizon = "WATCH";
  if (triageScore >= 72 && riskScore <= 45) horizon = "LONG_TERM";
  else if (triageScore >= 58 && riskScore <= 62) horizon = "SWING";

  const basePx = targetMean !== null ? targetMean : currentPrice;
  const bullPx = targetHigh !== null ? targetHigh : (basePx !== null ? basePx * 1.15 : null);
  const bearPx = targetLow !== null ? targetLow : (basePx !== null ? basePx * 0.85 : null);
  const valuationText = `Bear: ~${priceFmt(bearPx)} | Base: ~${priceFmt(basePx)} | Bull: ~${priceFmt(bullPx)} ${currency}`.trim();

  const why = `Quality ${qualityScore}/100, Growth ${growthScore}/100, Valuation ${valuationScore}/100, Financial health ${healthScore}/100, Consensus ${consensusScore}/100. Bull case: ${bull.length ? bull.join(" ") : "No strong fundamental edge detected."}`;
  const risks = `Bear case: ${bear.length ? bear.join(" ") : "No major red flag in available data."}`;

  const nextSteps = [
    coverage !== null && coverage < 45 ? "Backfill missing fundamentals before sizing aggressively." : "",
    analysts !== null && analysts < 3 ? "Consensus depth is limited; confirm with additional sources." : "",
    horizon === "WATCH" ? "Wait for next earnings/news catalyst before action." : "",
  ]
    .filter(Boolean)
    .join(" | ");

  const sourceUrl = symbol ? `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}` : "";
  const triageRecordId = sha1(`${runId}|triage|${symbol}`);
  const consensusRecordId = sha1(`${runId}|consensus|${symbol}`);

  const triageRow = {
    RecordId: triageRecordId,
    RunId: runId,
    UpdatedAt: nowIso,
    fetchedAt: fetchedAt,
    AsOfDate: asOfDate,
    Status: ok ? "OK" : "ERR_SOURCE",
    Error: err || null,
    Symbol: symbol,
    Name: companyName,
    Sector: sector,
    Industry: industry,
    Country: country,
    BoursoramaRef: boursoramaRef,
    Source: "yfinance_api",
    SourceUrl: sourceUrl,
    Score: triageScore,
    funda_conf: triageScore,
    risk_score: riskScore,
    quality_score: qualityScore,
    growth_score: growthScore,
    valuation_score: valuationScore,
    health_score: healthScore,
    consensus_score: consensusScore,
    horizon: horizon,
    current_price: currentPrice,
    target_price: targetMean,
    upside_pct: upside,
    recommendation: text(cons.recommendationKey || "", 64),
    analyst_count: analysts,
    valuation: valuationText,
    why: text(why, 3000),
    risks: text(risks, 3000),
    nextSteps: text(nextSteps, 1500),
    data_coverage_pct: coverage,
    strategy_version: String(j.strategy_version || "ag3_v2_fundamentals"),
    config_version: String(j.config_version || "ag3_v2_default"),
  };

  const consensusRow = {
    RecordId: consensusRecordId,
    RunId: runId,
    UpdatedAt: nowIso,
    AsOfDate: asOfDate,
    Symbol: symbol,
    Name: companyName,
    Sector: sector,
    recommendation: text(cons.recommendationKey || "", 64),
    recommendationMean: recMean,
    analystCount: analysts,
    currentPrice: currentPrice,
    targetMeanPrice: targetMean,
    targetHighPrice: targetHigh,
    targetLowPrice: targetLow,
    upsidePct: upside,
    dispersionPct: dispersionPct,
    confidenceProxy: triageScore,
    riskProxy: riskScore,
    Source: "yfinance_api",
    SourceUrl: sourceUrl,
    Status: ok ? "OK" : "ERR_SOURCE",
    Error: err || null,
    horizon: horizon,
  };

  function metricRow(section, metric, value, unit = null, notes = null) {
    if (value === null || value === undefined || value === "") return null;
    const rid = sha1(`${runId}|metric|${symbol}|${section}|${metric}|${asOfDate}`);
    return {
      Symbol: symbol,
      RecordId: rid,
      BoursoramaRef: boursoramaRef || null,
      DataType: "metric",
      Section: section,
      Metric: metric,
      Period: null,
      Value: value,
      Unit: unit,
      AsOfDate: asOfDate,
      SourceUrl: sourceUrl,
      ExtractedAt: nowIso,
      RunId: runId,
      SigHash: sha1(`${metric}|${String(value)}`),
      Title: null,
      Author: null,
      Excerpt: null,
      RawText: null,
      Signal: null,
      Score: null,
      Currency: currency,
      TitleOrLabel: null,
      Notes: notes,
    };
  }

  const metricRows = [
    metricRow("scores", "triage_score", triageScore, "/100"),
    metricRow("scores", "risk_score", riskScore, "/100"),
    metricRow("scores", "quality_score", qualityScore, "/100"),
    metricRow("scores", "growth_score", growthScore, "/100"),
    metricRow("scores", "valuation_score", valuationScore, "/100"),
    metricRow("scores", "health_score", healthScore, "/100"),
    metricRow("scores", "consensus_score", consensusScore, "/100"),
    metricRow("price", "current_price", currentPrice, currency),
    metricRow("price", "market_cap", marketCap, currency),
    metricRow("price", "beta", beta, null),
    metricRow("valuation", "trailing_pe", trailingPE, "x"),
    metricRow("valuation", "forward_pe", forwardPE, "x"),
    metricRow("valuation", "peg_ratio", peg, "x"),
    metricRow("valuation", "price_to_book", pb, "x"),
    metricRow("profitability", "gross_margin_pct", gm, "%"),
    metricRow("profitability", "operating_margin_pct", om, "%"),
    metricRow("profitability", "profit_margin_pct", pm, "%"),
    metricRow("profitability", "roe_pct", roe, "%"),
    metricRow("profitability", "roa_pct", roa, "%"),
    metricRow("growth", "revenue_growth_pct", rg, "%"),
    metricRow("growth", "earnings_growth_pct", eg, "%"),
    metricRow("growth", "earnings_q_growth_pct", eqg, "%"),
    metricRow("health", "debt_to_equity", debtToEquity, "x"),
    metricRow("health", "current_ratio", currentRatio, "x"),
    metricRow("health", "quick_ratio", quickRatio, "x"),
    metricRow("health", "fcf_yield_pct", fcfYield, "%"),
    metricRow("consensus", "target_mean_price", targetMean, currency),
    metricRow("consensus", "target_high_price", targetHigh, currency),
    metricRow("consensus", "target_low_price", targetLow, currency),
    metricRow("consensus", "upside_pct", upside, "%"),
    metricRow("consensus", "recommendation_mean", recMean, "score"),
    metricRow("consensus", "analyst_count", analysts, "count"),
  ].filter(Boolean);

  return {
    json: {
      ...j,
      triageRow,
      consensusRow,
      metricRows,
    },
  };
});

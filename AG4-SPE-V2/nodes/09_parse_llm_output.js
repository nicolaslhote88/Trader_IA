function safeParse(value) {
  try {
    if (typeof value === "object" && value !== null) return value;
    return JSON.parse(value);
  } catch {
    return {};
  }
}

function clampScore(v, lo, hi, dflt) {
  const n = Number(v);
  if (!Number.isFinite(n)) return dflt;
  return Math.max(lo, Math.min(hi, Math.round(n)));
}

function cleanSentiment(v) {
  const s = String(v || "").trim();
  if (["Bullish", "Bearish", "Neutral"].includes(s)) return s;
  return "Neutral";
}

function cleanHorizon(v) {
  const s = String(v || "").trim();
  if (["Intraday", "Days", "Weeks", "Months"].includes(s)) return s;
  return "Days";
}

function cleanUrgency(v) {
  const s = String(v || "").trim();
  if (["Low", "Medium", "High"].includes(s)) return s;
  return "Low";
}

function cleanSignal(v) {
  const s = String(v || "").trim();
  if (["BUY", "SELL", "NEUTRAL", "WATCH"].includes(s)) return s;
  return "WATCH";
}

function cleanDrivers(v) {
  if (!Array.isArray(v)) return "";
  const arr = v
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .slice(0, 5);
  return arr.join(" | ");
}

function toBool(v, d = false) {
  if (typeof v === "boolean") return v;
  if (v == null) return d;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(s)) return true;
  if (["0", "false", "no", "n"].includes(s)) return false;
  return d;
}

const j = $json || {};
const raw = j.output?.[0]?.content?.[0]?.text || j.content || "{}";
const ai = safeParse(raw);
const nowIso = new Date().toISOString();

const isRelevant = typeof ai.isRelevant === "boolean" ? ai.isRelevant : true;
const impactScore = isRelevant ? clampScore(ai.impactScore, -10, 10, 0) : 0;
const sentiment = isRelevant ? cleanSentiment(ai.sentiment) : "Neutral";
const category = isRelevant ? String(ai.category || "Noise").trim() : "Noise";
const suggestedSignal = isRelevant ? cleanSignal(ai.suggestedSignal) : "WATCH";
const urgency = isRelevant ? cleanUrgency(ai.urgency) : "Low";
const confidence = clampScore(ai.confidence, 0, 100, isRelevant ? 50 : 0);
const horizon = cleanHorizon(ai.horizon);
const keyDrivers = cleanDrivers(ai.keyDrivers);
const needsFollowUp = toBool(ai.needsFollowUp, false);

return [
  {
    json: {
      run_id: j.run_id || "",
      db_path: j.db_path || "/files/duckdb/ag4_spe_v2.duckdb",
      newsId: j.newsId || null,
      symbol: j.symbol || "UNKNOWN",
      companyName: j.companyName || j.symbol || "",
      source: j.source || "boursorama",
      boursoramaRef: j.boursoramaRef || "",
      listingUrl: j.actualitesUrl || "",
      url: j.articleUrl || j.url || "",
      canonicalUrl: j.articleCanonicalUrl || j.canonicalUrl || j.articleUrl || j.url || "",
      title: j.articleTitle || j.title || j.articleTitleGuess || "",
      publishedAt: j.publishedAt || null,
      snippet: j.snippet || "",
      text: j.text || "",
      summary: String(ai.summary || "").trim(),
      category,
      impactScore,
      sentiment,
      confidence,
      horizon,
      urgency,
      suggestedSignal,
      keyDrivers,
      needsFollowUp,
      isRelevant,
      relevanceReason: String(ai.relevanceReason || "").trim(),
      action: "analyze",
      reason: j._reason || "new_item",
      status: "ANALYZED",
      firstSeenAt: j.firstSeenAt || j.seenNowAt || nowIso,
      analyzedAt: nowIso,
      fetchedAt: nowIso,
      _articlesLoopReset: false,
    },
  },
];

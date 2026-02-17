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

const j = $json || {};
const raw = j.output?.[0]?.content?.[0]?.text || j.content || "{}";
const ai = safeParse(raw);
const nowIso = new Date().toISOString();

const isRelevant = typeof ai.isRelevant === "boolean" ? ai.isRelevant : true;
const impactScore = isRelevant ? clampScore(ai.impactScore, -10, 10, 0) : 0;

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
      category: String(ai.category || "Noise").trim(),
      impactScore,
      sentiment: cleanSentiment(ai.sentiment),
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


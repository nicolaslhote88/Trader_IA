function toBool(v, fallback = true) {
  if (typeof v === "boolean") return v;
  if (v == null) return fallback;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(s)) return true;
  if (["0", "false", "no", "n"].includes(s)) return false;
  return fallback;
}

function pickReason(j) {
  if (j._reason) return String(j._reason);
  if (j._filterReason) return String(j._filterReason);
  if (j.statusCode && Number(j.statusCode) !== 200) return "article_http_error";
  return "skipped";
}

function pickStatus(reason) {
  if (reason === "duplicate_known") return "SKIPPED_DUPLICATE";
  if (String(reason).startsWith("too_old")) return "SKIPPED_TOO_OLD";
  if (reason === "article_http_error") return "ARTICLE_HTTP_ERROR";
  return "SKIPPED";
}

const j = $json || {};
const nowIso = new Date().toISOString();

const reason = pickReason(j);

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
      publishedAt: j.publishedAt || j.publishedAtGuess || null,
      snippet: j.snippet || j.snippetGuess || "",
      text: j.text || "",
      summary: j.summary || "",
      category: j.category || "Noise",
      impactScore: Number.isFinite(Number(j.impactScore)) ? Number(j.impactScore) : 0,
      sentiment: j.sentiment || "Neutral",
      isRelevant: toBool(j.isRelevant, true),
      relevanceReason: j.relevanceReason || j._filterReason || "Skipped",
      action: "skip",
      reason,
      status: j.status || pickStatus(reason),
      firstSeenAt: j.firstSeenAt || j.seenNowAt || nowIso,
      analyzedAt: j.analyzedAt || nowIso,
      fetchedAt: nowIso,
      _articlesLoopReset: false,
    },
  },
];


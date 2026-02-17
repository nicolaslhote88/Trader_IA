const LOOKBACK_DAYS = 120;
const MAX_TEXT_LEN = 12000;

const cutoff = new Date();
cutoff.setDate(cutoff.getDate() - LOOKBACK_DAYS);

function parseDate(v) {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

return $input.all().map((item) => {
  const j = item.json || {};

  let runAI = true;
  let filterReason = "date_ok_or_missing";

  const p = parseDate(j.publishedAt);
  if (p && p < cutoff) {
    runAI = false;
    filterReason = `too_old:${j.publishedAt}`;
  }

  const contentRaw = String(j.text || j.snippet || "");
  let content = contentRaw.slice(0, MAX_TEXT_LEN);
  if (contentRaw.length > MAX_TEXT_LEN) {
    content += "\n...[CONTENT TRUNCATED]...";
  }

  const payload = {
    symbol: j.symbol || "UNKNOWN",
    targetCompany: j.companyName || j.symbol || "Unknown",
    title: j.title || j.articleTitle || "No title",
    date: j.publishedAt || "unknown",
    url: j.articleCanonicalUrl || j.canonicalUrl || j.articleUrl || j.url || "",
    content,
  };

  return {
    json: {
      ...j,
      llmInput: JSON.stringify(payload),
      _runAI: runAI,
      _filterReason: filterReason,
      _articlesLoopReset: false,
    },
  };
});


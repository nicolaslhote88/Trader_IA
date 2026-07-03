const crypto = require("crypto");

const LIMIT = 10;

function sha1(s) {
  return crypto.createHash("sha1").update(String(s), "utf8").digest("hex");
}

function clampPlausibleDate(d) {
  // F2 (2026-07-02) : garde-fou B1 etendu au listing — hors [now-2ans ; now+7j] -> null
  // (published_at NULL => les consommateurs retombent sur first_seen_at).
  const t = d.getTime();
  const now = Date.now();
  if (t < now - 730 * 86400000 || t > now + 7 * 86400000) return null;
  return d.toISOString();
}

function parseListingDate(raw) {
  if (!raw) return null;
  const s = String(raw).trim();

  // F2 (2026-07-02) : ISO d'abord — la regex FR NON ANCREE mutilait les dates deja ISO
  // ("2026-06-29" matchee comme 26/06/29 -> 2029-06-26, cf audit 20260702, 235 lignes).
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    const iso = new Date(s);
    if (!isNaN(iso.getTime())) return clampPlausibleDate(iso);
  }

  const fr = s.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b/);
  if (fr) {
    let year = Number(fr[3]);
    if (year < 100) year += 2000;
    const month = Number(fr[2]) - 1;
    const day = Number(fr[1]);
    const d = new Date(Date.UTC(year, month, day, 0, 0, 0));
    if (!isNaN(d.getTime())) return clampPlausibleDate(d);
  }

  const iso = new Date(s);
  if (!isNaN(iso.getTime())) return clampPlausibleDate(iso);
  return null;
}

function canonical(url) {
  return String(url || "").split("#")[0].split("?")[0];
}

return $input.all().map((inItem) => {
  const item = inItem.json || {};
  const raw = Array.isArray(item.articles_raw) ? item.articles_raw : [];
  const symbol = String(item.symbol || "").toUpperCase();

  const uniq = new Map();

  for (const a of raw) {
    const articleCanonicalUrl = canonical(a.articleCanonicalUrl || a.articleUrl || "");
    if (!articleCanonicalUrl) continue;

    const newsId = sha1(`${symbol}|${articleCanonicalUrl}`);
    if (uniq.has(newsId)) continue;

    const publishedAtGuess = parseListingDate(a.publishedAtGuess);

    uniq.set(newsId, {
      newsId,
      articleUrl: a.articleUrl || articleCanonicalUrl,
      articleCanonicalUrl,
      articleTitleGuess: a.articleTitleGuess || null,
      publishedAtGuess,
      publishedAtTs: publishedAtGuess ? new Date(publishedAtGuess).getTime() : null,
      snippetGuess: a.snippetGuess || null,
    });
  }

  const articles = Array.from(uniq.values())
    .sort((a, b) => {
      const x = a.publishedAtTs ?? -1;
      const y = b.publishedAtTs ?? -1;
      return y - x;
    })
    .slice(0, LIMIT)
    .map((a, idx) => {
      const { publishedAtTs, ...rest } = a;
      return { ...rest, articleOrder: idx + 1 };
    });

  const hasArticles = articles.length > 0;

  return {
    json: {
      ...item,
      articles,
      articles_count: articles.length,
      hasArticles,
      _articlesStatus: hasArticles ? "HAS_ARTICLES" : "NO_ARTICLES",
      articles_limit: LIMIT,
    },
    pairedItem: inItem.pairedItem,
  };
});

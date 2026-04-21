const out = [];

for (const it of $input.all()) {
  const base = it.json || {};
  const articles = Array.isArray(base.articles) ? base.articles : [];

  const baseCtx = { ...base };
  delete baseCtx.articles;
  delete baseCtx.articles_raw;
  delete baseCtx.listingHtml;

  let first = true;
  for (const article of articles) {
    const articleUrl = article.articleUrl || article.articleCanonicalUrl || null;
    if (!articleUrl) continue;

    const articleCanonicalUrl =
      article.articleCanonicalUrl || String(articleUrl).split("#")[0].split("?")[0];

    out.push({
      json: {
        ...baseCtx,
        newsId: article.newsId || null,
        articleUrl,
        articleCanonicalUrl,
        url: articleUrl,
        canonicalUrl: articleCanonicalUrl,
        articleTitleGuess: article.articleTitleGuess || null,
        publishedAtGuess: article.publishedAtGuess || null,
        snippetGuess: article.snippetGuess || null,
        _articlesLoopReset: first === true,
        _status: "HAS_ARTICLE",
      },
    });

    first = false;
  }
}

return out;


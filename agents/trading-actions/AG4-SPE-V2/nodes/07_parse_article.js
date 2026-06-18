const MAX_TEXT_CHARS = 50000;
const MAX_SNIPPET_CHARS = 1200;

function htmlEntityDecode(str) {
  if (!str) return "";
  return String(str)
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function stripTags(html) {
  if (!html) return "";
  let s = String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<\/?[^>]+>/g, " ");
  s = htmlEntityDecode(s);
  return s.replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
}

function getMetaContent(html, key, mode) {
  if (!html || !key) return null;
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(`<meta[^>]+${mode}\\s*=\\s*["']${escaped}["'][^>]*>`, "i");
  const m = html.match(re);
  if (!m) return null;
  const content = m[0].match(/content\s*=\s*["']([^"']+)["']/i);
  return content ? stripTags(content[1]) : null;
}

function getFirst(html, re) {
  const m = html && html.match(re);
  if (!m) return null;
  return stripTags(m[1]) || null;
}

function extractParagraphs(htmlBlock) {
  if (!htmlBlock) return "";
  const out = [];
  const re = /<p\b[^>]*>([\s\S]*?)<\/p>/gi;
  let m;
  while ((m = re.exec(htmlBlock)) !== null) {
    const t = stripTags(m[1]);
    if (t && t.length >= 20) out.push(t);
  }
  return out.join("\n\n").trim();
}

function extractMainBlock(html) {
  if (!html) return "";
  const article = html.match(/<article\b[^>]*>([\s\S]*?)<\/article>/i);
  if (article?.[1]) return article[1];
  const main = html.match(/<main\b[^>]*>([\s\S]*?)<\/main>/i);
  if (main?.[1]) return main[1];
  const body = html.match(/<body\b[^>]*>([\s\S]*?)<\/body>/i);
  if (body?.[1]) return body[1];
  return html;
}

function normalizeDate(raw) {
  // B1 (2026-06-17) — anti-corruption des dates (cf docs/audits/20260617_ag4_spe_v2_analysis.md).
  // 1) on privilégie l'ISO des sources fiables (<time datetime>, meta article:published_time) ;
  // 2) fallback date FR JJ/MM/AAAA ANCRÉE (\b...\b, année 4 chiffres) pour ne PAS capter une
  //    année isolée du corps (échéance obligataire 2031, plan 2030, etc.) ;
  // 3) garde-fou de plausibilité : toute date hors [now-2ans ; now+7j] -> null
  //    => published_at restera NULL et les consommateurs retombent sur first_seen_at.
  if (!raw) return null;
  const s = String(raw).trim();
  if (!s) return null;

  let d = null;
  const iso = new Date(s);
  if (!isNaN(iso.getTime())) {
    d = iso;
  } else {
    const fr = s.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{4})\b/);
    if (fr) {
      const cand = new Date(Date.UTC(Number(fr[3]), Number(fr[2]) - 1, Number(fr[1]), 0, 0, 0));
      if (!isNaN(cand.getTime())) d = cand;
    }
  }
  if (!d) return null;

  const now = Date.now();
  const t = d.getTime();
  if (t < now - 730 * 86400000 || t > now + 7 * 86400000) return null;
  return d.toISOString();
}

function truncate(str, maxLen) {
  const s = String(str || "");
  if (s.length <= maxLen) return s;
  return `${s.slice(0, maxLen)}\n\n[TRUNCATED ${s.length} -> ${maxLen}]`;
}

return $input.all().map((item) => {
  const j = { ...(item.json || {}) };
  const html = j.articleHtml || j.body || j.data || j.html || j.response || "";

  const h1 = getFirst(html, /<h1\b[^>]*>([\s\S]*?)<\/h1>/i);
  const ogTitle = getMetaContent(html, "og:title", "property");
  const docTitle = getFirst(html, /<title\b[^>]*>([\s\S]*?)<\/title>/i);
  const title = h1 || ogTitle || j.articleTitleGuess || j.title || docTitle || null;

  const timeDt = (html.match(/<time\b[^>]*datetime\s*=\s*["']([^"']+)["'][^>]*>/i) || [])[1];
  const metaPub = getMetaContent(html, "article:published_time", "property");
  const metaDate = getMetaContent(html, "date", "name");
  const publishedAt = normalizeDate(timeDt || metaPub || metaDate || j.publishedAt || j.publishedAtGuess);

  const metaDesc = getMetaContent(html, "description", "name");
  const ogDesc = getMetaContent(html, "og:description", "property");

  const main = extractMainBlock(html);
  let text = extractParagraphs(main);
  if (!text) text = stripTags(main).slice(0, 20000);
  text = truncate(text, MAX_TEXT_CHARS);

  let snippet = metaDesc || ogDesc || j.snippet || j.snippetGuess || (text ? text.slice(0, 280) : "");
  snippet = truncate(snippet, MAX_SNIPPET_CHARS);

  return {
    json: {
      ...j,
      title,
      articleTitle: title,
      publishedAt: publishedAt || j.publishedAt || null,
      snippet,
      text,
      parseMethod: "html_regex_v3",
      parsedAt: new Date().toISOString(),
      _articlesLoopReset: false,
    },
  };
});


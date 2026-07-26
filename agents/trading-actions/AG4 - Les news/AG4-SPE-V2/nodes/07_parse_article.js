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

function clampPlausibleDate(d) {
  // Garde-fou de plausibilite : hors [now-2ans ; now+7j] -> null
  // (published_at NULL => les consommateurs retombent sur first_seen_at).
  const t = d.getTime();
  const now = Date.now();
  if (t < now - 730 * 86400000 || t > now + 7 * 86400000) return null;
  return d.toISOString();
}

function normalizeDate(raw) {
  // FIX 2026-07-13 : S16 avait garde l'ancienne regex FR NON ancree testee AVANT
  // l'ISO, qui mutilait les dates ISO d'article ("2026-07-10T.." -> match 26-07-10 -> 2010-07-26)
  // et n'avait aucun garde-fou de plausibilite. Alignement sur S07 / 07_parse_article.js.
  if (!raw) return null;
  const s = String(raw).trim();

  // 1) ISO d'abord (source fiable <time datetime>/meta article:published_time)
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    const iso = new Date(s);
    if (!isNaN(iso.getTime())) return clampPlausibleDate(iso);
  }

  // 2) date FR JJ/MM/AAAA ANCREE, annee 4 chiffres uniquement (pas de 2 chiffres ambigus)
  const fr = s.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{4})\b/);
  if (fr) {
    const d = new Date(Date.UTC(Number(fr[3]), Number(fr[2]) - 1, Number(fr[1]), 0, 0, 0));
    if (!isNaN(d.getTime())) return clampPlausibleDate(d);
  }

  // 3) dernier recours : ISO libre
  const iso = new Date(s);
  if (!isNaN(iso.getTime())) return clampPlausibleDate(iso);
  return null;
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
      publishedAt: publishedAt || null, // F2 (2026-07-02) : ne plus contourner le garde B1 via j.publishedAt corrompu
      snippet,
      text,
      parseMethod: "html_regex_v3",
      parsedAt: new Date().toISOString(),
      _articlesLoopReset: false,
    },
  };
});

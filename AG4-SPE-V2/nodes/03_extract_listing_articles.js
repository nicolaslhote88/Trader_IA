function htmlEntityDecode(str) {
  if (!str) return "";
  return String(str)
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#x27;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)));
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

function toAbsolute(url) {
  if (!url) return null;
  const u = String(url).trim();
  if (!u) return null;
  if (u.startsWith("http://") || u.startsWith("https://")) return u;
  if (u.startsWith("//")) return "https:" + u;
  if (u.startsWith("/")) return "https://www.boursorama.com" + u;
  return "https://www.boursorama.com/" + u;
}

function canonicalize(url) {
  if (!url) return null;
  return String(url).split("#")[0].split("?")[0];
}

function decodeDataRel(b64) {
  try {
    return Buffer.from(String(b64), "base64").toString("utf8") || null;
  } catch {
    return null;
  }
}

function isBoursoramaArticle(url) {
  if (!url) return false;
  const u = canonicalize(url);
  return (
    /^https:\/\/www\.boursorama\.com\/bourse\/actualites\/.+-[0-9a-f]{32}$/i.test(u) ||
    /^https:\/\/www\.boursorama\.com\/actualites\/.+-[0-9a-f]{32}$/i.test(u)
  );
}

function extractFromLiBlock(blockHtml) {
  const block = String(blockHtml);
  let relUrl = null;

  const hrefMatch = block.match(/<a\b[^>]*href="([^"]+)"/i);
  if (hrefMatch?.[1]) relUrl = hrefMatch[1];

  if (!relUrl) {
    const relMatches = [...block.matchAll(/\bdata-rel="([^"]+)"/gi)].map((m) => m[1]);
    for (const b64 of relMatches) {
      const decoded = decodeDataRel(b64);
      if (decoded && decoded.includes("/actualites/")) {
        relUrl = decoded;
        break;
      }
    }
  }

  const articleUrl = toAbsolute(relUrl);
  const articleCanonicalUrl = canonicalize(articleUrl);

  let title = block.match(/\btitle="([^"]+)"/i)?.[1] || null;
  if (!title) {
    title = block.match(/c-list-details-news__subject[^>]*>([\s\S]*?)<\/(?:a|span)>/i)?.[1] || null;
  }
  title = title ? stripTags(title) : null;

  const publishedAtGuess = stripTags(block.match(/c-source__time[^>]*>([^<]+)</i)?.[1] || "") || null;
  const snippetGuess =
    stripTags(
      block.match(
        /<p[^>]*class="[^"]*c-list-details-news__content[^"]*"[^>]*>([\s\S]*?)<\/p>/i
      )?.[1] || ""
    ) || null;

  return {
    articleUrl,
    articleCanonicalUrl,
    articleTitleGuess: title,
    publishedAtGuess,
    snippetGuess,
  };
}

const input = $input.first().json;
const html = input.listingHtml || input.body || input.html || input.response || "";
const actualitesUrl = input.actualitesUrl ? canonicalize(input.actualitesUrl) : null;

const dropped = {
  null_url: 0,
  not_article: 0,
  is_listing: 0,
  duplicate: 0,
};

const articles_raw = [];
const seen = new Set();

const liRe = /<li\b[^>]*class="[^"]*c-list-details-news__line[^"]*"[^>]*>([\s\S]*?)<\/li>/gi;
const liBlocks = [...html.matchAll(liRe)].map((m) => m[1]);

let extractionMethod = "li_blocks";

if (liBlocks.length === 0) {
  extractionMethod = "fallback_anchors";
  const re = /<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gsi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const articleUrl = toAbsolute(m[1]);
    const articleCanonicalUrl = canonicalize(articleUrl);
    const title = stripTags(m[2]) || null;

    if (!articleUrl) {
      dropped.null_url++;
      continue;
    }
    if (actualitesUrl && articleCanonicalUrl === actualitesUrl) {
      dropped.is_listing++;
      continue;
    }
    if (!isBoursoramaArticle(articleUrl)) {
      dropped.not_article++;
      continue;
    }
    if (seen.has(articleCanonicalUrl)) {
      dropped.duplicate++;
      continue;
    }

    seen.add(articleCanonicalUrl);
    articles_raw.push({
      articleUrl,
      articleCanonicalUrl,
      articleTitleGuess: title,
      publishedAtGuess: null,
      snippetGuess: null,
    });
  }
} else {
  for (const block of liBlocks) {
    const parsed = extractFromLiBlock(block);
    if (!parsed.articleUrl) {
      dropped.null_url++;
      continue;
    }
    if (actualitesUrl && parsed.articleCanonicalUrl === actualitesUrl) {
      dropped.is_listing++;
      continue;
    }
    if (!isBoursoramaArticle(parsed.articleUrl)) {
      dropped.not_article++;
      continue;
    }
    if (seen.has(parsed.articleCanonicalUrl)) {
      dropped.duplicate++;
      continue;
    }
    seen.add(parsed.articleCanonicalUrl);
    articles_raw.push(parsed);
  }
}

return [
  {
    json: {
      ...input,
      extractionMethod,
      candidates_total: liBlocks.length,
      filtered_total: articles_raw.length,
      dropped_reason_counts: dropped,
      articles_raw,
      articles_raw_count: articles_raw.length,
    },
  },
];


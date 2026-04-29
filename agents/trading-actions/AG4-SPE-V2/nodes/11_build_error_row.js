const crypto = require("crypto");

function hash(s) {
  return crypto.createHash("sha1").update(String(s), "utf8").digest("hex");
}

function compactJson(obj) {
  try {
    return JSON.stringify(obj);
  } catch {
    return String(obj);
  }
}

const j = $json || {};
const nowIso = new Date().toISOString();

const stage = j._errorStage || (j.articleUrl ? "article_fetch" : "listing_fetch");
const url = j.articleUrl || j.articleCanonicalUrl || j.actualitesUrl || j.url || "";
const httpCode = Number.isFinite(Number(j.statusCode)) ? Number(j.statusCode) : null;

const message =
  j.error?.message ||
  j.errorMessage ||
  j.notes ||
  (httpCode && httpCode !== 200 ? `HTTP_${httpCode}` : "unknown_error");

const signature = [
  j.run_id || "no_run",
  stage,
  j.symbol || "no_symbol",
  url || "no_url",
  httpCode || "no_code",
  String(message).slice(0, 120),
  nowIso.slice(0, 16),
].join("|");

return [
  {
    json: {
      errorId: hash(signature),
      run_id: j.run_id || "",
      db_path: j.db_path || "/files/duckdb/ag4_spe_v2.duckdb",
      stage,
      symbol: j.symbol || "",
      companyName: j.companyName || "",
      url,
      httpCode,
      message: String(message),
      rawError: compactJson({
        statusCode: j.statusCode,
        error: j.error || null,
        responseHeaders: j.headers || null,
      }),
      occurredAt: nowIso,
    },
  },
];


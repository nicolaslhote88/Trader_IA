const BATCH_SIZE = 20;
const ROTATION_KEY = "ag4_spe_v2_last_symbol_index";

function safeJsonParse(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  if (typeof raw !== "string") return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function normalizeRef(v) {
  return String(v || "").trim().toUpperCase();
}

const rows = $input.all().map((i) => i.json || {});
const allCandidates = [];

for (const r of rows) {
  const enabled = r.enabled !== false && String(r.enabled || "").toLowerCase() !== "false";
  if (!enabled) continue;

  const symbol = String(r.symbol || r.Symbol || "").trim().toUpperCase();
  if (!symbol) continue;

  const notes = safeJsonParse(r.notesJson || r.Notes || null);
  const boursoramaRef = normalizeRef(
    r.boursoramaRef || r.BoursoramaRef || notes?.boursoramaRef || notes?.boursoramaCode || ""
  );
  if (!boursoramaRef) continue;

  const coursUrl = `https://www.boursorama.com/cours/${boursoramaRef}/`;
  const actualitesUrl = `https://www.boursorama.com/cours/actualites/${boursoramaRef}/`;

  allCandidates.push({
    json: {
      queueId: `${symbol}|boursorama|actualites`,
      symbol,
      companyName: String(r.companyName || r.Name || symbol),
      isin: r.isin || null,
      assetClass: r.assetClass || null,
      exchange: r.exchange || null,
      currency: r.currency || null,
      country: r.country || null,
      boursoramaRef,
      coursUrl,
      actualitesUrl,
      source: "boursorama",
      enabled: true,
      db_path: r.db_path || "/files/duckdb/ag4_spe_v2.duckdb",
    },
  });
}

const staticData = $getWorkflowStaticData("global");
let start = Number(staticData[ROTATION_KEY] || 0);
const totalItems = allCandidates.length;

if (!Number.isFinite(start) || start < 0 || start >= totalItems) {
  start = 0;
}

if (totalItems === 0) {
  staticData[ROTATION_KEY] = 0;
  return [];
}

const end = start + BATCH_SIZE;
const batch = allCandidates.slice(start, end);

staticData[ROTATION_KEY] = end >= totalItems ? 0 : end;

batch.forEach((item, idx) => {
  item.json._batchInfo = {
    batchIndex: idx + 1,
    globalIndex: start + idx + 1,
    totalItems,
    nextStart: staticData[ROTATION_KEY],
  };
});

return batch;


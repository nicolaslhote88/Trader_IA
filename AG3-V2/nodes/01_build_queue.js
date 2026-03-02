function pick(row, keys) {
  if (!row || typeof row !== "object") return null;
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null && String(row[key]).trim() !== "") {
      return row[key];
    }
    const lower = key.toLowerCase();
    for (const k of Object.keys(row)) {
      if (String(k).toLowerCase() === lower) {
        const v = row[k];
        if (v !== undefined && v !== null && String(v).trim() !== "") return v;
      }
    }
  }
  return null;
}

function truthy(v) {
  if (typeof v === "boolean") return v;
  const s = String(v || "").trim().toLowerCase();
  return ["1", "true", "yes", "y", "oui", "ok", "enabled"].includes(s);
}

function normalizeAssetClass(raw, symbol) {
  const cls = String(raw || "").trim().toUpperCase();
  if (cls === "FX" || cls === "FOREX") return "FX";
  if (cls === "CRYPTO") return "CRYPTO";
  if (cls === "EQUITY" || cls === "ETF" || cls === "STOCK") return "EQUITY";

  const sym = String(symbol || "").trim().toUpperCase();
  if (sym.startsWith("FX:") || sym.endsWith("=X")) return "FX";
  return "EQUITY";
}

const BATCH_SIZE = 50;
const ctx = $("AG3V2.00 - Init Context").first().json || {};
const rows = $input.all().map((i) => i.json || {});

const queue = [];
let skippedNonEquityCount = 0;
for (const r of rows) {
  const symbolRaw = pick(r, ["Symbol", "symbol", "Ticker", "ticker"]);
  if (!symbolRaw) continue;

  const symbol = String(symbolRaw).trim().toUpperCase();
  if (!symbol) continue;

  const assetClassRaw = pick(r, ["AssetClass", "assetClass", "asset_class", "Type", "type"]);
  const assetClass = normalizeAssetClass(assetClassRaw, symbol);
  if (assetClass !== "EQUITY") {
    skippedNonEquityCount += 1;
    continue;
  }

  const enabledRaw = pick(r, ["Enabled", "enabled", "Active", "active"]);
  if (ctx.only_enabled && enabledRaw !== null && !truthy(enabledRaw)) continue;

  const name = String(pick(r, ["Name", "name", "Company", "company"]) || symbol).trim();
  const sector = String(pick(r, ["Sector", "sector"]) || "").trim();
  const boursoramaRef = String(pick(r, ["BoursoramaRef", "boursorama_ref", "Boursorama"]) || "").trim();

  queue.push({
    run_id: ctx.run_id || null,
    nowIso: ctx.nowIso || new Date().toISOString(),
    asOfDate: ctx.asOfDate || null,
    api_base: ctx.api_base || "http://yfinance-api:8080",
    db_path: ctx.db_path || "/files/duckdb/ag3_v2.duckdb",
    strategy_version: ctx.strategy_version || "ag3_v2_fundamentals",
    config_version: ctx.config_version || "ag3_v2_default",
    Symbol: symbol,
    Name: name,
    Sector: sector,
    BoursoramaRef: boursoramaRef,
    UniverseEnabled: enabledRaw === null ? true : truthy(enabledRaw),
  });
}

// Deterministic order for batch rotation.
queue.sort((a, b) => String(a.Symbol).localeCompare(String(b.Symbol)));

const maxSymbols = Number(ctx.max_symbols || 0);
const finalQueue = Number.isFinite(maxSymbols) && maxSymbols > 0 ? queue.slice(0, Math.floor(maxSymbols)) : queue;

return [
  {
    json: {
      run_id: ctx.run_id || null,
      nowIso: ctx.nowIso || new Date().toISOString(),
      asOfDate: ctx.asOfDate || null,
      api_base: ctx.api_base || "http://yfinance-api:8080",
      db_path: ctx.db_path || "/files/duckdb/ag3_v2.duckdb",
      strategy_version: ctx.strategy_version || "ag3_v2_fundamentals",
      config_version: ctx.config_version || "ag3_v2_default",
      max_symbols: Number.isFinite(maxSymbols) && maxSymbols > 0 ? Math.floor(maxSymbols) : 0,
      batch_size: BATCH_SIZE,
      skipped_non_equity_count: skippedNonEquityCount,
      _all_queue: finalQueue,
      _total_pool: finalQueue.length,
    },
  },
];

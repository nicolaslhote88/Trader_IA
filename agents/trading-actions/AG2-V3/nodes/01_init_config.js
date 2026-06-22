// AG2-V3 - Init Config (actions/ETF/crypto only)
// Batch rotation remains handled by DuckDB init.
const DEFAULT_ROTATION_MODE = "ACTIONS_ONLY";
const DEFAULT_BATCH_SIZE = 10;
const DEFAULT_BATCH_STATE_KEY = "last_index_actions";

function getField(row, names) {
  if (!row || typeof row !== "object") return undefined;
  const keys = Object.keys(row);
  for (const name of names) {
    if (row[name] !== undefined) return row[name];
    const lower = String(name).toLowerCase();
    for (const k of keys) {
      if (String(k).toLowerCase() === lower) return row[k];
    }
  }
  return undefined;
}

function toBool(v, dflt = false) {
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return dflt;
  if (["1", "true", "yes", "y", "on", "enabled"].includes(s)) return true;
  if (["0", "false", "no", "n", "off", "disabled"].includes(s)) return false;
  return dflt;
}

function normAssetClass(v) {
  const raw = String(v ?? "").trim().toUpperCase();
  if (raw === "CRYPTO") return "CRYPTO";
  if (raw === "ETF") return "ETF";
  return "EQUITY";
}

function isCurrencyPairRow(row, rawSymbol, symbolYahoo) {
  const assetRaw = String(getField(row, ["AssetClass", "assetClass", "asset_class"]) ?? "").trim().toUpperCase();
  const s = String(rawSymbol || "").trim().toUpperCase();
  const y = String(symbolYahoo || "").trim().toUpperCase();
  return assetRaw === "CURRENCY" || s.startsWith("F" + "X:") || s.endsWith("=X") || y.endsWith("=X");
}

function buildInstrument(row) {
  const rawSymbol = String(
    getField(row, ["Symbol", "symbol", "Ticker", "ticker", "symbol_yahoo", "YahooSymbol"]) ?? ""
  )
    .trim()
    .toUpperCase();
  if (!rawSymbol) return null;

  const symbolYahoo = String(getField(row, ["symbol_yahoo", "YahooSymbol"]) ?? rawSymbol).trim().toUpperCase();
  if (isCurrencyPairRow(row, rawSymbol, symbolYahoo)) return null;

  const symbolInternal =
    String(getField(row, ["symbol_internal", "SymbolInternal"]) ?? "").trim().toUpperCase() || rawSymbol;

  return {
    symbol: symbolInternal,
    symbol_internal: symbolInternal,
    symbol_yahoo: symbolYahoo || rawSymbol,
    name: String(getField(row, ["Name", "name"]) ?? symbolInternal).trim(),
    asset_class: normAssetClass(getField(row, ["AssetClass", "assetClass", "asset_class"])),
    exchange: String(getField(row, ["Exchange", "exchange"]) ?? "Euronext Paris").trim(),
    currency: String(getField(row, ["Currency", "currency"]) ?? "EUR").trim().toUpperCase(),
    country: String(getField(row, ["Country", "country"]) ?? "").trim(),
    sector: String(getField(row, ["Sector", "sector"]) ?? "").trim(),
    industry: String(getField(row, ["Industry", "industry"]) ?? "").trim(),
    isin: String(getField(row, ["ISIN", "isin"]) ?? "").trim(),
    enabled: toBool(getField(row, ["Enabled", "enabled", "Active", "active"]), true),
    boursorama_ref: String(getField(row, ["BoursoramaRef", "boursorama_ref"]) ?? "").trim(),
  };
}

const items = $input.all();
const cfgSource = items[0]?.json || {};
const batchSizeRaw = Number(getField(cfgSource, ["AG2_BATCH_SIZE", "batch_size"]));
const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.floor(batchSizeRaw) : DEFAULT_BATCH_SIZE;
const rotationMode = String(getField(cfgSource, ["AG2_ROTATION_MODE", "rotation_mode"]) ?? DEFAULT_ROTATION_MODE)
  .trim()
  .toUpperCase();

const universeRaw = items.map((i) => i.json || {});
const universe = [];
for (const row of universeRaw) {
  const inst = buildInstrument(row);
  if (inst) universe.push(inst);
}

const processQueue = universe.filter((u) => u.enabled);
const batchStateKey = DEFAULT_BATCH_STATE_KEY;

if (processQueue.length === 0) {
  return [
    {
      json: {
        ok: false,
        error: "NO_SYMBOLS",
        universe_mode: "ACTIONS_ONLY",
        rotation_mode: rotationMode,
        batch_state_key: batchStateKey,
        universe_total: universe.length,
        symbols: [],
      },
    },
  ];
}

return [
  {
    json: {
      ok: true,
      _universe: universe,
      _process_queue: processQueue,
      _all_symbols: processQueue.map((u) => u.symbol),
      yfinance_api_base: String(getField(cfgSource, ["yfinance_api_base"]) ?? "http://yfinance-api:8080"),
      intraday: { interval: "1h", lookback_days: 60, max_bars: 200, min_bars: 50 },
      daily: { interval: "1d", lookback_days: 400, max_bars: 400, min_bars: 200 },
      batch_size: batchSize,
      universe_mode: "ACTIONS_ONLY",
      rotation_mode: rotationMode,
      batch_state_key: batchStateKey,
      strategy_version: "strategy_v3",
      config_version: "config_v3",
      prompt_version: "prompt_v3",
      universe_scope: ["EQUITY", "ETF", "CRYPTO"],
    },
  },
];

// AG2-V3 - Init Config (multi-asset with FX addon)
// Batch rotation remains handled by DuckDB init.

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

function toNum(v, dflt = null) {
  if (v === null || v === undefined || v === "") return dflt;
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
}

function normAssetClass(v, symbolYahoo) {
  const raw = String(v ?? "").trim().toUpperCase();
  if (raw === "FX" || raw === "FOREX") return "FX";
  if (raw === "CRYPTO") return "CRYPTO";
  if (raw === "EQUITY" || raw === "STOCK" || raw === "ETF") return "EQUITY";
  if (String(symbolYahoo || "").trim().toUpperCase().endsWith("=X")) return "FX";
  return "EQUITY";
}

function sanitizePair6(v) {
  const s = String(v ?? "")
    .toUpperCase()
    .replace(/^FX:/, "")
    .replace("=X", "")
    .replace(/[^A-Z]/g, "");
  if (s.length < 6) return "";
  return s.slice(0, 6);
}

function toFxYahoo(raw) {
  const pair = sanitizePair6(raw);
  return pair ? `${pair}=X` : "";
}

function buildInstrument(row) {
  const rawSymbol = String(
    getField(row, ["Symbol", "symbol", "Ticker", "ticker", "symbol_yahoo", "YahooSymbol"]) ?? ""
  )
    .trim()
    .toUpperCase();
  if (!rawSymbol) return null;

  const enabled = toBool(getField(row, ["Enabled", "enabled", "Active", "active"]), true);
  let symbolYahoo = String(getField(row, ["symbol_yahoo", "YahooSymbol"]) ?? rawSymbol).trim().toUpperCase();
  const assetClass = normAssetClass(getField(row, ["AssetClass", "assetClass", "asset_class"]), symbolYahoo);

  let baseCcy = String(getField(row, ["base_ccy", "BaseCCY", "baseCcy"]) ?? "")
    .trim()
    .toUpperCase();
  let quoteCcy = String(getField(row, ["quote_ccy", "QuoteCCY", "quoteCcy"]) ?? "")
    .trim()
    .toUpperCase();
  let symbolInternal = "";
  let pipSize = toNum(getField(row, ["pip_size", "PipSize"]), null);
  let priceDecimals = toNum(getField(row, ["price_decimals", "PriceDecimals"]), null);
  let tradingHours = String(getField(row, ["trading_hours", "TradingHours"]) ?? "").trim();

  if (assetClass === "FX") {
    symbolYahoo = toFxYahoo(symbolYahoo || rawSymbol);
    const pair = sanitizePair6(symbolYahoo);
    if (!pair) return null;
    baseCcy = baseCcy || pair.slice(0, 3);
    quoteCcy = quoteCcy || pair.slice(3, 6);
    symbolInternal =
      String(getField(row, ["symbol_internal", "SymbolInternal"]) ?? "").trim().toUpperCase() || `FX:${pair}`;
    if (pipSize === null) pipSize = quoteCcy === "JPY" ? 0.01 : 0.0001;
    if (priceDecimals === null) priceDecimals = quoteCcy === "JPY" ? 3 : 5;
    if (!tradingHours) tradingHours = "24x5";
  } else {
    symbolInternal =
      String(getField(row, ["symbol_internal", "SymbolInternal"]) ?? "").trim().toUpperCase() || rawSymbol;
    if (!tradingHours) tradingHours = "";
  }

  return {
    symbol: symbolInternal,
    symbol_internal: symbolInternal,
    symbol_yahoo: symbolYahoo || rawSymbol,
    name: String(getField(row, ["Name", "name"]) ?? symbolInternal).trim(),
    asset_class: assetClass,
    exchange: String(getField(row, ["Exchange", "exchange"]) ?? "Euronext Paris").trim(),
    currency: String(getField(row, ["Currency", "currency"]) ?? "EUR").trim().toUpperCase(),
    country: String(getField(row, ["Country", "country"]) ?? "").trim(),
    sector: String(getField(row, ["Sector", "sector"]) ?? "").trim(),
    industry: String(getField(row, ["Industry", "industry"]) ?? "").trim(),
    isin: String(getField(row, ["ISIN", "isin"]) ?? "").trim(),
    enabled,
    boursorama_ref: String(getField(row, ["BoursoramaRef", "boursorama_ref"]) ?? "").trim(),
    base_ccy: baseCcy || null,
    quote_ccy: quoteCcy || null,
    pip_size: pipSize,
    price_decimals: priceDecimals,
    trading_hours: tradingHours,
  };
}

const items = $input.all();

const cfgSource = items[0]?.json || {};
const enableFx =
  toBool(getField(cfgSource, ["ENABLE_FX", "enable_fx"]), false) ||
  toBool($env.ENABLE_FX, false);
const batchSizeRaw = toNum(getField(cfgSource, ["AG2_BATCH_SIZE", "batch_size"]), null);
const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.floor(batchSizeRaw) : 10;

const universeRaw = items.map((i) => i.json || {});
const universe = [];
for (const row of universeRaw) {
  const inst = buildInstrument(row);
  if (!inst) continue;
  universe.push(inst);
}

const processQueue = universe.filter((u) => {
  if (!u.enabled) return false;
  if (u.asset_class === "FX" && !enableFx) return false;
  return true;
});

if (processQueue.length === 0) {
  return [
    {
      json: {
        ok: false,
        error: "NO_SYMBOLS",
        enable_fx: enableFx,
        universe_total: universe.length,
        symbols: [],
      },
    },
  ];
}

const fxUniverseCount = processQueue.filter((u) => u.asset_class === "FX").length;
const universeScope = ["EQUITY", "CRYPTO"];
if (enableFx) universeScope.push("FX");

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
      enable_fx: enableFx,
      fx_universe_count: fxUniverseCount,
      strategy_version: "strategy_v3",
      config_version: "config_v3",
      prompt_version: "prompt_v3",
      universe_scope: universeScope,
    },
  },
];

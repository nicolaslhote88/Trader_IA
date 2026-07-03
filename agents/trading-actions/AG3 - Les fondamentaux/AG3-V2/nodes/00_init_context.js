function pad(n) {
  return String(n).padStart(2, "0");
}

function isoDate(d) {
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

const now = new Date();
const ts = `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}`;
const rand = Math.random().toString(16).slice(2, 8).toUpperCase();

const input = ($input.first() && $input.first().json) ? $input.first().json : {};
const maxSymbolsIn = Number(input.max_symbols);

return [
  {
    json: {
      run_id: `AG3V2_${ts}_${rand}`,
      nowIso: now.toISOString(),
      asOfDate: isoDate(now),
      api_base: String(input.api_base || "http://yfinance-api:8080"),
      db_path: String(input.db_path || "/files/duckdb/ag3_v2.duckdb"),
      max_symbols: Number.isFinite(maxSymbolsIn) && maxSymbolsIn > 0 ? Math.floor(maxSymbolsIn) : 0,
      only_enabled: true,
      strategy_version: "ag3_v2_fundamentals",
      config_version: "ag3_v2_default",
    },
  },
];

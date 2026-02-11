// AG2-V2 — Init Config + Batch Rotation + Explode
const items = $input.all();
const staticData = $getWorkflowStaticData('global');

// Extract symbols from Universe sheet
const symbols = items
  .map(i => (i.json.Symbol || i.json.symbol || '').trim())
  .filter(s => s.length > 0);

if (symbols.length === 0) {
  return [{ json: { ok: false, error: 'NO_SYMBOLS', symbols: [] } }];
}

// Batch rotation (round-robin, 25 per run)
const BATCH_SIZE = 25;
let idx = parseInt(staticData.lastIndex || '0', 10);
if (idx >= symbols.length) idx = 0;
const batch = symbols.slice(idx, idx + BATCH_SIZE);
const nextIdx = (idx + BATCH_SIZE >= symbols.length) ? 0 : idx + BATCH_SIZE;
staticData.lastIndex = nextIdx;

// Generate run ID
const now = new Date();
const ts = now.toISOString().replace(/[-:T]/g, '').slice(0, 14);
const run_id = `AG2V2_${ts}_${idx}`;

// Explode: one item per symbol
const output = batch.map((symbol, i) => ({
  json: {
    ok: true,
    symbol,
    run_id,
    yfinance_api_base: 'http://yfinance-api:8080',
    intraday: { interval: '1h', lookback_days: 60, max_bars: 200, min_bars: 50 },
    daily: { interval: '1d', lookback_days: 400, max_bars: 400, min_bars: 200 },
    batch_info: { start: idx, size: batch.length, total: symbols.length },
    _index: i,
  }
}));

// First item also carries universe data for DuckDB sync
output[0].json._universe = items.map(i => i.json);

return output;

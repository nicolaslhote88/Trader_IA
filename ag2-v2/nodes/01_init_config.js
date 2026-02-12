// AG2-V2 — Init Config (pass-through, no batch rotation)
// Batch rotation is handled by DuckDB Init (persisted in DB)
const items = $input.all();

// Extract symbols from Universe sheet
const symbols = items
  .map(i => (i.json.Symbol || i.json.symbol || '').trim())
  .filter(s => s.length > 0);

if (symbols.length === 0) {
  return [{ json: { ok: false, error: 'NO_SYMBOLS', symbols: [] } }];
}

// Pass ALL symbols + config to DuckDB Init (which handles batch slicing)
return [{
  json: {
    ok: true,
    _all_symbols: symbols,
    _universe: items.map(i => i.json),
    yfinance_api_base: 'http://yfinance-api:8080',
    intraday: { interval: '1h', lookback_days: 60, max_bars: 200, min_bars: 50 },
    daily: { interval: '1d', lookback_days: 400, max_bars: 400, min_bars: 200 },
  }
}];

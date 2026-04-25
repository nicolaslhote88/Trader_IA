const AG4_V3_ALLOWED_PAIRS = [
  'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'NZDUSD', 'USDCAD',
  'EURGBP', 'EURJPY', 'EURCHF', 'EURAUD', 'EURCAD', 'EURNZD',
  'GBPJPY', 'GBPCHF', 'GBPAUD', 'GBPCAD',
  'AUDJPY', 'AUDNZD', 'AUDCAD',
  'NZDJPY', 'NZDCAD',
  'CADJPY', 'CHFJPY', 'CADCHF',
  'CHFCAD', 'JPYNZD',
];

const unique = new Set(AG4_V3_ALLOWED_PAIRS);
if (unique.size !== 27 || unique.size !== AG4_V3_ALLOWED_PAIRS.length) {
  throw new Error(`AG2-FX universe must contain exactly 27 distinct pairs, got ${unique.size}`);
}

const runId = `AG2FX_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`;

return [{
  json: {
    run_id: runId,
    as_of: new Date().toISOString(),
    db_path: process.env.AG2_FX_V1_DUCKDB_PATH || '/files/duckdb/ag2_fx_v1.duckdb',
    yfinance_api_base: process.env.YFINANCE_API_URL || 'http://yfinance-api:8080',
    dry_run: String(process.env.AG1_FX_DRY_RUN || process.env.FX_DRY_RUN || '').toLowerCase() === '1',
    universe: AG4_V3_ALLOWED_PAIRS,
    interval: '1d',
    lookback_days: 420,
    max_bars: 420,
  },
}];

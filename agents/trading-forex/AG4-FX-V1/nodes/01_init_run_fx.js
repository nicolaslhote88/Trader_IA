const runId = `AG4FXD_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`;

return [{
  json: {
    run_id: runId,
    as_of: new Date().toISOString(),
    lookback_hours: Number($env.AG4_FX_LOOKBACK_HOURS || 24),
    db_path: $env.AG4_FX_V1_DUCKDB_PATH || '/files/duckdb/ag4_fx_v1.duckdb',
    ag4_v3_path: $env.AG4_DUCKDB_PATH || '/files/duckdb/ag4_v3.duckdb',
    ag4_forex_path: $env.AG4_FOREX_DUCKDB_PATH || '/files/duckdb/ag4_forex_v1.duckdb',
  },
}];

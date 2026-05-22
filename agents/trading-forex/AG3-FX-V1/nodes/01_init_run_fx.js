const runId = `AG3FX_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`;

return [{
  json: {
    run_id: runId,
    as_of: new Date().toISOString(),
    workflow_version: 'AG3-FX-V1',
    db_path: $env.AG3_FX_V1_DUCKDB_PATH || '/files/duckdb/ag3_fx_v1.duckdb',
    ag2_fx_path: $env.AG2_FX_V1_DUCKDB_PATH || '/files/duckdb/ag2_fx_v1.duckdb',
    ag4_fx_path: $env.AG4_FX_V1_DUCKDB_PATH || '/files/duckdb/ag4_fx_v1.duckdb',
    macro_duckdb_path: $env.MACRO_DUCKDB_PATH || '/files/duckdb/macro_data.duckdb',
    schema_path: $env.AG3_FX_V1_SCHEMA_PATH || '/files/AG3-FX-V1-EXPORT/sql/ag3_fx_v1_schema.sql',
    fred_api_key: $env.FRED_API_KEY || '',
    macro_fetch_timeout_seconds: Number($env.AG3_FX_MACRO_FETCH_TIMEOUT_SECONDS || 8),
    max_equilibrium_shift_pct: Number($env.AG3_FX_MAX_EQUILIBRIUM_SHIFT_PCT || 0.06),
  },
}];

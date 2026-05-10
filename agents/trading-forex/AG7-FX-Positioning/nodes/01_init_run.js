// AG7-FX-Positioning — Init Run
// Pilier 3 : Positionnement spéculatif (COT CFTC)
const runId = `AG7POS_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`;

return [{
  json: {
    run_id: runId,
    as_of: new Date().toISOString(),
    macro_api_url: $env.MACRO_DATA_API_URL || 'http://macro-data-api:8081',
    macro_duckdb_path: $env.MACRO_DUCKDB_PATH || '/files/duckdb/macro_data.duckdb',
  },
}];

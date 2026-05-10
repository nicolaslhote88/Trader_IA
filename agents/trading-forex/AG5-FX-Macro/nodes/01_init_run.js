// AG5-FX-Macro — Init Run
// Pilier 1 : Analyse Macro/Flows par devise
const runId = `AG5MACRO_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`;

return [{
  json: {
    run_id: runId,
    as_of: new Date().toISOString(),
    macro_api_url: $env.MACRO_DATA_API_URL || 'http://macro-data-api:8081',
    macro_duckdb_path: $env.MACRO_DUCKDB_PATH || '/files/duckdb/macro_data.duckdb',
  },
}];

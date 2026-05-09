// AG8-FX-Rates — Init Run
// Stratégie de courbe des taux souverains (pentification/aplatissement)
const runId = `AG8RATES_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`;

return [{
  json: {
    run_id: runId,
    as_of: new Date().toISOString(),
    macro_api_url: $env.MACRO_DATA_API_URL || 'http://macro-data-api:8081',
    macro_duckdb_path: $env.MACRO_DUCKDB_PATH || '/files/duckdb/macro_data.duckdb',
    steepening_threshold_bps: 10,  // +10bps/mois = signal pentification
  },
}];

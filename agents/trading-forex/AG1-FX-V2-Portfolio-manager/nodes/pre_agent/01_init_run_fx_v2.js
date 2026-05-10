// AG1-FX-V2 — Init Run (Framework 3 Piliers)
const DEFAULT_VARIANT = 'chatgpt52';
const variant = $env.AG1_FX_V2_VARIANT || $json.variant || DEFAULT_VARIANT;
const modelByVariant = {
  chatgpt52: 'gpt-5.5',
  grok41_reasoning: 'grok-4.20-0309-reasoning',
  gemini30_pro: 'models/gemini-3.1-pro-preview',
};
const dbPathByVariant = {
  chatgpt52: $env.AG1_FX_V2_CHATGPT52_DUCKDB_PATH || '/files/duckdb/ag1_fx_v2_chatgpt52.duckdb',
  grok41_reasoning: $env.AG1_FX_V2_GROK41_REASONING_DUCKDB_PATH || '/files/duckdb/ag1_fx_v2_grok41_reasoning.duckdb',
  gemini30_pro: $env.AG1_FX_V2_GEMINI30_PRO_DUCKDB_PATH || '/files/duckdb/ag1_fx_v2_gemini30_pro.duckdb',
};
const model = $env.AG1_FX_LLM_MODEL_OVERRIDE || $json.llm_model || modelByVariant[variant] || modelByVariant[DEFAULT_VARIANT];

return [{
  json: {
    run_id: `AG1FXV2_${variant}_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`,
    as_of: new Date().toISOString(),
    llm_model: model,
    variant,
    db_path: dbPathByVariant[variant] || dbPathByVariant[DEFAULT_VARIANT],
    ag2_fx_path: $env.AG2_FX_V1_DUCKDB_PATH || '/files/duckdb/ag2_fx_v1.duckdb',
    ag4_fx_path: $env.AG4_FX_V1_DUCKDB_PATH || '/files/duckdb/ag4_fx_v1.duckdb',
    macro_duckdb_path: $env.MACRO_DUCKDB_PATH || '/files/duckdb/macro_data.duckdb',
    macro_api_url: $env.MACRO_DATA_API_URL || 'http://macro-data-api:8081',
    schema_path: $env.AG1_FX_V2_LEDGER_SCHEMA_PATH || '/files/AG1-FX-V2-EXPORT/sql/ag1_fx_v2_schema.sql',
    dry_run: String($env.AG1_FX_DRY_RUN || '').toLowerCase() === '1',
    // Config 3 piliers
    three_pillars_threshold: 0.20,     // seuil minimum d'alignement par pilier
    volatility_target_pct: 0.12,       // cible de volatilité portefeuille 12%
    leverage_max: Number($env.AG1_FX_V2_LEVERAGE_MAX || '2.0'),
  },
}];

const DEFAULT_VARIANT = 'chatgpt52';
const variant = $env.AG1_FX_VARIANT || $json.variant || DEFAULT_VARIANT;
const modelByVariant = {
  chatgpt52: 'gpt-5.5',
  grok41_reasoning: 'grok-4.20-0309-reasoning',
  gemini30_pro: 'models/gemini-3.1-pro-preview',
};
const dbPathByVariant = {
  chatgpt52: $env.AG1_FX_V1_CHATGPT52_DUCKDB_PATH || '/files/duckdb/ag1_fx_v1_chatgpt52.duckdb',
  grok41_reasoning: $env.AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH || '/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb',
  gemini30_pro: $env.AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH || '/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb',
};
const model = $env.AG1_FX_LLM_MODEL_OVERRIDE || $json.llm_model || modelByVariant[variant] || modelByVariant[DEFAULT_VARIANT];

return [{
  json: {
    run_id: `AG1FX_${variant}_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`,
    as_of: new Date().toISOString(),
    llm_model: model,
    variant,
    db_path: dbPathByVariant[variant] || dbPathByVariant[DEFAULT_VARIANT],
    ag2_fx_path: $env.AG2_FX_V1_DUCKDB_PATH || '/files/duckdb/ag2_fx_v1.duckdb',
    ag4_fx_path: $env.AG4_FX_V1_DUCKDB_PATH || '/files/duckdb/ag4_fx_v1.duckdb',
    ag3_fx_path: $env.AG3_FX_V1_DUCKDB_PATH || '/files/duckdb/ag3_fx_v1.duckdb',
    schema_path: $env.AG1_FX_V1_LEDGER_SCHEMA_PATH || '/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql',
    dry_run: String($env.AG1_FX_DRY_RUN || '').toLowerCase() === '1',
  },
}];

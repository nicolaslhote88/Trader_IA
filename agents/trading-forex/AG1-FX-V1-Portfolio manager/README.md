# AG1-FX-V1 Portfolio Manager

Dedicated Forex-only portfolio manager fork for three isolated LLM portfolios.

## Role

- Reads `ag2_fx_v1.duckdb` technical signals and `ag4_fx_v1.duckdb` macro/news digest.
- Maintains one DuckDB ledger per model:
  - `ag1_fx_v1_chatgpt52.duckdb`
  - `ag1_fx_v1_grok41_reasoning.duckdb`
  - `ag1_fx_v1_gemini30_pro.duckdb`
- Starts each ledger with 10,000 EUR, leverage 1, configurable via `cfg.portfolio_config.leverage_max`.
- Enforces the FX risk checks before simulated fills.
- Sends a compact `llm_brief` to the model instead of the raw AG2/AG4 payload. The
  full `brief` is still kept in the workflow context for risk checks, fills,
  conversions and snapshots.
- The init node derives `llm_model` from the selected variant. Use
  `AG1_FX_LLM_MODEL_OVERRIDE` only for an intentional one-off model override.
- The compact news pack filters out top-news items without an FX directional hint
  when usable FX-specific news is available.

## Cron (updated 2026-05-05)

- `chatgpt52`: `30 4,8,12,16,20 * * 1-5` (04:30, 08:30, 12:30, 16:30, 20:30)
- `grok41_reasoning`: `35 4,8,12,16,20 * * 1-5` (04:35, 08:35, 12:35, 16:35, 20:35)
- `gemini30_pro`: `40 4,8,12,16,20 * * 1-5` (04:40, 08:40, 12:40, 16:40, 20:40)
- Portfolio valuation: `AG1-FX-PF-V1` runs `0 0 * * * 1-5` (hourly, Monday-Friday).

All cron schedules use `Europe/Paris`. The 5-minute stagger between LLMs avoids
DuckDB read-concurrency conflicts on the shared bases `ag2_fx_v1.duckdb` and
`ag4_fx_v1.duckdb`. Each PM run is scheduled after AG2-FX
(04:00 / 08:00 / 12:00 / 16:00 / 20:00) so it always reads a fresh technical
snapshot, while AG4-FX keeps its 09:15 / 14:15 macro/news cadence.
The hourly valuation workflow is separate from the PM workflows: it only refreshes
`core.portfolio_snapshot` from current FX prices and never creates trade decisions.

`generate_model_variants.py` is the source of truth: never edit the per-model
JSON workflows by hand — regenerate them.

## Safety

The generated workflows use real LangChain agent nodes for all three variants:
OpenAI (`chatgpt52`), xAI Grok (`grok41_reasoning`) and Google Gemini
(`gemini30_pro`). The downstream parser, risk manager, execution simulator and
ledger writes are wired after each provider-specific agent merge.

## Local replay

```powershell
cd "agents/trading-forex/AG1-FX-V1-Portfolio manager"
python generate_model_variants.py
python nodes/post_agent/duckdb_writer.py init-schema --db .\ag1_fx_v1_smoke.duckdb --schema .\sql\ag1_fx_v1_schema.sql
```

## Schema

See `sql/ag1_fx_v1_schema.sql` and `infra/migrations/ag1_fx_v1/20260426_init.sql`.

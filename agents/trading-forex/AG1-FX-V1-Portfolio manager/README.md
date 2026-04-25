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

## Cron (updated 2026-04-26)

- `chatgpt52`: `30 9,14 * * 1-5` (09:30, 14:30)
- `grok41_reasoning`: `45 9,14 * * 1-5` (09:45, 14:45)
- `gemini30_pro`: `0 10,15 * * 1-5` (10:00, 15:00)

All cron schedules use `Europe/Paris`. The 15-minute stagger between LLMs avoids
DuckDB read-concurrency conflicts on the shared bases `ag2_fx_v1.duckdb` and
`ag4_fx_v1.duckdb`. Each PM run is scheduled after AG2-FX (08:00 / 12:00) and
AG4-FX (09:15 / 14:15) so it always reads the freshest technical + macro snapshot.

`generate_model_variants.py` is the source of truth: never edit the per-model
JSON workflows by hand — regenerate them.

## Safety

The generated workflows keep a P3-safe `LLM Decision Placeholder` that emits `hold` decisions until Nicolas validates the first manual run and the real provider nodes are connected in n8n. The downstream parser, risk manager, execution simulator and ledger writes are already wired.

## Local replay

```powershell
cd "agents/trading-forex/AG1-FX-V1-Portfolio manager"
python generate_model_variants.py
python nodes/post_agent/duckdb_writer.py init-schema --db .\ag1_fx_v1_smoke.duckdb --schema .\sql\ag1_fx_v1_schema.sql
```

## Schema

See `sql/ag1_fx_v1_schema.sql` and `infra/migrations/ag1_fx_v1/20260426_init.sql`.

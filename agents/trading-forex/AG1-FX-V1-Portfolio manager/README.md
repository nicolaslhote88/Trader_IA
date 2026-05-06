# AG1-FX-V1 Portfolio Manager

Dedicated Forex-only portfolio manager fork. The repository still generates the
three LLM variants, but production paper trading currently publishes only the
`chatgpt52` workflow because there is a single IBKR paper portfolio available.
`grok41_reasoning` and `gemini30_pro` must stay inactive unless they are moved to
a separate broker account or a broker-side portfolio namespace.

## Role

- Reads `ag2_fx_v1.duckdb` technical signals, `ag3_fx_v1.duckdb` fundamental
  context and `ag4_fx_v1.duckdb` macro/news digest.
- Maintains one DuckDB ledger per model:
  - `ag1_fx_v1_chatgpt52.duckdb`
  - `ag1_fx_v1_grok41_reasoning.duckdb`
  - `ag1_fx_v1_gemini30_pro.duckdb`
- Production paper mode uses `ag1_fx_v1_chatgpt52.duckdb` as the only
  executable ledger. The other ledgers may be reset and kept for offline tests,
  but their n8n workflows are unpublished.
- Starts each ledger with 10,000 EUR, leverage 1, configurable via
  `cfg.portfolio_config.leverage_max`.
- Sends a compact `llm_brief` to the model instead of the raw AG2/AG3/AG4
  payload. The full `brief` is still kept in the workflow context for downstream
  risk checks, fills, conversions and snapshots.
- The init node derives `llm_model` from the selected variant. Use
  `AG1_FX_LLM_MODEL_OVERRIDE` only for an intentional one-off model override.
- The compact news pack filters out top-news items without an FX directional hint
  when usable FX-specific news is available.
- The IBKR execution node `nodes/post_agent/11b_ibkr_send_orders_fx.py` is wired
  in the template and all variants between safety validation and fill handling.
  In `IBKR_DRY_RUN=false`, orders are submitted to `ibkr-broker`, must pass the
  paper-account guard, and are considered filled only after an IBKR fill is
  matched. Without a matched fill the order remains `submitted` for the hourly
  reconciliation workflow.

## Production Paper Mode (Updated 2026-05-06)

- Active PM workflow: `AG1-FX-V1 Portfolio Manager - chatgpt52`.
- Inactive PM workflows: `grok41_reasoning`, `gemini30_pro`.
- Account guard: `IBKR_REQUIRE_PAPER_ACCOUNT=true` and paper prefixes include
  `DU`.
- DuckDB is a local ledger, not the broker source of truth. Before every PM run,
  node `03_load_portfolio_state_fx.py` compares open DuckDB lots with current
  IBKR positions. A divergence activates the kill switch and blocks new orders.
- A global lock (`AG1_FX_LOCK_PATH`, default `/files/locks/ag1_fx_active.lock`)
  prevents overlapping PM runs against the single broker account.
- Node `12_simulate_fills_fx.py` still simulates fills in dry-run. In paper/live
  mode it writes only confirmed IBKR fills and never invents a simulated fill.
- Node `17_log_run_fx.py` releases the global lock at the end of the run.
- `AG1-FX-PF-V1 - Hourly Portfolio Valuation` reconciles the GPT ledger hourly,
  imports confirmed fills for submitted orders, and writes
  `core.reconciliation_log`.

## Cron (Updated 2026-05-06)

- `chatgpt52`: `30 4,8,12,16,20 * * 1-5` (04:30, 08:30, 12:30, 16:30, 20:30)
- `grok41_reasoning`: generated with `35 4,8,12,16,20 * * 1-5`, but unpublished
  in production paper mode.
- `gemini30_pro`: generated with `40 4,8,12,16,20 * * 1-5`, but unpublished in
  production paper mode.
- Portfolio valuation: `AG1-FX-PF-V1` runs `0 0 * * * 1-5` (hourly,
  Monday-Friday).

All cron schedules use `Europe/Paris`. The 5-minute stagger is retained in the
generated files for future multi-account tests, but only GPT should be active
while a single IBKR paper account is shared. Each PM run is scheduled after
AG2-FX and AG3-FX so it reads fresh technical and fundamental context.

The hourly valuation workflow is separate from the PM workflows: it refreshes
`core.portfolio_snapshot`, imports confirmed broker fills, records reconciliation
state and never creates trade decisions.

`generate_model_variants.py` is the source of truth: never edit the per-model
JSON workflows by hand. Regenerate them instead.

## Safety

The generated workflows use real LangChain agent nodes for all three variants:
OpenAI (`chatgpt52`), xAI Grok (`grok41_reasoning`) and Google Gemini
(`gemini30_pro`). The downstream parser, risk manager, broker sender, fill
handler and ledger writes are wired after each provider-specific agent merge.

## Local Replay

```powershell
cd "agents/trading-forex/AG1-FX-V1-Portfolio manager"
python generate_model_variants.py
python nodes/post_agent/duckdb_writer.py init-schema --db .\ag1_fx_v1_smoke.duckdb --schema .\sql\ag1_fx_v1_schema.sql
```

## Schema

See `sql/ag1_fx_v1_schema.sql` and `infra/migrations/ag1_fx_v1/20260426_init.sql`.

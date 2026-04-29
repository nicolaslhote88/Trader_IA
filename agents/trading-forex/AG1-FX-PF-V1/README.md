# AG1-FX-PF-V1 Portfolio Valuation

Hourly mark-to-market workflow for the three dedicated AG1-FX portfolios.

## Role

- Reads the three AG1-FX DuckDB ledgers:
  - `ag1_fx_v1_chatgpt52.duckdb`
  - `ag1_fx_v1_grok41_reasoning.duckdb`
  - `ag1_fx_v1_gemini30_pro.duckdb`
- Fetches hourly FX prices through `yfinance-api` using `<PAIR>=X` symbols.
- Falls back to the latest AG2-FX technical prices when `yfinance-api` is unavailable.
- Writes a fresh `core.portfolio_snapshot` row in each AG1-FX ledger.
- Does not ask an LLM for decisions and does not open or close positions.

## Cron

`0 0 * * * 1-5`

Runs every hour, Monday to Friday, in `Europe/Paris`.

## Local rebuild

```powershell
cd "agents/trading-forex/AG1-FX-PF-V1"
python build_workflow.py
```

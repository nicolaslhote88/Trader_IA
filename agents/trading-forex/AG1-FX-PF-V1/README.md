# AG1-FX-PF-V1 Portfolio Valuation

Hourly mark-to-market and broker reconciliation workflow for AG1-FX.

## Role

- Production paper mode targets only `ag1_fx_v1_chatgpt52.duckdb`, because GPT is
  the only AG1-FX portfolio manager currently published against the single IBKR
  paper account.
- The Grok and Gemini ledgers remain versioned for offline comparison, but the
  hourly production reconciliation does not touch them by default.
- Fetches hourly FX prices through `yfinance-api` using `<PAIR>=X` symbols.
- Falls back to the latest AG2-FX technical prices when `yfinance-api` is
  unavailable.
- Writes a fresh `core.portfolio_snapshot` row in the target AG1-FX ledger.
- Snapshot P&L is the official portfolio reconciliation:
  `equity_eur = cash_ledger_eur + realized_pnl_eur + floating_pnl_eur - all
  core.fills.fees_eur`, then `pnl_total_eur = equity_eur - initial_capital_eur`.
- `cash_eur` stores the cash ledger balance. `margin_used_eur` and
  `margin_free_eur` stay separate; earlier dashboard readings that summed
  `cash_eur + margin_used_eur` were only valid when `cash_eur` had been
  populated by the old margin-free bug.
- `notes` carries the P&L decomposition (`cash_ledger_eur`,
  `realized_pnl_eur`, `floating_pnl_eur`, `fees_eur`, `notional_eur`) so the
  dashboard can reconcile snapshot P&L with lot-level tables.
- Imports confirmed IBKR fills for orders left in `submitted` state by the PM
  workflow.
- Imports IBKR commission data for those fills into both `core.fills.fees_eur`
  and the audit table `core.fill_costs`, preserving the raw broker payload used
  for fee attribution.
- Compares DuckDB open lots with current IBKR positions and writes
  `core.reconciliation_log`.
- Sets `cfg.portfolio_config.kill_switch_active=true` and blocks new orders when
  broker positions diverge from the DuckDB ledger.
- Does not ask an LLM for decisions and does not open or close positions.

## Cron

`0 0 * * * 1-5`

Runs every hour, Monday to Friday, in `Europe/Paris`.

## Local Rebuild

```powershell
cd "agents/trading-forex/AG1-FX-PF-V1"
python build_workflow.py
```

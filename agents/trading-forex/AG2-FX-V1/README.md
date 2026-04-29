# AG2-FX-V1

Technical signal agent dedicated to the AG1-FX-V1 Forex sandbox.

## Role

- Maintains the 27-pair FX universe aligned with `agents/common/AG4-V3/nodes/10_parse_llm_output.js`.
- Fetches daily Yahoo Finance bars through `yfinance-api`.
- Computes FX-only indicators, pivots, regimes and a bounded `signal_score` in `[-1, 1]`.
- Writes to `/files/duckdb/ag2_fx_v1.duckdb`.

## Cron

`0 0,4,8,12,16,20 * * 1-5` in `Europe/Paris` (6x/day, every 4h, covering forex 24/5 — Asia/Europe/US sessions). Updated 2026-04-26.

## Local replay

```powershell
cd agents/trading-forex/AG2-FX-V1
python build_workflow.py
```

For smoke tests without `yfinance-api`, set `AG1_FX_DRY_RUN=1`; node `03_fetch_yfinance_fx.py` then generates deterministic synthetic bars.

## Schema

See `sql/ag2_fx_v1_schema.sql` and `infra/migrations/ag2_fx_v1/20260426_init.sql`.

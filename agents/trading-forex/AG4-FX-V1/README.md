# AG4-FX-V1

Macro/news digest agent dedicated to the AG1-FX-V1 Forex sandbox.

## Role

- Reads global AG4 news from `ag4_v3.duckdb` where `impact_asset_class` contains `FX` or `Mixed`.
- Reads FX-channel news, macro regime and pair bias from `ag4_forex_v1.duckdb`.
- Deduplicates the last 24 hours and writes three payload sections to `ag4_fx_v1.duckdb`: `top_news`, `pair_focus`, `macro_regime`.

## Cron

`15 9,14 * * 1-5` in `Europe/Paris` (2x/day at 09:15 and 14:15, inside the Paris stock exchange open window 09:00–17:30). Updated 2026-04-26.

## Local replay

```powershell
cd agents/trading-forex/AG4-FX-V1
python build_workflow.py
```

## Schema

See `sql/ag4_fx_v1_schema.sql` and `infra/migrations/ag4_fx_v1/20260426_init.sql`.

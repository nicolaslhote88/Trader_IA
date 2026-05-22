# AG3-FX-V1 - Forex Fundamental Analyst

AG3-FX-V1 adds a calculated fundamental layer between AG2-FX/AG4-FX and AG1-FX.
It scores the relative macro strength of each currency, converts that strength into
pair bias, and computes an operational equilibrium target and band for each active
FX pair.

## Inputs

- `/files/duckdb/ag2_fx_v1.duckdb`
  - `main.universe_fx`
  - latest `main.technical_signals_fx`
- `/files/duckdb/ag4_fx_v1.duckdb`
  - latest `main.fx_digest` sections: `top_news`, `pair_focus`, `macro_regime`
- `/files/duckdb/macro_data.duckdb`
  - `macro.policy_rates` for current central-bank policy rates
  - `macro.country_indicators` for CPI, GDP, labor and external-balance inputs when available
  - `rates.yield_curve` and `pillars.currency_scores` for real-yield/carry proxy fallback
- Optional external macro observations through FRED when `FRED_API_KEY` is set.
- Public World Bank macro observations for real yields, inflation, growth, labor and external balance where no FRED mapping is configured.
- AG2-FX IBKR snapshot fields, when available, for live spot/spread-aware equilibrium bands.

AG3-FX first reuses the central macro-data service so AG5-AG8 outputs are not
duplicated by weaker external fallbacks. `policy_rate` comes from
`macro.policy_rates`; `real_yield` uses nominal rate minus CPI when fresh and falls
back to the AG6 carry score as an explicit proxy when CPI is stale.

The global `DEGRADED_MACRO_DATA` flag is evaluated on the liquid core basket
`USD, EUR, JPY, GBP, CHF, AUD, CAD, NZD`. Extended currencies remain visible with
their own `missing_factors` and `stale_factors`, but incomplete MXN/SEK/NOK/CNH
coverage no longer degrades the entire AG3 run. If the core macro layer is
unavailable, AG3-FX still emits one row per active pair using AG4-FX macro/news
proxies plus AG2-FX spot/ATR; in that mode confidence is capped at `0.45`.

## Output

Primary database:

```text
/files/duckdb/ag3_fx_v1.duckdb
```

AG1-FX reads:

```sql
SELECT pair, payload_json
FROM main.v_ag3_fx_ag1_summary
ORDER BY pair;
```

Each `payload_json` contains:

- fundamental base/quote strength and directional bias
- equilibrium target mid/low/high
- spot vs equilibrium mispricing
- drivers, invalidators and data caveats
- compact schema version `AG3_FX_V1`

## Schedule

The workflow runs after AG2-FX and before AG1-FX:

```text
20 4,8,12,16,20 * * 1-5
```

That maps to 04:20 / 08:20 / 12:20 / 16:20 / 20:20 Europe/Paris.

## Generate workflow

```bash
python agents/trading-forex/AG3-FX-V1/build_workflow.py
```

Generated file:

```text
agents/trading-forex/AG3-FX-V1/workflow/AG3_FX_workflow_v1.json
```

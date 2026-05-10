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
- Optional external macro observations through FRED when `FRED_API_KEY` is set.
- Public World Bank macro observations for real yields, inflation, growth, labor and external balance where no FRED mapping is configured.
- AG2-FX IBKR snapshot fields, when available, for live spot/spread-aware equilibrium bands.

If external macro data is unavailable, AG3-FX still emits one row per active pair
using AG4-FX macro/news proxies plus AG2-FX spot/ATR. In that mode targets are
marked `DEGRADED_MACRO_DATA` and confidence is capped at `0.45`.

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

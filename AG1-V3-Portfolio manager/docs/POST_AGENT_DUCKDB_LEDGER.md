# AG1 Post-Agent Migration: DuckDB Portfolio Ledger v2

## What changed

The downstream AG1 execution path is now ledger-first in DuckDB:

1. `7 - Validate & Enforce Safety`
2. `8 - Build DuckDB Bundle`
3. `9 - Upsert Run Bundle (DuckDB)`
4. `10 - Post-Run Health (DuckDB)`

Legacy Google Sheets append/upsert branches are kept but disabled in AG1 workflow variants (`workflow/variants/AG1_workflow_v3__*.json`).

## New files

- `AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql`
- `AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py`
- `AG1-V3-Portfolio manager/nodes/post_agent/08_build_duckdb_bundle.code.js`
- `AG1-V3-Portfolio manager/nodes/post_agent/09_upsert_run_bundle_duckdb.code.py`
- `AG1-V3-Portfolio manager/nodes/post_agent/10_post_run_health.code.py`

## DuckDB writer API

`duckdb_writer.py` exposes:

- `init_schema(db_path)`
- `upsert_run_bundle(db_path, bundle_json)`
- `compute_snapshots(db_path, run_id)`

Implementation notes:

- Uses `INSERT ... ON CONFLICT DO UPDATE` (no `INSERT OR REPLACE`).
- Stores audit payloads in JSON columns.
- Rebuilds `core.position_lots` from `core.fills` + `core.orders` using FIFO.
- Recomputes `positions_snapshot`, `portfolio_snapshot`, `risk_metrics`.

## Bundle contract

`08_build_duckdb_bundle.code.js` outputs:

```json
{
  "run_id": "RUN_...",
  "db_path": "/files/duckdb/ag1_v3.duckdb",
  "bundle": {
    "run": {},
    "instruments": [],
    "orders": [],
    "fills": [],
    "cash_ledger": [],
    "ai_signals": [],
    "market_prices": [],
    "alerts": [],
    "backfill_queue": [],
    "snapshots": {}
  }
}
```

## Runtime prerequisites

- Python package `duckdb` available in the runtime used by n8n Python Code node.
- Writer module reachable by path:
  - `AG1_DUCKDB_WRITER_PATH` env var (recommended), or
  - default fallback paths in `09_upsert_run_bundle_duckdb.code.py`.
- Note: the current workflow code still uses legacy fallback defaults under `/files/AG1-V3-EXPORT/...`.
  If you mount this pack under another path, set `AG1_DUCKDB_WRITER_PATH` and `AG1_LEDGER_SCHEMA_PATH`.
- DB file path from:
  - `item.json.db_path`, or
  - `AG1_DUCKDB_PATH`, default `/files/duckdb/ag1_v3.duckdb`.

## Optional CLI usage

`duckdb_writer.py` can also be called via Execute Command:

```bash
python AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py init-schema --db /files/duckdb/ag1_v3.duckdb
python AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py upsert-run-bundle --db /files/duckdb/ag1_v3.duckdb --bundle-file /tmp/bundle.json
python AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py compute-snapshots --db /files/duckdb/ag1_v3.duckdb --run-id RUN_20260219_101500_ab12cd34
```

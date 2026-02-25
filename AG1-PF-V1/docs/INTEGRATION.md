# AG1-PF-V1 Integration Notes

## 1) Audit of the provided PF workflow

### What is coherent
- `SplitInBatches` wiring follows the same pattern used in other project workflows:
  - `main[1]` is the per-item loop branch (price fetch + MTM)
  - `main[0]` is the done branch (sheet update)
- The MTM logic is resilient (`1h`/`1d` merge, fallback to existing `LastPrice`, skip-safe diagnostics).
- Google Sheets update is constrained to mutable columns (`LastPrice`, `MarketValue`, `UnrealizedPnL`, `UpdatedAt`) and keeps identity metadata.

### Gaps vs project standards (DuckDB-first pattern)
- No persistent run log (`run_id`, status, counters).
- No DuckDB history table for MTM changes.
- No DuckDB latest table for fast queries and monitoring.
- No explicit re-sync path from DuckDB to Sheets.

## 2) Target pattern for this workflow

- Keep Google Sheets as human control layer (manual edits stay possible).
- Add DuckDB persistence in parallel (history + latest + run log).
- Keep existing Sheets update behavior unchanged.

## 3) Required n8n wiring changes

### 3.1 Add config keys in `PF.00 — Config`
Add these assignments:
- `portfolio_db_path` = `/local-files/duckdb/ag1_v2_chatgpt52.duckdb` (fallback only)
- `portfolio_db_paths_json` = `["/local-files/duckdb/ag1_v2_chatgpt52.duckdb","/local-files/duckdb/ag1_v2_grok41_reasoning.duckdb","/local-files/duckdb/ag1_v2_gemini30_pro.duckdb"]`
- `workflow_name` = `PF Portfolio MTM Updater`
- Optional `run_id` expression (if you want deterministic IDs):
  - `={{ 'PFMTM_' + $now.toFormat('yyyyLLddHHmmss') }}`

### 3.2 Enrich output of `PF.08 — Build Sheet Updates`
Keep current logic, but include these fields in each emitted row so DuckDB has complete records:
- `Symbol`
- `ISIN`
- `Quantity`
- `AvgPrice`
- `run_id`
- `portfolio_db_path`
- `workflow_name`

Suggested additions in the mapped object:

```javascript
Symbol: j.symbol || j.Symbol || "",
ISIN: j.ISIN || "",
Quantity: j.qty ?? j.Quantity ?? "",
AvgPrice: j.avgPrice ?? j.AvgPrice ?? "",
run_id: j.run_id || "",
portfolio_db_path: j.portfolio_db_path || "",
workflow_name: j.workflow_name || "",
```

### 3.3 Insert a Python Code node between `PF.08` and `Update Row`
- Node name suggestion: `PF.08B — Write Positions MTM DuckDB`
- Language: `pythonNative`
- Paste code from: `AG1-PF-V1/nodes/01_write_positions_mtm_duckdb.py`

New chain:
- `PF.08 — Build Sheet Updates` -> `PF.08B — Write Positions MTM DuckDB` -> `Update Row`

This preserves your live Sheets update while persisting each MTM run in DuckDB.

## 4) Optional: DuckDB -> Sheets recovery sync

If you want a manual recovery workflow (rebuild Sheet MTM values from DuckDB), use:
- `AG1-PF-V1/nodes/02_sync_positions_mtm_to_rows.py`

Typical chain:
- `Manual Trigger` -> `Code (pythonNative): 02_sync_positions_mtm_to_rows.py` -> `Google Sheets Update Row`

## 5) DuckDB schema reference

The schema used by the writer node is also provided in:
- `AG1-PF-V1/sql/schema.sql`

Tables:
- `portfolio_positions_mtm_latest`
- `portfolio_positions_mtm_history`
- `portfolio_positions_mtm_run_log`

# AG3-V2 - Fundamental Analyst (DuckDB-first)

## Goal
Provide a reliable fundamental workflow for equities, with persistent outputs in DuckDB:
- `fundamentals_snapshot` (raw yfinance payload, normalized per symbol/run)
- `fundamentals_triage_history` (score, risks, thesis, horizon)
- `analyst_consensus_history` (target prices and recommendation proxy)
- `fundamental_metrics_history` (normalized metric rows)
- `run_log` (run lifecycle and counters)

## Why this V2
The previous Boursorama HTML parsing approach is fragile (layout changes, anti-bot walls, intermittent blocks).
V2 is API-first (yfinance) and DuckDB-first for reliability, traceability, and query performance.

## Flow
1. Load `Universe` from Google Sheets.
2. Build symbol queue (`Symbol`, optional `BoursoramaRef`).
3. Fetch `/fundamentals?symbol=...` from `yfinance-api`.
4. Compute:
   - `Score` (0-100)
   - `risk_score` (0-100)
   - bull/bear thesis
   - valuation scenarios (`Bear/Base/Bull`)
   - horizon (`SWING` / `LONG_TERM` / `WATCH`)
5. Upsert rows into DuckDB only.
6. Finalize run statistics in `run_log`.

## Files
- `AG3-V2/build_workflow.py`: workflow generator.
- `AG3-V2/nodes/`: JS logic for context, queue, scoring and row preparation.
- `AG3-V2/AG3-V2-workflow.json`: generated n8n workflow to import.

## Generate workflow JSON
From `AG3-V2/`:

```bash
python build_workflow.py > AG3-V2-workflow.json
```

## Runtime requirements
- `yfinance-api` service reachable (default: `http://yfinance-api:8080`).
- `duckdb` available in Python runner.
- Google Sheets credential only for reading `Universe`.
- DuckDB volume mounted (default path: `/files/duckdb/ag3_v2.duckdb`).

## Notes
- This V2 intentionally avoids hard dependency on Boursorama page parsing.
- No AG3 write-back to Google Sheets anymore.
- `Split In Batches` wiring is explicit:
  - output `main[0]` = loop branch (fetch/process/write)
  - output `main[1]` = done branch (`Finalize Run`)

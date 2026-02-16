# AG3-V2 - Fundamental Analyst (API-first)

## Goal
Provide a reliable fundamental workflow for equities, with outputs directly usable by:
- `AG3_Triage_History` (score, risks, thesis, horizon)
- `research_analyst_consensus` (target prices and recommendation proxy)
- `Fundamental_Data` (normalized metric rows)

## Why this V2
The previous Boursorama HTML parsing approach is fragile (layout changes, anti-bot walls, intermittent blocks).
V2 is API-first using `yfinance-api` and keeps the same strategic objective for the "Fundamental Analyst".

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
5. Upsert rows to the three sheets.

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
- Google Sheets credential configured in n8n.
- Sheet tabs available:
  - `Universe`
  - `AG3_Triage_History`
  - `research_analyst_consensus`
  - `Fundamental_Data`

## Notes
- This V2 intentionally avoids hard dependency on Boursorama page parsing.
- If you still want Boursorama-specific enrichments, add them as optional low-priority branches after scoring.

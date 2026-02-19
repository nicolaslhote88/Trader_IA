# AG1 V2 Export Pack

This folder contains a GitHub-ready export of the updated AG1 workflow.

## Content

- `workflow/AG1_workflow_general.json`
- `nodes/NODE_SUMMARY.tsv`
- `nodes/pre_agent/*`
- `nodes/agent_input/*`
- `nodes/post_agent/*`
- `sql/portfolio_ledger_schema_v2.sql`
- `docs/POST_AGENT_DUCKDB_LEDGER.md`
- `export_to_github.ps1`

## Included updates

- Portfolio context from AG1 DuckDB (`AG1_DUCKDB_PATH`) with fallback.
- Multi-agent prep from AG2/AG3/AG4/AG4-SPE/YF enrichment DuckDB sources.
- Matrix and briefing calculation before Agent #1.
- Final input assembly for Agent #1 with:
- `portfolioBrief`
- `sector_brief`
- `opportunity_brief`
- `opportunity_pack`
- `opportunity_stats`
- `matrix_thresholds`
- Post-agent migration to DuckDB ledger (`runs/orders/fills/cash_ledger/position_lots/snapshots`).
- Google Sheets append/upsert nodes in the post-agent branch are disabled in workflow export.

## Expected env vars

- `AG1_DUCKDB_PATH=/files/duckdb/ag1_v2.duckdb`
- `AG2_DUCKDB_PATH=/files/duckdb/ag2_v2.duckdb`
- `AG3_DUCKDB_PATH=/files/duckdb/ag3_v2.duckdb`
- `AG4_DUCKDB_PATH=/files/duckdb/ag4_v2.duckdb`
- `AG4_SPE_DUCKDB_PATH=/files/duckdb/ag4_spe_v2.duckdb`
- `YF_ENRICH_DUCKDB_PATH=/files/duckdb/yf_enrichment_v1.duckdb`
- `AG1_DUCKDB_WRITER_PATH=/files/AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py` (recommended)
- `AG1_LEDGER_SCHEMA_PATH=/files/AG1-V2-EXPORT/sql/portfolio_ledger_schema_v2.sql` (optional override)

## n8n import

Import `workflow/AG1_workflow_general.json`.

## Git export (same branch)

Manual:

```powershell
git add -- "AG1 - Workflow g*ral.json" AG1-V2-EXPORT
git commit -m "AG1: update pre-agent data prep and export pack"
git push origin $(git rev-parse --abbrev-ref HEAD)
```

Script:

```powershell
powershell -ExecutionPolicy Bypass -File .\AG1-V2-EXPORT\export_to_github.ps1 -Push
```

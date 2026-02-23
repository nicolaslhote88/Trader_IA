# AG1 V2 Portfolio Manager Pack

This folder is the canonical AG1 workflow pack rebuilt from the manually maintained n8n workflow.

## Source of truth

- `workflow/AG1 - Workflow général.json` (manual n8n export kept unchanged)

## Generated export artifacts

- `workflow/AG1_workflow_general.json` (normalized UTF-8 copy)
- `workflow/nodes/*` (selected critical nodes and code)
- `workflow/sql/portfolio_ledger_schema_v2.sql`
- `workflow/docs/POST_AGENT_DUCKDB_LEDGER.md`
- `nodes/*` (mirror for direct mounting/reference)
- `sql/portfolio_ledger_schema_v2.sql`
- `docs/POST_AGENT_DUCKDB_LEDGER.md`

## Notes

- Code nodes are extracted from the workflow JSON.
- `duckdb_writer.py` is an external runtime dependency for node `9 - Upsert Run Bundle (DuckDB)`.

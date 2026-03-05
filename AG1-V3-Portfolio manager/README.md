# AG1 V3 Portfolio Manager Pack

This folder is the canonical AG1 workflow pack rebuilt from the maintained V3 template.

## Source of truth

- `workflow/AG1_workflow_template_v3.json` (canonical V3 template)

## Generated export artifacts

- `workflow/variants/AG1_workflow_v3__*.json` (model-specific workflows)
- `workflow/nodes/*` (selected critical nodes and code)
- `workflow/sql/portfolio_ledger_schema_v2.sql`
- `workflow/docs/POST_AGENT_DUCKDB_LEDGER.md`
- `nodes/*` (mirror for direct mounting/reference)
- `sql/portfolio_ledger_schema_v2.sql`
- `docs/POST_AGENT_DUCKDB_LEDGER.md`

## Notes

- Code nodes are extracted from the workflow JSON.
- `duckdb_writer.py` is an external runtime dependency for node `9 - Upsert Run Bundle (DuckDB)`.

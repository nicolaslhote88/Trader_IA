# AG1 V2 Workflow Pack (Subfolder Export)

Source workflow (manual import): `AG1 - Workflow général.json`

This subfolder contains a GitHub-ready export pack aligned with the corrected workflow JSON in this directory.

## Content

- `AG1_workflow_general.json` (normalized UTF-8 copy for GitHub/import)
- `nodes/NODE_SUMMARY.tsv`
- `nodes/pre_agent/*`
- `nodes/agent_input/*`
- `nodes/post_agent/*`
- `docs/POST_AGENT_DUCKDB_LEDGER.md`
- `sql/portfolio_ledger_schema_v2.sql`

## Notes

- Node `.node.json` files are exported directly from the workflow JSON.
- Code files (`.code.js`, `.code.py`) are extracted from n8n Code nodes.
- `duckdb_writer.py` is copied from the parent AG1 pack because Node 9 references an external writer script.
- The original imported workflow file is kept unchanged.

## n8n import

Import `AG1_workflow_general.json` (or the original source file if you prefer).

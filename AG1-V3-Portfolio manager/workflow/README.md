# AG1 V3 Workflow Pack

Template source: `AG1_workflow_template_v3.json`

This subfolder contains a GitHub-ready export pack for AG1 V3.

## Content

- `AG1_workflow_template_v3.json` (canonical template)
- `variants/AG1_workflow_v3__*.json` (model-specific workflows for n8n import)
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

## n8n import

Import one file from `variants/AG1_workflow_v3__*.json`.

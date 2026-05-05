# AG1 V3 Portfolio Manager Pack

The whole canonical pack now lives in [`workflow/`](workflow/).

- Source of truth : `workflow/AG1_workflow_template_v3.json`
- Nodes extraits : `workflow/nodes/`
- Schema DuckDB : `workflow/sql/portfolio_ledger_schema_v2.sql`
- Variants par modèle : `workflow/variants/`

## IBKR execution node

Le node `workflow/nodes/post_agent/07b_ibkr_send_orders.js` est insere dans le
template et les variantes entre `7 - Validate & Enforce Safety` et
`8 - Build DuckDB Bundle`.

Par defaut, `IBKR_DRY_RUN=true` garde le workflow en sandbox et le node ne
contacte pas `ibkr-broker`. Pour tester le chemin HTTP sans ordre live, definir
`IBKR_SEND_DRY_RUN_TO_BROKER=true`. Le passage live se fait uniquement via
`IBKR_DRY_RUN=false` cote VPS.

Utilitaires au niveau parent :

- `rebuild_pack.py` — régénère les fichiers `workflow/nodes/*` et les variants depuis le template.
- `export_to_github.ps1` — helper PowerShell pour commit + push ciblé sur ce dossier.

Voir [`docs/dev/rebuild_pack.md`](../docs/dev/rebuild_pack.md) pour la procédure.

# AG1 V3 Portfolio Manager Pack

The whole canonical pack now lives in [`workflow/`](workflow/).

- Source of truth : `workflow/AG1_workflow_template_v3.json`
- Nodes extraits : `workflow/nodes/`
- Schema DuckDB : `workflow/sql/portfolio_ledger_schema_v2.sql`
- Variants par modèle : `workflow/variants/`

Utilitaires au niveau parent :

- `rebuild_pack.py` — régénère les fichiers `workflow/nodes/*` et les variants depuis le template.
- `export_to_github.ps1` — helper PowerShell pour commit + push ciblé sur ce dossier.

Voir [`docs/dev/rebuild_pack.md`](../docs/dev/rebuild_pack.md) pour la procédure.

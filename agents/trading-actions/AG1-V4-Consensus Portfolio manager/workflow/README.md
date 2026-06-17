# AG1 V4 Workflow Pack

Socle de generation: `AG1_workflow_template_v4.json`.

Export importable dans n8n et source de verite operationnelle:
`AG1_workflow_v4_consensus.json`.

## Generation

```bash
python "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/build_v4_workflow.py"
```

Le script injecte les codes extraits dans le workflow, garde les trois branches
LLM du template, route `AG1.00` vers GPT/Grok/Claude en parallele, ajoute le
merge 4 entrees puis le node de consensus.

## Contrat DuckDB

- Base : `/files/duckdb/ag1_v4_consensus.duckdb`
- Writer : `/files/AG1-V4-EXPORT/nodes/post_agent/duckdb_writer.py`
- Schema : `/files/AG1-V4-EXPORT/sql/portfolio_ledger_schema_v4.sql`
- Capital initial seed : 10 000 EUR dans `cfg.portfolio_config` et
  `core.cash_ledger`.

Le node 9 V4 refuse le vieux chemin partage `ag1_v3.duckdb` et privilegie les
variables `AG1_V4_*`.

## Import n8n

Importer `AG1_workflow_v4_consensus.json`. Le workflow est exporte inactif par
defaut pour eviter une activation involontaire pendant le deploiement.
